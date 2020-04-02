#[cfg(windows)]
use super::codec::TokenSSPI;

use crate::{
    protocol::{
        codec::{
            BytesData, Decode, Packet, TokenColMetaData, TokenDone, TokenEnvChange, TokenError,
            TokenInfo, TokenLoginAck, TokenOrder, TokenReturnValue, TokenRow,
        },
        Context,
    },
    Error, TokenType,
};
use bytes::{Buf, BytesMut};
use futures::{ready, Stream, TryStream, TryStreamExt};
use std::{
    convert::TryFrom,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task,
};
use task::Poll;
use tracing::{event, Level};

#[derive(Debug)]
pub enum ReceivedToken {
    NewResultset(Arc<TokenColMetaData>),
    Row(TokenRow),
    Done(TokenDone),
    DoneInProc(TokenDone),
    DoneProc(TokenDone),
    ReturnStatus(u32),
    ReturnValue(TokenReturnValue),
    Order(TokenOrder),
    EnvChange(TokenEnvChange),
    Info(TokenInfo),
    LoginAck(TokenLoginAck),
    #[cfg(windows)]
    SSPI(TokenSSPI),
}

pub(crate) struct TokenStream<'a, S> {
    packet_stream: Pin<&'a mut S>,
    context: &'a Context,
    buf: BytesMut,
    has_more_data: bool,
}

impl<'a, S> TokenStream<'a, S>
where
    S: Stream<Item = crate::Result<Packet>> + Unpin + 'a,
{
    pub(crate) fn new(packet_stream: &'a mut S, context: &'a Context) -> Self {
        Self {
            packet_stream: Pin::new(packet_stream),
            context,
            buf: BytesMut::new(),
            has_more_data: true,
        }
    }

    pub(crate) async fn flush_done(&mut self) -> crate::Result<TokenDone> {
        loop {
            match self.try_next().await? {
                Some(ReceivedToken::Done(token)) => return Ok(token),
                Some(_) => (),
                None => return Err(crate::Error::Protocol("Never got DONE token.".into())),
            }
        }
    }

    #[cfg(windows)]
    pub(crate) async fn flush_sspi(&mut self) -> crate::Result<TokenSSPI> {
        loop {
            match self.try_next().await? {
                Some(ReceivedToken::SSPI(token)) => return Ok(token),
                Some(_) => (),
                None => return Err(crate::Error::Protocol("Never got SSPI token.".into())),
            }
        }
    }

    fn get_col_metadata(&mut self) -> crate::Result<ReceivedToken> {
        let meta = Arc::new(TokenColMetaData::decode(&mut self.buf)?);
        self.context.set_last_meta(meta.clone());
        event!(Level::TRACE, ?meta);
        Ok(ReceivedToken::NewResultset(meta))
    }

    fn get_row(&mut self) -> crate::Result<ReceivedToken> {
        let mut src = BytesData::new(&mut self.buf, self.context);
        let row = TokenRow::decode(&mut src)?;
        event!(Level::TRACE, message = ?row);
        Ok(ReceivedToken::Row(row))
    }

    fn get_return_value(&mut self) -> crate::Result<ReceivedToken> {
        let return_value = TokenReturnValue::decode(&mut self.buf)?;
        event!(Level::TRACE, message = ?return_value);
        Ok(ReceivedToken::ReturnValue(return_value))
    }

    fn get_return_status(&mut self) -> crate::Result<ReceivedToken> {
        let status = self.buf.get_u32_le();
        Ok(ReceivedToken::ReturnStatus(status))
    }

    fn get_error(&mut self) -> crate::Result<ReceivedToken> {
        let mut src = BytesData::new(&mut self.buf, self.context);
        let err = TokenError::decode(&mut src)?;
        event!(Level::ERROR, message = %err.message, code = err.code);
        Err(Error::Server(err))
    }

    fn get_order(&mut self) -> crate::Result<ReceivedToken> {
        let order = TokenOrder::decode(&mut self.buf)?;
        event!(Level::TRACE, message = ?order);
        Ok(ReceivedToken::Order(order))
    }

    fn get_done_value(&mut self) -> crate::Result<ReceivedToken> {
        let mut src = BytesData::new(&mut self.buf, self.context);
        let done = TokenDone::decode(&mut src)?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::Done(done))
    }

    fn get_done_proc_value(&mut self) -> crate::Result<ReceivedToken> {
        let mut src = BytesData::new(&mut self.buf, self.context);
        let done = TokenDone::decode(&mut src)?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::DoneProc(done))
    }

    fn get_done_in_proc_value(&mut self) -> crate::Result<ReceivedToken> {
        let mut src = BytesData::new(&mut self.buf, self.context);
        let done = TokenDone::decode(&mut src)?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::DoneInProc(done))
    }

    fn get_env_change(&mut self) -> crate::Result<ReceivedToken> {
        let change = TokenEnvChange::decode(&mut self.buf)?;

        if let TokenEnvChange::PacketSize(new_size, _) = change {
            self.context.packet_size.store(new_size, Ordering::SeqCst);
        };

        event!(Level::INFO, "{}", change);

        Ok(ReceivedToken::EnvChange(change))
    }

    fn get_info(&mut self) -> crate::Result<ReceivedToken> {
        let info = TokenInfo::decode(&mut self.buf)?;
        event!(Level::INFO, "{}", info.message);
        Ok(ReceivedToken::Info(info))
    }

    fn get_login_ack(&mut self) -> crate::Result<ReceivedToken> {
        let ack = TokenLoginAck::decode(&mut self.buf)?;
        event!(Level::INFO, "{} version {}", ack.prog_name, ack.version);
        Ok(ReceivedToken::LoginAck(ack))
    }

    #[cfg(windows)]
    fn get_sspi(&mut self) -> crate::Result<ReceivedToken> {
        let sspi = TokenSSPI::decode(&mut self.buf)?;
        event!(Level::INFO, "SSPI response");
        Ok(ReceivedToken::SSPI(sspi))
    }

    fn fetch_packet(&mut self, cx: &mut task::Context<'_>) -> Poll<Option<crate::Result<()>>> {
        if self.has_more_data {
            match ready!(self.packet_stream.as_mut().try_poll_next(cx)?) {
                Some(packet) => {
                    self.has_more_data = !packet.is_last();
                    let (_, payload) = packet.into_parts();
                    self.buf.extend(payload);

                    Poll::Ready(Some(Ok(())))
                }
                _ => Poll::Ready(None),
            }
        } else {
            Poll::Ready(None)
        }
    }

    fn enough_data_for(&self, ty: TokenType) -> bool {
        use TokenType::*;

        match ty {
            ReturnStatus => self.buf.len() >= 4,
            Error | Info | Order | ColInfo | ReturnValue | LoginAck | SSPI | EnvChange
            | ColMetaData => {
                let len = (&self.buf[0..2]).get_u16_le() as usize + 2;
                self.buf.len() >= len
            }
            Row => {
                let len = self.context.row_size().unwrap();
                self.buf.len() >= len
            }
            Done | DoneProc | DoneInProc => {
                let len = self.context.version.done_row_count_bytes() as usize + 4;
                self.buf.len() >= len
            }
            x => panic!("Token type {:?} not supported", x),
        }
    }
}

impl<'a, S> Stream for TokenStream<'a, S>
where
    S: Stream<Item = crate::Result<Packet>> + Unpin + 'a,
{
    type Item = crate::Result<ReceivedToken>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if !this.has_more_data && this.buf.is_empty() {
            return Poll::Ready(None);
        }

        if this.buf.is_empty() {
            ready!(this.fetch_packet(cx));
        }

        let ty_byte = this.buf.get_u8();

        let ty = TokenType::try_from(ty_byte)
            .map_err(|_| Error::Protocol(format!("invalid token type {:x}", ty_byte).into()))?;

        while this.has_more_data && !this.enough_data_for(ty) {
            ready!(this.fetch_packet(cx));
        }

        match ty {
            TokenType::ReturnStatus => Poll::Ready(Some(this.get_return_status())),
            TokenType::ColMetaData => Poll::Ready(Some(this.get_col_metadata())),
            TokenType::Row => Poll::Ready(Some(this.get_row())),
            TokenType::Done => Poll::Ready(Some(this.get_done_value())),
            TokenType::DoneProc => Poll::Ready(Some(this.get_done_proc_value())),
            TokenType::DoneInProc => Poll::Ready(Some(this.get_done_in_proc_value())),
            TokenType::ReturnValue => Poll::Ready(Some(this.get_return_value())),
            TokenType::Error => Poll::Ready(Some(this.get_error())),
            TokenType::Order => Poll::Ready(Some(this.get_order())),
            TokenType::EnvChange => Poll::Ready(Some(this.get_env_change())),
            TokenType::Info => Poll::Ready(Some(this.get_info())),
            TokenType::LoginAck => Poll::Ready(Some(this.get_login_ack())),
            #[cfg(windows)]
            TokenType::SSPI => Poll::Ready(Some(this.get_sspi())),
            _ => panic!("Token {:?} unimplemented!", ty),
        }
    }
}
