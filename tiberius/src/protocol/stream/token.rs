#[cfg(windows)]
use super::codec::TokenSSPI;
use crate::{
    async_read_le_ext::AsyncReadLeExt,
    protocol::{
        codec::{
            TokenColMetaData, TokenDone, TokenEnvChange, TokenError, TokenInfo, TokenLoginAck,
            TokenOrder, TokenReturnValue, TokenRow,
        },
        Context,
    },
    Error, TokenType,
};
use futures::{Stream, TryStreamExt};
use std::{
    convert::TryFrom,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
};
use tokio::io::AsyncReadExt;
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
    conn: &'a mut S,
    context: Arc<Context>,
    row_mode: bool,
}

impl<'a, S> TokenStream<'a, S>
where
    S: AsyncReadLeExt + Unpin + 'a,
{
    pub(crate) fn new(conn: &'a mut S, context: Arc<Context>) -> Self {
        Self {
            conn,
            context,
            row_mode: false,
        }
    }

    pub(crate) async fn flush_done(self) -> crate::Result<TokenDone> {
        let mut stream = self.try_unfold();
        let mut stream = unsafe { Pin::new_unchecked(&mut *stream) };

        loop {
            match stream.try_next().await? {
                Some(ReceivedToken::Done(token)) => return Ok(token),
                Some(_) => (),
                None => return Err(crate::Error::Protocol("Never got DONE token.".into())),
            }
        }
    }

    #[cfg(windows)]
    pub(crate) async fn flush_sspi(self) -> crate::Result<TokenSSPI> {
        let mut stream = self.try_unfold();
        let mut stream = unsafe { Pin::new_unchecked(&mut *stream) };

        loop {
            match stream.try_next().await? {
                Some(ReceivedToken::SSPI(token)) => return Ok(token),
                Some(_) => (),
                None => return Err(crate::Error::Protocol("Never got SSPI token.".into())),
            }
        }
    }

    async fn get_col_metadata(&mut self) -> crate::Result<ReceivedToken> {
        let meta = Arc::new(TokenColMetaData::decode(self.conn).await?);
        self.context.set_last_meta(meta.clone());

        event!(Level::TRACE, ?meta);

        Ok(ReceivedToken::NewResultset(meta))
    }

    async fn get_row(&mut self) -> crate::Result<ReceivedToken> {
        self.row_mode = true;
        let return_value = TokenRow::decode(self.conn, &self.context).await?;
        self.row_mode = false;

        event!(Level::TRACE, message = ?return_value);
        Ok(ReceivedToken::Row(return_value))
    }

    async fn get_return_value(&mut self) -> crate::Result<ReceivedToken> {
        let return_value = TokenReturnValue::decode(self.conn).await?;
        event!(Level::TRACE, message = ?return_value);
        Ok(ReceivedToken::ReturnValue(return_value))
    }

    async fn get_return_status(&mut self) -> crate::Result<ReceivedToken> {
        let status = self.conn.read_u32_le().await?;
        Ok(ReceivedToken::ReturnStatus(status))
    }

    async fn get_error(&mut self) -> crate::Result<ReceivedToken> {
        let err = TokenError::decode(self.conn, &self.context).await?;
        event!(Level::ERROR, message = %err.message, code = err.code);
        Err(Error::Server(err))
    }

    async fn get_order(&mut self) -> crate::Result<ReceivedToken> {
        let order = TokenOrder::decode(self.conn).await?;
        event!(Level::TRACE, message = ?order);
        Ok(ReceivedToken::Order(order))
    }

    async fn get_done_value(&mut self) -> crate::Result<ReceivedToken> {
        let done = TokenDone::decode(self.conn, &*self.context).await?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::Done(done))
    }

    async fn get_done_proc_value(&mut self) -> crate::Result<ReceivedToken> {
        let done = TokenDone::decode(self.conn, &*self.context).await?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::DoneProc(done))
    }

    async fn get_done_in_proc_value(&mut self) -> crate::Result<ReceivedToken> {
        let done = TokenDone::decode(self.conn, &*self.context).await?;
        event!(Level::TRACE, "{}", done);
        Ok(ReceivedToken::DoneInProc(done))
    }

    async fn get_env_change(&mut self) -> crate::Result<ReceivedToken> {
        let change = TokenEnvChange::decode(self.conn).await?;

        if let TokenEnvChange::PacketSize(new_size, _) = change {
            self.context.packet_size.store(new_size, Ordering::SeqCst);
        };

        event!(Level::INFO, "{}", change);

        Ok(ReceivedToken::EnvChange(change))
    }

    async fn get_info(&mut self) -> crate::Result<ReceivedToken> {
        let info = TokenInfo::decode(self.conn).await?;
        event!(Level::INFO, "{}", info.message);
        Ok(ReceivedToken::Info(info))
    }

    async fn get_login_ack(&mut self) -> crate::Result<ReceivedToken> {
        let ack = TokenLoginAck::decode(self.conn).await?;
        event!(Level::INFO, "{} version {}", ack.prog_name, ack.version);
        Ok(ReceivedToken::LoginAck(ack))
    }

    #[cfg(windows)]
    async fn get_sspi(&mut self) -> crate::Result<ReceivedToken> {
        let sspi = TokenSSPI::decode(self.conn).await?;
        event!(Level::INFO, "SSPI response");
        Ok(ReceivedToken::SSPI(sspi))
    }

    pub fn try_unfold(self) -> Box<dyn Stream<Item = crate::Result<ReceivedToken>> + 'a> {
        let s = futures::stream::try_unfold((self, true), |(mut this, more_data)| async move {
            if !more_data {
                return Ok(None);
            }

            let ty_byte = this.conn.read_u8().await?;

            let ty = TokenType::try_from(ty_byte)
                .map_err(|_| Error::Protocol(format!("invalid token type {:x}", ty_byte).into()))?;

            let token = match ty {
                TokenType::ReturnStatus => this.get_return_status().await?,
                TokenType::ColMetaData => this.get_col_metadata().await?,
                TokenType::Row => this.get_row().await?,
                TokenType::Done => {
                    let result = this.get_done_value().await?;
                    return Ok(Some((result, (this, false))));
                }
                TokenType::DoneProc => this.get_done_proc_value().await?,
                TokenType::DoneInProc => this.get_done_in_proc_value().await?,
                TokenType::ReturnValue => this.get_return_value().await?,
                TokenType::Error => this.get_error().await?,
                TokenType::Order => this.get_order().await?,
                TokenType::EnvChange => this.get_env_change().await?,
                TokenType::Info => this.get_info().await?,
                TokenType::LoginAck => this.get_login_ack().await?,
                #[cfg(windows)]
                TokenType::SSPI => this.get_sspi().await?,
                _ => panic!("Token {:?} unimplemented!", ty),
            };

            Ok(Some((token, (this, true))))
        });

        Box::new(s)
    }
}
