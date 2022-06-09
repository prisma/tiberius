use asynchronous_codec::BytesMut;
use enumflags2::BitFlags;
use futures::{AsyncRead, AsyncWrite};
use tracing::{event, Level};

use crate::{client::Connection, sql_read_bytes::SqlReadBytes, ExecuteResult};

use super::{
    BaseMetaDataColumn, ColumnFlag, Encode, MetaDataColumn, PacketHeader, PacketStatus,
    TokenColMetaData, TokenDone, TokenRow, TypeInfo, HEADER_BYTES,
};

/// Column metadata for a bulk load request.
#[derive(Debug, Default, Clone)]
pub struct BulkLoadMetadata<'a> {
    columns: Vec<MetaDataColumn<'a>>,
}

impl<'a> BulkLoadMetadata<'a> {
    /// Creates a metadata with no columns specified.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column to the request. Order should be same as the order of data
    /// in the rows.
    pub fn add_column<C>(&mut self, name: &'a str, ty: TypeInfo, flags: C)
    where
        C: Into<BitFlags<ColumnFlag>>,
    {
        self.columns.push(MetaDataColumn {
            base: BaseMetaDataColumn {
                flags: flags.into(),
                ty,
            },
            col_name: name.into(),
        });
    }

    pub(crate) fn column_descriptions(&self) -> impl Iterator<Item = String> + '_ {
        self.columns.iter().map(|c| format!("{}", c))
    }
}

impl<'a> Encode<BytesMut> for BulkLoadMetadata<'a> {
    fn encode(self, dst: &mut BytesMut) -> crate::Result<()> {
        let cmd = TokenColMetaData {
            columns: self.columns,
        };

        cmd.encode(dst)
    }
}

/// A handler for a bulk insert data flow.
#[derive(Debug)]
pub struct BulkLoadRequest<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    connection: &'a mut Connection<S>,
    packet_id: u8,
    buf: BytesMut,
}

impl<'a, S> BulkLoadRequest<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub(crate) fn new(
        connection: &'a mut Connection<S>,
        meta: BulkLoadMetadata<'a>,
    ) -> crate::Result<Self> {
        let packet_id = connection.context_mut().next_packet_id();
        let mut buf = BytesMut::new();

        meta.encode(&mut buf)?;

        let this = Self {
            connection,
            packet_id,
            buf,
        };

        Ok(this)
    }

    /// Adds a new row to the bulk insert, flushing only when having a full packet of data.
    ///
    /// # Warning
    ///
    /// After the last row, [`finalize`] must be called to flush the buffered
    /// data and for the data to actually be available in the table.
    ///
    /// [`finalize`]: #method.finalize
    pub async fn send(&mut self, row: TokenRow<'a>) -> crate::Result<()> {
        let packet_size = (self.connection.context().packet_size() as usize) - HEADER_BYTES;

        row.encode(&mut self.buf)?;

        while dbg!(self.buf.len()) >= dbg!(packet_size) {
            let header = PacketHeader::bulk_load(self.packet_id);
            let data = self.buf.split_to(packet_size);

            event!(
                Level::TRACE,
                "Bulk insert packet ({} bytes)",
                data.len() + HEADER_BYTES,
            );

            self.connection.write_to_wire(header, data).await?;
        }

        Ok(())
    }

    /// Ends the bulk load, flushing all pending data to the wire.
    ///
    /// This method must be called after sending all the data to flush all
    /// pending data and to get the server actually to store the rows to the
    /// table.
    pub async fn finalize(mut self) -> crate::Result<ExecuteResult> {
        TokenDone::default().encode(&mut self.buf)?;

        let mut header = PacketHeader::bulk_load(self.packet_id);
        header.set_status(PacketStatus::EndOfMessage);

        let data = self.buf.split();

        event!(
            Level::TRACE,
            "Finalizing a bulk insert ({} bytes)",
            data.len() + HEADER_BYTES,
        );

        dbg!(self.connection.write_to_wire(header, data).await)?;
        dbg!(self.connection.flush_sink().await)?;

        ExecuteResult::new(self.connection).await
    }
}
