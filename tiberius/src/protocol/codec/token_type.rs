use super::Decode;
use crate::{uint_enum, Error};
use bytes::{Buf, BytesMut};
use std::convert::TryFrom;

uint_enum! {
    /// Types of tokens in a token stream. Read from the first byte of the stream.
    pub enum TokenType {
        /// Used to send the status value of an RPC to the client. The server
        /// also uses this token to send the result status value of a stored
        /// procedure executed through SQL Batch.
        ///
        /// This token MUST be returned to the client when an RPC is executed by
        /// the server.
        ///
        /// Length: 4 bytes (u32)
        ReturnStatus = 0x79,
        ColMetaData = 0x81,
        /// Used to send an error message to the client.
        ///
        /// Length: as u16
        Error = 0xAA,
        /// Used to send an information message to the client.
        ///
        /// Length: as u16
        Info = 0xAB,
        /// Used to inform the client by which columns the data is ordered.
        ///
        /// Length: as u16
        Order = 0xA9,
        /// Describes the column information in browse mode.
        ///
        /// Length: as u16
        ColInfo = 0xA5,
        /// Used to send the return value of an RPCto the client. When an RPC is
        /// executed, the associated parameters may be defined as input or
        /// output (or "return") parameters.
        ///
        /// This token is used to send a description of the return parameter to
        /// the client. This token is also used to describe the value returned
        /// by a user-defined function (UDF) when executed as an RPC.
        ///
        /// Length: as u16
        ReturnValue = 0xAC,
        /// Used to send a response to a login request to the client.
        ///
        /// Length: as u16
        LoginAck = 0xAD,
        /// Used to send a complete row, as defined by the COLNAME and COLFMT
        /// tokens, to the client.
        ///
        /// Length: Calculated from the column metadata.
        Row = 0xD1,
        NbcRow = 0xD2,
        /// The SSPI token returned during the login process.
        ///
        /// Length: as u16
        SSPI = 0xED,
        /// A notification of an environment change (such as database and
        /// language).
        ///
        /// Length: as u16
        EnvChange = 0xE3,
        /// Indicates the completion status of a SQL statement.
        ///
        /// This token is used to indicate the completion of a SQL statement.
        /// Because multiple SQL statements may be sent to the server in a
        /// single SQL batch, multiple DONE tokens may be generated. In this
        /// case, all but the final DONE token will have a Status value with the
        /// DONE_MORE bit set.
        ///
        /// A DONE token is returned for each SQL statement in the SQL batch,
        /// except for variable declarations.
        ///
        /// For execution of SQL statements within stored procedures, DONEPROC
        /// and DONEINPROC tokens are used in place of DONE tokens.
        ///
        /// Length: 8 or 12 bytes (after SQL Server 2005)
        Done = 0xFD,
        /// Indicates the completion status of a stored procedure. This is also
        /// generated for stored procedures executed through SQL statements.
        ///
        /// Length: 8 or 12 bytes (after SQL Server 2005)
        DoneProc = 0xFE,
        /// Indicates the completion status of a SQL statement within a stored procedure.
        ///
        /// Length: 8 or 12 bytes (after SQL Server 2005)
        DoneInProc = 0xFF,
    }
}

impl Decode<BytesMut> for TokenType {
    fn decode(src: &mut BytesMut) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let ty_byte = src.get_u8();

        let ty = TokenType::try_from(ty_byte)
            .map_err(|_| Error::Protocol(format!("invalid token type {:x}", ty_byte).into()))?;

        Ok(ty)
    }
}
