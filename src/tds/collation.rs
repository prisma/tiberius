///! legacy implementation of collations (or codepages rather) for dealing with varchar's with legacy databases
///! references [1] which has some mappings from the katmai (SQL Server 2008) source code and is a TDS driver
///! directly from microsoft
///! [2] is helpful to map CP1234 to the appropriate encoding
///!
///! [1] https://github.com/Microsoft/mssql-jdbc/blob/eb14f63077c47ef1fc1c690deb8cfab602baeb85/src/main/java/com/microsoft/sqlserver/jdbc/SQLCollation.java
///! [2] https://github.com/lifthrasiir/rust-encoding/blob/496823171f15d9b9446b2ec3fb7765f22346256b/src/label.rs#L282
use encoding::{self, Encoding};

#[derive(Debug, Clone, Copy)]
pub struct Collation {
    /// LCID ColFlags Version
    info: u32,
    /// Sortid
    sort_id: u8,
}

impl Collation {
    pub fn new(info: u32, sort_id: u8) -> Self {
        Self { info, sort_id }
    }

    /// return the locale id part of the LCID (the specification here uses ambiguous terms)
    pub fn lcid(&self) -> u16 {
        (self.info & 0xffff) as u16
    }

    /// return an encoding for a given collation
    pub fn encoding(&self) -> Option<&'static dyn Encoding> {
        if self.sort_id == 0 {
            lcid_to_encoding(self.lcid())
        } else {
            sortid_to_encoding(self.sort_id)
        }
    }
}

/// https://github.com/Microsoft/mssql-jdbc/blob/eb14f63077c47ef1fc1c690deb8cfab602baeb85/src/main/java/com/microsoft/sqlserver/jdbc/SQLCollation.java#L102-L310
/// maps an LCID (it's locale part which is only 2 bytes) to a codepage
///
/// generate the code below from source code:
/// 1. (regex)replace: (.*?)\((.*?),(.*?)\) with $2 => $3
/// 2. replace: Encoding.CP(.*?) with encoding::all::WINDOWS_$1
/// 3. replace: Encoding.UNICODE with encoding::all::UTF16_LE
//
/// the unimplemented!() one's are not supported by rust-encoding
pub fn lcid_to_encoding(locale: u16) -> Option<&'static dyn Encoding> {
    match locale {
        0x0401 => Some(encoding::all::WINDOWS_1256),
        0x0402 => Some(encoding::all::WINDOWS_1251),
        0x0403 => Some(encoding::all::WINDOWS_1252),
        // CP950
        0x0404 | 0x0c04 | 0x1404 => Some(encoding::all::BIG5_2003),
        0x0405 => Some(encoding::all::WINDOWS_1250),
        0x0406 => Some(encoding::all::WINDOWS_1252),
        0x0407 => Some(encoding::all::WINDOWS_1252),
        0x0408 => Some(encoding::all::WINDOWS_1253),
        0x0409 => Some(encoding::all::WINDOWS_1252),
        0x040a => Some(encoding::all::WINDOWS_1252),
        0x040b => Some(encoding::all::WINDOWS_1252),
        0x040c => Some(encoding::all::WINDOWS_1252),
        0x040d => Some(encoding::all::WINDOWS_1255),
        0x040e => Some(encoding::all::WINDOWS_1250),
        0x040f => Some(encoding::all::WINDOWS_1252),
        0x0410 => Some(encoding::all::WINDOWS_1252),
        // CP932
        0x0411 => Some(encoding::all::WINDOWS_31J),
        0x0412 => Some(encoding::all::WINDOWS_949),
        0x0413 => Some(encoding::all::WINDOWS_1252),
        0x0414 => Some(encoding::all::WINDOWS_1252),
        0x0415 => Some(encoding::all::WINDOWS_1250),
        0x0416 => Some(encoding::all::WINDOWS_1252),
        0x0417 => Some(encoding::all::WINDOWS_1252),
        0x0418 => Some(encoding::all::WINDOWS_1250),
        0x0419 => Some(encoding::all::WINDOWS_1251),
        0x041a => Some(encoding::all::WINDOWS_1250),
        0x041b => Some(encoding::all::WINDOWS_1250),
        0x041c => Some(encoding::all::WINDOWS_1250),
        0x041d => Some(encoding::all::WINDOWS_1252),
        0x041e => Some(encoding::all::WINDOWS_874),
        0x041f => Some(encoding::all::WINDOWS_1254),
        0x0420 => Some(encoding::all::WINDOWS_1256),
        0x0421 => Some(encoding::all::WINDOWS_1252),
        0x0422 => Some(encoding::all::WINDOWS_1251),
        0x0423 => Some(encoding::all::WINDOWS_1251),
        0x0424 => Some(encoding::all::WINDOWS_1250),
        0x0425 => Some(encoding::all::WINDOWS_1257),
        0x0426 => Some(encoding::all::WINDOWS_1257),
        0x0427 => Some(encoding::all::WINDOWS_1257),
        0x0428 => Some(encoding::all::WINDOWS_1251),
        0x0429 => Some(encoding::all::WINDOWS_1256),
        0x042a => Some(encoding::all::WINDOWS_1258),
        0x042b => Some(encoding::all::WINDOWS_1252),
        0x042c => Some(encoding::all::WINDOWS_1254),
        0x042d => Some(encoding::all::WINDOWS_1252),
        0x042e => Some(encoding::all::WINDOWS_1252),
        0x042f => Some(encoding::all::WINDOWS_1251),
        0x0432 => Some(encoding::all::WINDOWS_1252),
        0x0434 => Some(encoding::all::WINDOWS_1252),
        0x0435 => Some(encoding::all::WINDOWS_1252),
        0x0436 => Some(encoding::all::WINDOWS_1252),
        0x0437 => Some(encoding::all::WINDOWS_1252),
        0x0438 => Some(encoding::all::WINDOWS_1252),
        0x0439 => Some(encoding::all::UTF_16LE),
        0x043a => Some(encoding::all::UTF_16LE),
        0x043b => Some(encoding::all::WINDOWS_1252),
        0x043e => Some(encoding::all::WINDOWS_1252),
        0x043f => Some(encoding::all::WINDOWS_1251),
        0x0440 => Some(encoding::all::WINDOWS_1251),
        0x0441 => Some(encoding::all::WINDOWS_1252),
        0x0442 => Some(encoding::all::WINDOWS_1250),
        0x0443 => Some(encoding::all::WINDOWS_1254),
        0x0444 => Some(encoding::all::WINDOWS_1251),
        0x0445 => Some(encoding::all::UTF_16LE),
        0x0446 => Some(encoding::all::UTF_16LE),
        0x0447 => Some(encoding::all::UTF_16LE),
        0x0448 => Some(encoding::all::UTF_16LE),
        0x0449 => Some(encoding::all::UTF_16LE),
        0x044a => Some(encoding::all::UTF_16LE),
        0x044b => Some(encoding::all::UTF_16LE),
        0x044c => Some(encoding::all::UTF_16LE),
        0x044d => Some(encoding::all::UTF_16LE),
        0x044e => Some(encoding::all::UTF_16LE),
        0x044f => Some(encoding::all::UTF_16LE),
        0x0450 => Some(encoding::all::WINDOWS_1251),
        0x0451 => Some(encoding::all::UTF_16LE),
        0x0452 => Some(encoding::all::WINDOWS_1252),
        0x0453 => Some(encoding::all::UTF_16LE),
        0x0454 => Some(encoding::all::UTF_16LE),
        0x0456 => Some(encoding::all::WINDOWS_1252),
        0x0457 => Some(encoding::all::UTF_16LE),
        0x045a => Some(encoding::all::UTF_16LE),
        0x045b => Some(encoding::all::UTF_16LE),
        0x045d => Some(encoding::all::WINDOWS_1252),
        0x045e => Some(encoding::all::WINDOWS_1252),
        0x0461 => Some(encoding::all::UTF_16LE),
        0x0462 => Some(encoding::all::WINDOWS_1252),
        0x0463 => Some(encoding::all::UTF_16LE),
        0x0464 => Some(encoding::all::WINDOWS_1252),
        0x0465 => Some(encoding::all::UTF_16LE),
        0x0468 => Some(encoding::all::WINDOWS_1252),
        0x046a => Some(encoding::all::WINDOWS_1252),
        0x046b => Some(encoding::all::WINDOWS_1252),
        0x046c => Some(encoding::all::WINDOWS_1252),
        0x046d => Some(encoding::all::WINDOWS_1251),
        0x046e => Some(encoding::all::WINDOWS_1252),
        0x046f => Some(encoding::all::WINDOWS_1252),
        0x0470 => Some(encoding::all::WINDOWS_1252),
        0x0478 => Some(encoding::all::WINDOWS_1252),
        0x047a => Some(encoding::all::WINDOWS_1252),
        0x047c => Some(encoding::all::WINDOWS_1252),
        0x047e => Some(encoding::all::WINDOWS_1252),
        0x0480 => Some(encoding::all::WINDOWS_1256),
        0x0481 => Some(encoding::all::UTF_16LE),
        0x0482 => Some(encoding::all::WINDOWS_1252),
        0x0483 => Some(encoding::all::WINDOWS_1252),
        0x0484 => Some(encoding::all::WINDOWS_1252),
        0x0485 => Some(encoding::all::WINDOWS_1251),
        0x0486 => Some(encoding::all::WINDOWS_1252),
        0x0487 => Some(encoding::all::WINDOWS_1252),
        0x0488 => Some(encoding::all::WINDOWS_1252),
        0x048c => Some(encoding::all::WINDOWS_1256),
        0x0801 => Some(encoding::all::WINDOWS_1256),
        // CP936
        0x0804 | 0x1004 => Some(encoding::all::GB18030),
        0x0807 => Some(encoding::all::WINDOWS_1252),
        0x0809 => Some(encoding::all::WINDOWS_1252),
        0x080a => Some(encoding::all::WINDOWS_1252),
        0x080c => Some(encoding::all::WINDOWS_1252),
        0x0810 => Some(encoding::all::WINDOWS_1252),
        0x0813 => Some(encoding::all::WINDOWS_1252),
        0x0814 => Some(encoding::all::WINDOWS_1252),
        0x0816 => Some(encoding::all::WINDOWS_1252),
        0x081a => Some(encoding::all::WINDOWS_1250),
        0x081d => Some(encoding::all::WINDOWS_1252),
        0x0827 => Some(encoding::all::WINDOWS_1257),
        0x082c => Some(encoding::all::WINDOWS_1251),
        0x082e => Some(encoding::all::WINDOWS_1252),
        0x083b => Some(encoding::all::WINDOWS_1252),
        0x083c => Some(encoding::all::WINDOWS_1252),
        0x083e => Some(encoding::all::WINDOWS_1252),
        0x0843 => Some(encoding::all::WINDOWS_1251),
        0x0845 => Some(encoding::all::UTF_16LE),
        0x0850 => Some(encoding::all::WINDOWS_1251),
        0x085d => Some(encoding::all::WINDOWS_1252),
        0x085f => Some(encoding::all::WINDOWS_1252),
        0x086b => Some(encoding::all::WINDOWS_1252),
        0x0c01 => Some(encoding::all::WINDOWS_1256),
        0x0c07 => Some(encoding::all::WINDOWS_1252),
        0x0c09 => Some(encoding::all::WINDOWS_1252),
        0x0c0a => Some(encoding::all::WINDOWS_1252),
        0x0c0c => Some(encoding::all::WINDOWS_1252),
        0x0c1a => Some(encoding::all::WINDOWS_1251),
        0x0c3b => Some(encoding::all::WINDOWS_1252),
        0x0c6b => Some(encoding::all::WINDOWS_1252),
        0x1001 => Some(encoding::all::WINDOWS_1256),
        0x1007 => Some(encoding::all::WINDOWS_1252),
        0x1009 => Some(encoding::all::WINDOWS_1252),
        0x100a => Some(encoding::all::WINDOWS_1252),
        0x100c => Some(encoding::all::WINDOWS_1252),
        0x101a => Some(encoding::all::WINDOWS_1250),
        0x103b => Some(encoding::all::WINDOWS_1252),
        0x1401 => Some(encoding::all::WINDOWS_1256),
        0x1407 => Some(encoding::all::WINDOWS_1252),
        0x1409 => Some(encoding::all::WINDOWS_1252),
        0x140a => Some(encoding::all::WINDOWS_1252),
        0x140c => Some(encoding::all::WINDOWS_1252),
        0x141a => Some(encoding::all::WINDOWS_1250),
        0x143b => Some(encoding::all::WINDOWS_1252),
        0x1801 => Some(encoding::all::WINDOWS_1256),
        0x1809 => Some(encoding::all::WINDOWS_1252),
        0x180a => Some(encoding::all::WINDOWS_1252),
        0x180c => Some(encoding::all::WINDOWS_1252),
        0x181a => Some(encoding::all::WINDOWS_1250),
        0x183b => Some(encoding::all::WINDOWS_1252),
        0x1c01 => Some(encoding::all::WINDOWS_1256),
        0x1c09 => Some(encoding::all::WINDOWS_1252),
        0x1c0a => Some(encoding::all::WINDOWS_1252),
        0x1c1a => Some(encoding::all::WINDOWS_1251),
        0x1c3b => Some(encoding::all::WINDOWS_1252),
        0x2001 => Some(encoding::all::WINDOWS_1256),
        0x2009 => Some(encoding::all::WINDOWS_1252),
        0x200a => Some(encoding::all::WINDOWS_1252),
        0x201a => Some(encoding::all::WINDOWS_1251),
        0x203b => Some(encoding::all::WINDOWS_1252),
        0x2401 => Some(encoding::all::WINDOWS_1256),
        0x2409 => Some(encoding::all::WINDOWS_1252),
        0x240a => Some(encoding::all::WINDOWS_1252),
        0x243b => Some(encoding::all::WINDOWS_1252),
        0x2801 => Some(encoding::all::WINDOWS_1256),
        0x2809 => Some(encoding::all::WINDOWS_1252),
        0x280a => Some(encoding::all::WINDOWS_1252),
        0x2c01 => Some(encoding::all::WINDOWS_1256),
        0x2c09 => Some(encoding::all::WINDOWS_1252),
        0x2c0a => Some(encoding::all::WINDOWS_1252),
        0x3001 => Some(encoding::all::WINDOWS_1256),
        0x3009 => Some(encoding::all::WINDOWS_1252),
        0x300a => Some(encoding::all::WINDOWS_1252),
        0x3401 => Some(encoding::all::WINDOWS_1256),
        0x3409 => Some(encoding::all::WINDOWS_1252),
        0x340a => Some(encoding::all::WINDOWS_1252),
        0x3801 => Some(encoding::all::WINDOWS_1256),
        0x380a => Some(encoding::all::WINDOWS_1252),
        0x3c01 => Some(encoding::all::WINDOWS_1256),
        0x3c0a => Some(encoding::all::WINDOWS_1252),
        0x4001 => Some(encoding::all::WINDOWS_1256),
        0x4009 => Some(encoding::all::WINDOWS_1252),
        0x400a => Some(encoding::all::WINDOWS_1252),
        0x4409 => Some(encoding::all::WINDOWS_1252),
        0x440a => Some(encoding::all::WINDOWS_1252),
        0x4809 => Some(encoding::all::WINDOWS_1252),
        0x480a => Some(encoding::all::WINDOWS_1252),
        0x4c0a => Some(encoding::all::WINDOWS_1252),
        0x500a => Some(encoding::all::WINDOWS_1252),
        0x540a => Some(encoding::all::WINDOWS_1252),
        _ => None,
    }
}

/// [1] https://github.com/Microsoft/mssql-jdbc/blob/eb14f63077c47ef1fc1c690deb8cfab602baeb85/src/main/java/com/microsoft/sqlserver/jdbc/SQLCollation.java#L362-L482
/// [2] https://msdn.microsoft.com/de-de/library/ms144250(v=sql.105).aspx
///
/// [2] does only contain 3/4 of the content [1] contains, so the source code is again the better source of information
///
/// generate the code below from source code:
/// 1. (regex)replace .*\((.*?),.*?,(.*?)\) with $1 => $2
/// 2. see above/as above
pub fn sortid_to_encoding(sort_id: u8) -> Option<&'static dyn Encoding> {
    match sort_id {
        // 30 | 31 | 32 | 33 | 34 | 35 => Some(encoding::all::WINDOWS_437),
        // 40 | 41 | 42 | 43 | 44 | 45 | 49 => Some(encoding::all::WINDOWS_850),
        50 => Some(encoding::all::WINDOWS_1252),
        51 => Some(encoding::all::WINDOWS_1252),
        52 => Some(encoding::all::WINDOWS_1252),
        53 => Some(encoding::all::WINDOWS_1252),
        54 => Some(encoding::all::WINDOWS_1252),
        // 55 | 56 | 57 | 58 | 59 | 60 | 61 => Some(encoding::all::WINDOWS_850),
        71 => Some(encoding::all::WINDOWS_1252),
        72 => Some(encoding::all::WINDOWS_1252),
        73 => Some(encoding::all::WINDOWS_1252),
        74 => Some(encoding::all::WINDOWS_1252),
        75 => Some(encoding::all::WINDOWS_1252),
        80 => Some(encoding::all::WINDOWS_1250),
        81 => Some(encoding::all::WINDOWS_1250),
        82 => Some(encoding::all::WINDOWS_1250),
        83 => Some(encoding::all::WINDOWS_1250),
        84 => Some(encoding::all::WINDOWS_1250),
        85 => Some(encoding::all::WINDOWS_1250),
        86 => Some(encoding::all::WINDOWS_1250),
        87 => Some(encoding::all::WINDOWS_1250),
        88 => Some(encoding::all::WINDOWS_1250),
        89 => Some(encoding::all::WINDOWS_1250),
        90 => Some(encoding::all::WINDOWS_1250),
        91 => Some(encoding::all::WINDOWS_1250),
        92 => Some(encoding::all::WINDOWS_1250),
        93 => Some(encoding::all::WINDOWS_1250),
        94 => Some(encoding::all::WINDOWS_1250),
        95 => Some(encoding::all::WINDOWS_1250),
        96 => Some(encoding::all::WINDOWS_1250),
        97 => Some(encoding::all::WINDOWS_1250),
        98 => Some(encoding::all::WINDOWS_1250),
        104 => Some(encoding::all::WINDOWS_1251),
        105 => Some(encoding::all::WINDOWS_1251),
        106 => Some(encoding::all::WINDOWS_1251),
        107 => Some(encoding::all::WINDOWS_1251),
        108 => Some(encoding::all::WINDOWS_1251),
        112 => Some(encoding::all::WINDOWS_1253),
        113 => Some(encoding::all::WINDOWS_1253),
        114 => Some(encoding::all::WINDOWS_1253),
        120 => Some(encoding::all::WINDOWS_1253),
        121 => Some(encoding::all::WINDOWS_1253),
        122 => Some(encoding::all::WINDOWS_1253),
        124 => Some(encoding::all::WINDOWS_1253),
        128 => Some(encoding::all::WINDOWS_1254),
        129 => Some(encoding::all::WINDOWS_1254),
        130 => Some(encoding::all::WINDOWS_1254),
        136 => Some(encoding::all::WINDOWS_1255),
        137 => Some(encoding::all::WINDOWS_1255),
        138 => Some(encoding::all::WINDOWS_1255),
        144 => Some(encoding::all::WINDOWS_1256),
        145 => Some(encoding::all::WINDOWS_1256),
        146 => Some(encoding::all::WINDOWS_1256),
        152 => Some(encoding::all::WINDOWS_1257),
        153 => Some(encoding::all::WINDOWS_1257),
        154 => Some(encoding::all::WINDOWS_1257),
        155 => Some(encoding::all::WINDOWS_1257),
        156 => Some(encoding::all::WINDOWS_1257),
        157 => Some(encoding::all::WINDOWS_1257),
        158 => Some(encoding::all::WINDOWS_1257),
        159 => Some(encoding::all::WINDOWS_1257),
        160 => Some(encoding::all::WINDOWS_1257),
        183 => Some(encoding::all::WINDOWS_1252),
        184 => Some(encoding::all::WINDOWS_1252),
        185 => Some(encoding::all::WINDOWS_1252),
        186 => Some(encoding::all::WINDOWS_1252),
        // CP 932
        192 | 193 | 200 => Some(encoding::all::WINDOWS_31J),
        194 => Some(encoding::all::WINDOWS_949),
        195 => Some(encoding::all::WINDOWS_949),
        // CP950
        196 | 197 | 202 => Some(encoding::all::BIG5_2003),
        // CP936 (GB18030 is an extension of it with more chars), should be backwards-compatible)
        198 | 199 | 203 => Some(encoding::all::GB18030),
        201 => Some(encoding::all::WINDOWS_949),
        204 => Some(encoding::all::WINDOWS_874),
        205 => Some(encoding::all::WINDOWS_874),
        206 => Some(encoding::all::WINDOWS_874),
        210 => Some(encoding::all::WINDOWS_1252),
        211 => Some(encoding::all::WINDOWS_1252),
        212 => Some(encoding::all::WINDOWS_1252),
        213 => Some(encoding::all::WINDOWS_1252),
        214 => Some(encoding::all::WINDOWS_1252),
        215 => Some(encoding::all::WINDOWS_1252),
        216 => Some(encoding::all::WINDOWS_1252),
        217 => Some(encoding::all::WINDOWS_1252),
        _ => None,
    }
}

/* TODO
#[cfg(test)]
mod tests {
    use futures_state_stream::StateStream;
    use tokio::executor::current_thread;
    use crate::tests::new_connection;

    #[test]
    fn select_nvarchar_collation_test() {
        let c1 = new_connection();
        let query = c1.simple_query(
            "select cast(cast(N'cześć' as nvarchar(5)) collate Polish_CI_AI as varchar(5))",
        );
        let mut i = 0;
        {
            let future = query.for_each(|x| {
                let val: &str = x.get(0);
                assert_eq!(val, "cześć");
                i += 1;
                Ok(())
            });
            current_thread::block_on_all(future).unwrap();
        }
        assert_eq!(i, 1);
    }
}
*/
