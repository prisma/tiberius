use crate::{error::Error, sql_read_bytes::SqlReadBytes, ColumnData};
use bytes::BufMut;

/// Encode an `f64` as a money/smallmoney value into `dst`, prefixed with a
/// single length byte (as expected for a nullable `Money`/`Moneyn` column in a
/// bulk-load row). `max_len` is the column's declared length (8 for `money`,
/// 4 for `smallmoney`). Money is stored on the wire as a scaled integer
/// (value * 10_000).
///
/// Returns an error (rather than silently corrupting the value) when `val` is
/// not finite (`NaN`/`±Inf`) or when the scaled value falls outside the range
/// representable by the target column. Rust's `as` cast from float to integer
/// saturates, so without this check `NaN` would encode as `0` and out-of-range
/// values would clamp to `i32::MAX`/`i64::MAX`, both of which would be
/// undetectably wrong on the wire.
pub(crate) fn encode<B>(dst: &mut B, max_len: usize, val: f64) -> crate::Result<()>
where
    B: BufMut,
{
    if !val.is_finite() {
        return Err(Error::BulkInput(
            format!("money: value {val} is not finite (NaN/Inf cannot be encoded)").into(),
        ));
    }

    // Scale into money's fixed-point (4 decimal places) domain. Rounding is done
    // in f64 for parity with the previous behaviour, then range-checked in f64
    // (comparing against the target integer bounds) before narrowing, so an
    // out-of-range value is rejected instead of saturating on the `as` cast.
    let scaled = (val * 1e4).round();

    if max_len == 4 {
        // smallmoney: scaled value must fit in an i32.
        if scaled < f64::from(i32::MIN) || scaled > f64::from(i32::MAX) {
            return Err(Error::BulkInput(
                format!(
                    "money: value {val} is out of range for smallmoney \
                     (-214_748.3648 ..= 214_748.3647)"
                )
                .into(),
            ));
        }
        dst.put_u8(4);
        dst.put_i32_le(scaled as i32);
    } else {
        // money: scaled value must fit in an i64.
        if scaled < i64::MIN as f64 || scaled > i64::MAX as f64 {
            return Err(Error::BulkInput(
                format!(
                    "money: value {val} is out of range for money \
                     (-922_337_203_685_477.5808 ..= 922_337_203_685_477.5807)"
                )
                .into(),
            ));
        }
        dst.put_u8(8);
        let scaled = scaled as i64;
        // money is transmitted as two 32-bit words, high word first.
        dst.put_i32_le((scaled >> 32) as i32);
        dst.put_u32_le(scaled as u32);
    }

    Ok(())
}

pub(crate) async fn decode<R>(src: &mut R, len: u8) -> crate::Result<ColumnData<'static>>
where
    R: SqlReadBytes + Unpin,
{
    let res = match len {
        0 => ColumnData::F64(None),
        4 => ColumnData::F64(Some(src.read_i32_le().await? as f64 / 1e4)),
        8 => ColumnData::F64(Some({
            let high = src.read_i32_le().await? as i64;
            let low = src.read_u32_le().await? as f64;

            ((high << 32) as f64 + low) / 1e4
        })),
        _ => {
            return Err(Error::Protocol(
                format!("money: length of {} is invalid", len).into(),
            ))
        }
    };

    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reverses the on-wire money representation the same way `decode` does,
    // so we can assert `encode` is the exact inverse without a live server.
    fn decode_bytes(bytes: &[u8]) -> f64 {
        let len = bytes[0];
        match len {
            4 => i32::from_le_bytes(bytes[1..5].try_into().unwrap()) as f64 / 1e4,
            8 => {
                let high = i32::from_le_bytes(bytes[1..5].try_into().unwrap()) as i64;
                let low = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as f64;
                ((high << 32) as f64 + low) / 1e4
            }
            _ => panic!("invalid length"),
        }
    }

    #[test]
    fn encode_smallmoney_roundtrips() {
        let mut buf = Vec::new();
        encode(&mut buf, 4, 1234.5678).unwrap();
        assert_eq!(buf[0], 4);
        assert_eq!(buf.len(), 5);
        assert_eq!(decode_bytes(&buf), 1234.5678);
    }

    #[test]
    fn encode_money_roundtrips() {
        for val in [0.0, 1.0, -1.0, 1234.5678, -9999.9999, 92233720368.5477] {
            let mut buf = Vec::new();
            encode(&mut buf, 8, val).unwrap();
            assert_eq!(buf[0], 8);
            assert_eq!(buf.len(), 9);
            assert!((decode_bytes(&buf) - val).abs() < 1e-3, "val={}", val);
        }
    }

    // `NaN` must be rejected: the old `as i32`/`as i64` cast turned it silently
    // into `0`, corrupting the row without any error.
    #[test]
    fn encode_nan_is_rejected() {
        for max_len in [4usize, 8usize] {
            let mut buf = Vec::new();
            let err = encode(&mut buf, max_len, f64::NAN).unwrap_err();
            assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
            assert!(buf.is_empty(), "nothing must be written on rejection");
        }
    }

    // `+Inf` must be rejected: the old cast saturated it to `i32::MAX`/`i64::MAX`.
    #[test]
    fn encode_positive_infinity_is_rejected() {
        for max_len in [4usize, 8usize] {
            let mut buf = Vec::new();
            let err = encode(&mut buf, max_len, f64::INFINITY).unwrap_err();
            assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
            assert!(buf.is_empty());
        }
    }

    // `-Inf` must be rejected: the old cast saturated it to `i32::MIN`/`i64::MIN`.
    #[test]
    fn encode_negative_infinity_is_rejected() {
        for max_len in [4usize, 8usize] {
            let mut buf = Vec::new();
            let err = encode(&mut buf, max_len, f64::NEG_INFINITY).unwrap_err();
            assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
            assert!(buf.is_empty());
        }
    }

    // Finite values beyond the column's range must be rejected rather than
    // saturating to the min/max on the wire.
    #[test]
    fn encode_over_max_is_rejected() {
        // Just past smallmoney's max (214_748.3647).
        let mut buf = Vec::new();
        let err = encode(&mut buf, 4, 214_749.0).unwrap_err();
        assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
        assert!(buf.is_empty());

        // Below smallmoney's min (-214_748.3648).
        let mut buf = Vec::new();
        let err = encode(&mut buf, 4, -214_749.0).unwrap_err();
        assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");

        // Well beyond money's max (~9.22e14).
        let mut buf = Vec::new();
        let err = encode(&mut buf, 8, 1e15).unwrap_err();
        assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
        assert!(buf.is_empty());

        // Well below money's min.
        let mut buf = Vec::new();
        let err = encode(&mut buf, 8, -1e15).unwrap_err();
        assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
    }

    // Boundary values at the edge of each column's range must still be accepted
    // and round-trip correctly.
    #[test]
    fn encode_boundaries_are_accepted() {
        // smallmoney min/max.
        for val in [214_748.3647_f64, -214_748.3648_f64] {
            let mut buf = Vec::new();
            encode(&mut buf, 4, val).unwrap();
            assert_eq!(buf[0], 4);
            assert!((decode_bytes(&buf) - val).abs() < 1e-3, "val={val}");
        }

        // money min/max.
        for val in [922_337_203_685_477.5807_f64, -922_337_203_685_477.5808_f64] {
            let mut buf = Vec::new();
            encode(&mut buf, 8, val).unwrap();
            assert_eq!(buf[0], 8);
            // f64 cannot represent the money extremes exactly; allow a tolerance
            // proportional to the magnitude (~0.1 currency units here).
            assert!((decode_bytes(&buf) - val).abs() < 1.0, "val={val}");
        }
    }
}
