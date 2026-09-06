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

/// Encode a [`Numeric`] into a money/smallmoney value into `dst`, prefixed with
/// a single length byte, using exact integer arithmetic.
///
/// Money is a fixed-point decimal with scale 4 stored as a scaled integer
/// (`value * 10_000`). Going via `f64` (as the plain `f64` path does) is lossy
/// for large magnitudes: money's scaled range reaches ~9.2e18, well beyond the
/// 2^53 exact-integer range of `f64`, so big values would be corrupted. This
/// path rescales the `Numeric`'s i128 mantissa to scale 4 entirely in i128, so
/// every in-range money value round-trips exactly.
///
/// Rounding, when the source scale is finer than 4, is round-half-away-from-zero
/// to match the `.round()` used by the `f64` [`encode`] path. Values that do not
/// fit the target column's integer range (`i64` for money, `i32` for
/// smallmoney) are rejected with [`Error::BulkInput`] rather than silently
/// wrapping or saturating.
pub(crate) fn encode_numeric<B>(
    dst: &mut B,
    max_len: usize,
    num: &crate::tds::Numeric,
) -> crate::Result<()>
where
    B: BufMut,
{
    let scale = num.scale();
    let mantissa = num.value();

    // Rescale the mantissa to money's fixed scale of 4, in i128.
    let scaled: i128 = if scale == 4 {
        mantissa
    } else if scale < 4 {
        let factor = 10i128.pow((4 - scale) as u32);
        mantissa.checked_mul(factor).ok_or_else(|| {
            Error::BulkInput(
                format!("money: numeric value {num} overflows while rescaling to money").into(),
            )
        })?
    } else {
        // scale > 4: divide, rounding half away from zero.
        let divisor = 10i128.pow((scale - 4) as u32);
        let quotient = mantissa / divisor;
        let remainder = (mantissa % divisor).abs();
        if remainder * 2 >= divisor {
            quotient + mantissa.signum()
        } else {
            quotient
        }
    };

    if max_len == 4 {
        // smallmoney: scaled value must fit in an i32.
        if scaled < i32::MIN as i128 || scaled > i32::MAX as i128 {
            return Err(Error::BulkInput(
                format!(
                    "money: numeric value {num} is out of range for smallmoney \
                     (-214_748.3648 ..= 214_748.3647)"
                )
                .into(),
            ));
        }
        dst.put_u8(4);
        dst.put_i32_le(scaled as i32);
    } else {
        // money: scaled value must fit in an i64.
        if scaled < i64::MIN as i128 || scaled > i64::MAX as i128 {
            return Err(Error::BulkInput(
                format!(
                    "money: numeric value {num} is out of range for money \
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

    // Reconstruct the exact scaled integer from the on-wire bytes without going
    // through `f64`, so large-magnitude values can be asserted exactly.
    fn decode_bytes_scaled(bytes: &[u8]) -> i64 {
        match bytes[0] {
            4 => i32::from_le_bytes(bytes[1..5].try_into().unwrap()) as i64,
            8 => {
                let high = i32::from_le_bytes(bytes[1..5].try_into().unwrap()) as i64;
                let low = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as i64;
                (high << 32) | low
            }
            _ => panic!("invalid length"),
        }
    }

    // A large money value whose scaled form (value * 1e4) exceeds 2^53: the old
    // `f64::from(num)` path would corrupt it, but `encode_numeric` keeps it exact.
    #[test]
    fn encode_numeric_large_value_is_exact() {
        // 12_345_678_901_234.5678 in money units => scaled = 123456789012345678,
        // which is far larger than 2^53 (~9.007e15).
        let scaled: i128 = 123_456_789_012_345_678;
        let num = crate::tds::Numeric::new_with_scale(scaled, 4);

        let mut buf = Vec::new();
        encode_numeric(&mut buf, 8, &num).unwrap();
        assert_eq!(buf[0], 8);
        assert_eq!(decode_bytes_scaled(&buf) as i128, scaled);

        // Demonstrate the f64 path is lossy for the same value (guards the
        // motivation for this function): the round-tripped f64 scaled integer
        // differs from the exact one.
        let via_f64 = (f64::from(num) * 1e4).round() as i128;
        assert_ne!(via_f64, scaled, "f64 path unexpectedly stayed exact");
    }

    // scale < 4 is rescaled up by 10^(4-scale) with no loss.
    #[test]
    fn encode_numeric_scale_less_than_four() {
        // 12.34 stored as mantissa 1234 at scale 2.
        let num = crate::tds::Numeric::new_with_scale(1234, 2);
        let mut buf = Vec::new();
        encode_numeric(&mut buf, 8, &num).unwrap();
        // 12.34 * 1e4 = 123400.
        assert_eq!(decode_bytes_scaled(&buf), 123_400);
    }

    // scale > 4 divides by 10^(scale-4), rounding half away from zero.
    #[test]
    fn encode_numeric_scale_greater_than_four_rounds_half_away() {
        // 1.23455 at scale 5 -> scale 4 rounds to 1.2346 (half rounds away).
        let num = crate::tds::Numeric::new_with_scale(123_455, 5);
        let mut buf = Vec::new();
        encode_numeric(&mut buf, 8, &num).unwrap();
        assert_eq!(decode_bytes_scaled(&buf), 12_346);

        // Same magnitude negative: -1.23455 -> -1.2346.
        let num = crate::tds::Numeric::new_with_scale(-123_455, 5);
        let mut buf = Vec::new();
        encode_numeric(&mut buf, 8, &num).unwrap();
        assert_eq!(decode_bytes_scaled(&buf), -12_346);

        // Below the half point truncates toward zero: 1.23454 -> 1.2345.
        let num = crate::tds::Numeric::new_with_scale(123_454, 5);
        let mut buf = Vec::new();
        encode_numeric(&mut buf, 8, &num).unwrap();
        assert_eq!(decode_bytes_scaled(&buf), 12_345);
    }

    // Numeric values beyond the target column's range are rejected.
    #[test]
    fn encode_numeric_out_of_range_is_rejected() {
        // Past smallmoney's i32 scaled range.
        let num = crate::tds::Numeric::new_with_scale(3_000_000_000, 4);
        let mut buf = Vec::new();
        let err = encode_numeric(&mut buf, 4, &num).unwrap_err();
        assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
        assert!(buf.is_empty());

        // Past money's i64 scaled range.
        let num = crate::tds::Numeric::new_with_scale(10_000_000_000_000_000_000, 4);
        let mut buf = Vec::new();
        let err = encode_numeric(&mut buf, 8, &num).unwrap_err();
        assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
        assert!(buf.is_empty());

        // Overflow while rescaling a low-scale, huge mantissa up to scale 4.
        let num = crate::tds::Numeric::new_with_scale(i128::MAX, 0);
        let mut buf = Vec::new();
        let err = encode_numeric(&mut buf, 8, &num).unwrap_err();
        assert!(matches!(err, Error::BulkInput(_)), "got {err:?}");
    }

    // Boundary money/smallmoney values encode exactly through the integer path.
    #[test]
    fn encode_numeric_boundaries_are_exact() {
        // smallmoney max/min (scaled i32 bounds), as scale-4 Numerics.
        for scaled in [i32::MAX as i128, i32::MIN as i128] {
            let num = crate::tds::Numeric::new_with_scale(scaled, 4);
            let mut buf = Vec::new();
            encode_numeric(&mut buf, 4, &num).unwrap();
            assert_eq!(buf[0], 4);
            assert_eq!(decode_bytes_scaled(&buf) as i128, scaled);
        }

        // money max/min (scaled i64 bounds).
        for scaled in [i64::MAX as i128, i64::MIN as i128] {
            let num = crate::tds::Numeric::new_with_scale(scaled, 4);
            let mut buf = Vec::new();
            encode_numeric(&mut buf, 8, &num).unwrap();
            assert_eq!(buf[0], 8);
            assert_eq!(decode_bytes_scaled(&buf) as i128, scaled);
        }
    }
}
