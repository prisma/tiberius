use std::fmt::{self, Debug, Display, Formatter};
use std::cmp::PartialEq;

/// Represent a sql Decimal / Numeric type. It is stored in a i128 and has a
/// maximum precision of 38 decimals.
#[derive(Copy, Clone)]
pub struct Numeric {
    value: i128,
    scale: u8,
}

impl Numeric {
    /// Creates a new Numeric value.
    /// 
    /// # Panic
    /// It will panic if the scale exceed 37.
    pub fn new_with_scale(value: i128, scale: u8) -> Self {
        // scale cannot exceed 37 since a
        // max precision of 38 is possible here.
        assert!(scale < 38);

        Numeric {
            value,
            scale,
        }
    }

    /// Extract the decimal part.
    pub fn dec_part(self) -> i128 {
        let scale = self.pow_scale();
        self.value - (self.value / scale) * scale
    }

    /// Extract the integer part.
    pub fn int_part(self) -> i128 {
        self.value / self.pow_scale()
    }

    #[inline]
    fn pow_scale(self) -> i128 {
        10i128.pow(self.scale as u32)
    }

    /// The scale (where is the decimal point) of the value.
    #[inline]
    pub fn scale(&self) -> u8 {
        self.scale
    }

    /// The internal integer value
    #[inline]
    pub fn value(&self) -> i128 {
        self.value
    }
}

impl Debug for Numeric {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}.{:0pad$}", self.int_part(), self.dec_part(), pad = self.scale as usize)
    }
}

impl Display for Numeric {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "{:?}", self)
    }
}

impl Eq for Numeric {}

impl From<Numeric> for f64 {
    fn from(n: Numeric) -> f64 {
        n.dec_part() as f64 / n.pow_scale() as f64 + n.int_part() as f64
    }
}

impl From<Numeric> for i128 {
    fn from(n: Numeric) -> i128 {
        n.int_part()
    }
}

impl From<Numeric> for u128 {
    fn from(n: Numeric) -> u128 {
        n.int_part() as u128
    }
}

impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        if self.scale < other.scale {
            10i128.pow((other.scale - self.scale) as u32) * self.value == other.value
        } else if self.scale > other.scale {
            10i128.pow((self.scale - other.scale) as u32) * other.value == self.value
        } else {
            self.value == other.value
        }
    }
}

#[test]
fn test_numeric_eq() {
    assert_eq!(Numeric { value: 100501, scale: 2 }, Numeric { value: 1005010, scale: 3 });
    assert!(Numeric { value: 100501, scale: 2 } != Numeric { value: 10050, scale: 1 });
}

#[test]
fn test_numeric_to_f64() {
    assert_eq!(f64::from(Numeric::new_with_scale(57705, 2)), 577.05);
}

#[test]
fn test_numeric_to_int_dec_part() {
    let n = Numeric::new_with_scale(57705, 2);
    assert_eq!(n.int_part(), 577);
    assert_eq!(n.dec_part(), 05);
}
