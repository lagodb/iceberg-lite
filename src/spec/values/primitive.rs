// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Primitive literal types

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::mem;

use ordered_float::OrderedFloat;

/// Values present in iceberg type
#[derive(Clone, Debug)]
pub enum PrimitiveLiteral {
    /// 0x00 for false, non-zero byte for true
    Boolean(bool),
    /// Stored as 4-byte little-endian
    Int(i32),
    /// Stored as 8-byte little-endian
    Long(i64),
    /// Stored as 4-byte little-endian
    Float(OrderedFloat<f32>),
    /// Stored as 8-byte little-endian
    Double(OrderedFloat<f64>),
    /// UTF-8 bytes (without length)
    String(String),
    /// Binary value (without length)
    Binary(Vec<u8>),
    /// Stored as 16-byte big-endian
    Int128(i128),
    /// Stored as 16-byte big-endian
    UInt128(u128),
    /// When a number is larger than it can hold
    AboveMax,
    /// When a number is smaller than it can hold
    BelowMin,
}

impl PrimitiveLiteral {
    // Iceberg uses the equivalent of Java's floatToIntBits/doubleToLongBits
    // for floating-point value identity: preserve signed zero and normalize NaN.
    const CANONICAL_F32_NAN_BITS: u32 = 0x7fc0_0000;
    const CANONICAL_F64_NAN_BITS: u64 = 0x7ff8_0000_0000_0000;

    fn canonical_f32_bits(value: f32) -> u32 {
        if value.is_nan() {
            Self::CANONICAL_F32_NAN_BITS
        } else {
            value.to_bits()
        }
    }

    fn canonical_f64_bits(value: f64) -> u64 {
        if value.is_nan() {
            Self::CANONICAL_F64_NAN_BITS
        } else {
            value.to_bits()
        }
    }

    fn equal_f32(left: f32, right: f32) -> bool {
        if left == right {
            left != 0.0 || left.to_bits() == right.to_bits()
        } else {
            left.is_nan() && right.is_nan()
        }
    }

    fn equal_f64(left: f64, right: f64) -> bool {
        if left == right {
            left != 0.0 || left.to_bits() == right.to_bits()
        } else {
            left.is_nan() && right.is_nan()
        }
    }

    fn compare_f32(left: f32, right: f32) -> Ordering {
        if left < right {
            Ordering::Less
        } else if left > right {
            Ordering::Greater
        } else {
            i32::from_ne_bytes(Self::canonical_f32_bits(left).to_ne_bytes()).cmp(
                &i32::from_ne_bytes(Self::canonical_f32_bits(right).to_ne_bytes()),
            )
        }
    }

    fn compare_f64(left: f64, right: f64) -> Ordering {
        if left < right {
            Ordering::Less
        } else if left > right {
            Ordering::Greater
        } else {
            i64::from_ne_bytes(Self::canonical_f64_bits(left).to_ne_bytes()).cmp(
                &i64::from_ne_bytes(Self::canonical_f64_bits(right).to_ne_bytes()),
            )
        }
    }

    fn variant_order(&self) -> u8 {
        use PrimitiveLiteral::*;

        match self {
            Boolean(_) => 0,
            Int(_) => 1,
            Long(_) => 2,
            Float(_) => 3,
            Double(_) => 4,
            String(_) => 5,
            Binary(_) => 6,
            Int128(_) => 7,
            UInt128(_) => 8,
            AboveMax => 9,
            BelowMin => 10,
        }
    }

    /// Returns true if the Literal represents a primitive type
    /// that can be a NaN, and that it's value is NaN
    pub fn is_nan(&self) -> bool {
        match self {
            PrimitiveLiteral::Double(val) => val.is_nan(),
            PrimitiveLiteral::Float(val) => val.is_nan(),
            _ => false,
        }
    }
}

impl PartialEq for PrimitiveLiteral {
    fn eq(&self, other: &Self) -> bool {
        use PrimitiveLiteral::*;

        match (self, other) {
            (Boolean(left), Boolean(right)) => left == right,
            (Int(left), Int(right)) => left == right,
            (Long(left), Long(right)) => left == right,
            (Float(left), Float(right)) => Self::equal_f32(left.0, right.0),
            (Double(left), Double(right)) => Self::equal_f64(left.0, right.0),
            (String(left), String(right)) => left == right,
            (Binary(left), Binary(right)) => left == right,
            (Int128(left), Int128(right)) => left == right,
            (UInt128(left), UInt128(right)) => left == right,
            (AboveMax, AboveMax) | (BelowMin, BelowMin) => true,
            _ => false,
        }
    }
}

impl Eq for PrimitiveLiteral {}

impl Hash for PrimitiveLiteral {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use PrimitiveLiteral::*;

        mem::discriminant(self).hash(state);
        match self {
            Boolean(value) => value.hash(state),
            Int(value) => value.hash(state),
            Long(value) => value.hash(state),
            Float(value) => {
                Self::canonical_f32_bits(value.0).hash(state);
            }
            Double(value) => {
                Self::canonical_f64_bits(value.0).hash(state);
            }
            String(value) => value.hash(state),
            Binary(value) => value.hash(state),
            Int128(value) => value.hash(state),
            UInt128(value) => value.hash(state),
            AboveMax | BelowMin => {}
        }
    }
}

impl PartialOrd for PrimitiveLiteral {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use PrimitiveLiteral::*;

        match (self, other) {
            (Boolean(left), Boolean(right)) => left.partial_cmp(right),
            (Int(left), Int(right)) => left.partial_cmp(right),
            (Long(left), Long(right)) => left.partial_cmp(right),
            (Float(left), Float(right)) => Some(Self::compare_f32(left.0, right.0)),
            (Double(left), Double(right)) => Some(Self::compare_f64(left.0, right.0)),
            (String(left), String(right)) => left.partial_cmp(right),
            (Binary(left), Binary(right)) => left.partial_cmp(right),
            (Int128(left), Int128(right)) => left.partial_cmp(right),
            (UInt128(left), UInt128(right)) => left.partial_cmp(right),
            (AboveMax, AboveMax) | (BelowMin, BelowMin) => Some(Ordering::Equal),
            _ => Some(self.variant_order().cmp(&other.variant_order())),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashSet;
    use std::hash::{Hash, Hasher};

    use super::PrimitiveLiteral;
    use crate::spec::{Literal, Struct};

    #[derive(Default)]
    struct RecordingHasher {
        bytes: Vec<u8>,
    }

    impl Hasher for RecordingHasher {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, bytes: &[u8]) {
            self.bytes.extend_from_slice(bytes);
        }
    }

    fn recorded_hash_bytes(value: &PrimitiveLiteral) -> Vec<u8> {
        let mut hasher = RecordingHasher::default();
        value.hash(&mut hasher);
        hasher.bytes
    }

    #[test]
    fn float_partition_values_distinguish_signed_zero() {
        let negative_zero = Struct::from_iter([Some(Literal::float(-0.0_f32))]);
        let positive_zero = Struct::from_iter([Some(Literal::float(0.0_f32))]);
        assert_ne!(negative_zero, positive_zero);

        let partitions = HashSet::from([negative_zero, positive_zero]);

        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn double_partition_values_distinguish_signed_zero() {
        let negative_zero = Struct::from_iter([Some(Literal::double(-0.0_f64))]);
        let positive_zero = Struct::from_iter([Some(Literal::double(0.0_f64))]);
        assert_ne!(negative_zero, positive_zero);

        let partitions = HashSet::from([negative_zero, positive_zero]);

        assert_eq!(partitions.len(), 2);
    }

    #[test]
    fn float_and_double_normalize_nan_for_equality_and_hashing() {
        let float_values = HashSet::from([
            PrimitiveLiteral::Float(f32::from_bits(0x7fc0_0001).into()),
            PrimitiveLiteral::Float(f32::from_bits(0xffc0_0002).into()),
        ]);
        let double_values = HashSet::from([
            PrimitiveLiteral::Double(f64::from_bits(0x7ff8_0000_0000_0001).into()),
            PrimitiveLiteral::Double(f64::from_bits(0xfff8_0000_0000_0002).into()),
        ]);

        assert_eq!(
            PrimitiveLiteral::Float(f32::from_bits(0x7fc0_0001).into()),
            PrimitiveLiteral::Float(f32::from_bits(0xffc0_0002).into()),
        );
        assert_eq!(
            PrimitiveLiteral::Double(f64::from_bits(0x7ff8_0000_0000_0001).into(),),
            PrimitiveLiteral::Double(f64::from_bits(0xfff8_0000_0000_0002).into(),),
        );
        assert_eq!(float_values.len(), 1);
        assert_eq!(double_values.len(), 1);
    }

    #[test]
    fn primitive_literal_partial_order_covers_floats_and_variants() {
        assert_eq!(
            PrimitiveLiteral::Float((-0.0_f32).into())
                .partial_cmp(&PrimitiveLiteral::Float(0.0_f32.into())),
            Some(Ordering::Less)
        );
        assert_eq!(
            PrimitiveLiteral::Double((-0.0_f64).into())
                .partial_cmp(&PrimitiveLiteral::Double(0.0_f64.into())),
            Some(Ordering::Less)
        );

        let float_nan = PrimitiveLiteral::Float(f32::from_bits(0x7fc0_0001).into());
        let negative_float_nan =
            PrimitiveLiteral::Float(f32::from_bits(0xffc0_0002).into());
        assert_eq!(
            float_nan.partial_cmp(&negative_float_nan),
            Some(Ordering::Equal)
        );
        assert_eq!(
            PrimitiveLiteral::Float(f32::INFINITY.into()).partial_cmp(&float_nan),
            Some(Ordering::Less)
        );

        let double_nan =
            PrimitiveLiteral::Double(f64::from_bits(0x7ff8_0000_0000_0001).into());
        let negative_double_nan =
            PrimitiveLiteral::Double(f64::from_bits(0xfff8_0000_0000_0002).into());
        assert_eq!(
            double_nan.partial_cmp(&negative_double_nan),
            Some(Ordering::Equal)
        );
        assert_eq!(
            PrimitiveLiteral::Double(f64::INFINITY.into()).partial_cmp(&double_nan),
            Some(Ordering::Less)
        );

        let variants = [
            PrimitiveLiteral::Boolean(false),
            PrimitiveLiteral::Int(0),
            PrimitiveLiteral::Long(0),
            PrimitiveLiteral::Float(0.0_f32.into()),
            PrimitiveLiteral::Double(0.0_f64.into()),
            PrimitiveLiteral::String(String::new()),
            PrimitiveLiteral::Binary(Vec::new()),
            PrimitiveLiteral::Int128(0),
            PrimitiveLiteral::UInt128(0),
            PrimitiveLiteral::AboveMax,
            PrimitiveLiteral::BelowMin,
        ];
        for pair in variants.windows(2) {
            assert_eq!(
                pair[0].partial_cmp(&pair[1]),
                Some(Ordering::Less),
                "expected {:?} to sort before {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn float_hashing_distinguishes_signed_zero_and_normalizes_nan() {
        assert_ne!(
            recorded_hash_bytes(&PrimitiveLiteral::Float((-0.0_f32).into())),
            recorded_hash_bytes(&PrimitiveLiteral::Float(0.0_f32.into()))
        );
        assert_ne!(
            recorded_hash_bytes(&PrimitiveLiteral::Double((-0.0_f64).into())),
            recorded_hash_bytes(&PrimitiveLiteral::Double(0.0_f64.into()))
        );

        assert_eq!(
            recorded_hash_bytes(&PrimitiveLiteral::Float(
                f32::from_bits(0x7fc0_0001).into(),
            )),
            recorded_hash_bytes(&PrimitiveLiteral::Float(
                f32::from_bits(0xffc0_0002).into(),
            ))
        );
        assert_eq!(
            recorded_hash_bytes(&PrimitiveLiteral::Double(
                f64::from_bits(0x7ff8_0000_0000_0001).into(),
            )),
            recorded_hash_bytes(&PrimitiveLiteral::Double(
                f64::from_bits(0xfff8_0000_0000_0002).into(),
            ))
        );
    }
}
