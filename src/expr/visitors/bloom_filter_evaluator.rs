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

//! Evaluates predicates against Parquet bloom filters to determine whether
//! a row group can be skipped.

use std::collections::{HashMap, HashSet};

use fnv::FnvHashSet;
use parquet::basic::Type as PhysicalType;
use parquet::bloom_filter::Sbbf;
use parquet::data_type::ByteArray;

use crate::Result;
use crate::expr::visitors::bound_predicate_visitor::{BoundPredicateVisitor, visit};
use crate::expr::{BoundPredicate, BoundReference};
use crate::spec::decimal_utils::decimal_to_fixed_length_bytes_exact;
use crate::spec::{Datum, PrimitiveLiteral, PrimitiveType, Type};

const ROW_GROUP_MIGHT_MATCH: Result<bool> = Ok(true);
const ROW_GROUP_CANT_MATCH: Result<bool> = Ok(false);

/// A column's bloom filter for one row group, together with the file's physical
/// encoding of that column. A probe must be encoded the way the writer encoded
/// the values it inserted, so the encoding travels with the filter.
pub(crate) struct ColumnBloomFilter {
    sbbf: Sbbf,
    physical_type: PhysicalType,
    /// `type_length` from the file's column descriptor. Only meaningful for
    /// `FIXED_LEN_BYTE_ARRAY`.
    type_length: i32,
}

impl ColumnBloomFilter {
    pub(crate) fn new(
        sbbf: Sbbf,
        physical_type: PhysicalType,
        type_length: i32,
    ) -> Self {
        Self {
            sbbf,
            physical_type,
            type_length,
        }
    }
}

pub(crate) struct BloomFilterEvaluator<'a> {
    /// Maps Iceberg field_id -> bloom filter for this row group
    bloom_filters: &'a HashMap<i32, ColumnBloomFilter>,
}

impl BloomFilterEvaluator<'_> {
    /// Evaluate the predicate against the provided bloom filters.
    /// Returns `false` if the row group definitely does not match,
    /// `true` if it might match.
    pub(crate) fn eval(
        filter: &BoundPredicate,
        bloom_filters: &HashMap<i32, ColumnBloomFilter>,
    ) -> Result<bool> {
        if bloom_filters.is_empty() {
            return ROW_GROUP_MIGHT_MATCH;
        }

        let mut evaluator = BloomFilterEvaluator { bloom_filters };
        visit(&mut evaluator, filter)
    }

    fn check_datum(&self, reference: &BoundReference, datum: &Datum) -> bool {
        let field_id = reference.field().id;
        let Some(column) = self.bloom_filters.get(&field_id) else {
            // No bloom filter for this column — conservatively might match
            return true;
        };

        check_in_bloom_filter(column, datum)
    }
}

/// Collects field IDs that appear in `eq` or `in` predicates — the only
/// predicate types that benefit from bloom filter checks.
///
/// Each node returns the field IDs its subtree contributes, mirroring
/// [`BloomFilterEvaluator`]'s structure so that a field is collected only where
/// the evaluator can act on it. In particular `not` discards its subtree: the
/// evaluator's `not` returns might-match regardless, so a filter fetched for a
/// field under a `NOT` could never prune and would be pure wasted I/O.
///
/// Boolean fields are skipped for the same reason: with only two possible values,
/// a column chunk's min/max statistics already determine membership exactly, so a
/// bloom filter cannot prune anything statistics did not, and reading one costs a
/// round trip to learn nothing.
pub(crate) fn collect_bloom_filter_field_ids(
    predicate: &BoundPredicate,
) -> Result<HashSet<i32>> {
    visit(&mut BloomFilterFieldIdCollector, predicate)
}

struct BloomFilterFieldIdCollector;

/// Field IDs worth fetching a bloom filter for: none if probing the column could
/// never prune.
fn probeable_field_id(reference: &BoundReference) -> HashSet<i32> {
    if matches!(
        reference.field().field_type.as_ref(),
        Type::Primitive(PrimitiveType::Boolean)
    ) {
        return HashSet::new();
    }
    HashSet::from([reference.field().id])
}

impl BoundPredicateVisitor for BloomFilterFieldIdCollector {
    type T = HashSet<i32>;

    fn always_true(&mut self) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn always_false(&mut self) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn and(&mut self, mut lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        lhs.extend(rhs);
        Ok(lhs)
    }

    fn or(&mut self, mut lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        lhs.extend(rhs);
        Ok(lhs)
    }

    fn not(&mut self, _inner: Self::T) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn is_null(
        &mut self,
        _r: &BoundReference,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn not_null(
        &mut self,
        _r: &BoundReference,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn is_nan(
        &mut self,
        _r: &BoundReference,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn not_nan(
        &mut self,
        _r: &BoundReference,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn less_than(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn less_than_or_eq(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn greater_than(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn greater_than_or_eq(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn eq(
        &mut self,
        r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(probeable_field_id(r))
    }

    fn not_eq(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn starts_with(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn not_starts_with(
        &mut self,
        _r: &BoundReference,
        _l: &Datum,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }

    fn r#in(
        &mut self,
        r: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(probeable_field_id(r))
    }

    fn not_in(
        &mut self,
        _r: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _p: &BoundPredicate,
    ) -> Result<Self::T> {
        Ok(HashSet::new())
    }
}

/// Check whether a datum value might be present in the bloom filter.
///
/// The value must be checked using the same physical encoding the Parquet
/// writer used when inserting into the bloom filter. We use the actual
/// physical type from the column metadata to ensure correctness regardless
/// of which writer produced the file.
fn check_in_bloom_filter(column: &ColumnBloomFilter, datum: &Datum) -> bool {
    let ColumnBloomFilter {
        sbbf,
        physical_type,
        type_length,
    } = column;
    let physical_type = *physical_type;

    match datum.literal() {
        // Parquet does not define bloom filter semantics for BOOLEAN — parquet-java
        // only hashes int/long/float/double/binary. arrow-rs does write them, but min/max
        // statistics already decide membership for a two-valued domain, so there is nothing to gain.
        PrimitiveLiteral::Boolean(_) => true,
        // A promoted column (int -> long, float -> double) keeps its original
        // physical width in files written before the promotion, and the writer
        // hashed that width, so probe at the file's width rather than the
        // predicate's.
        PrimitiveLiteral::Int(v) => match physical_type {
            PhysicalType::INT32 => sbbf.check(v),
            PhysicalType::INT64 => sbbf.check(&i64::from(*v)),
            _ => true,
        },
        PrimitiveLiteral::Long(v) => match physical_type {
            PhysicalType::INT64 => sbbf.check(v),
            PhysicalType::INT32 => match i32::try_from(*v) {
                Ok(narrowed) => sbbf.check(&narrowed),
                // Too wide for an INT32 column to hold, so the value is provably
                // absent and pruning here would be sound; kept conservative for now.
                Err(_) => true,
            },
            _ => true,
        },
        PrimitiveLiteral::Float(v) => match physical_type {
            PhysicalType::FLOAT => sbbf.check(&v.0),
            PhysicalType::DOUBLE => sbbf.check(&f64::from(v.0)),
            _ => true,
        },
        PrimitiveLiteral::Double(v) => match physical_type {
            PhysicalType::DOUBLE => sbbf.check(&v.0),
            PhysicalType::FLOAT => {
                let narrowed = v.0 as f32;
                // Only an exactly representable value can equal a widened f32.
                if f64::from(narrowed) == v.0 {
                    sbbf.check(&narrowed)
                } else {
                    true
                }
            }
            _ => true,
        },
        PrimitiveLiteral::String(v) => sbbf.check(v.as_str()),
        PrimitiveLiteral::Binary(v) => sbbf.check(v.as_slice()),
        PrimitiveLiteral::Int128(v) => {
            // Decimal: dispatch based on the actual Parquet physical type
            // from the file, not inferred from precision.
            match physical_type {
                // Narrow only when the mantissa round-trips; a truncated copy would
                // hash to an unrelated slot.
                PhysicalType::INT32 => match i32::try_from(*v) {
                    Ok(narrowed) => sbbf.check(&narrowed),
                    Err(_) => true,
                },
                PhysicalType::INT64 => match i64::try_from(*v) {
                    Ok(narrowed) => sbbf.check(&narrowed),
                    Err(_) => true,
                },
                PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                    // Encode to the file's declared length, not one derived from
                    // the Iceberg precision: a widened precision would change the
                    // length and miss every entry the writer inserted.
                    match usize::try_from(*type_length)
                        .ok()
                        .and_then(|len| decimal_to_fixed_length_bytes_exact(*v, len))
                    {
                        Some(bytes) => sbbf.check(&ByteArray::from(bytes)),
                        // Unusable length, or a value too large for the column to
                        // hold — conservatively might match.
                        None => true,
                    }
                }
                // Known gap: BYTE_ARRAY decimals are valid in Parquet (though not in
                // Iceberg's Appendix A) and some older Spark writers emit them. Not
                // probed because BYTE_ARRAY carries no `type_length` and sign-extension
                // padding is writer-dependent, so a single-length probe would miss
                // padded entries and prune row groups that do hold the value. A sound
                // version ORs a check over every length from minimal..=16.
                _ => true, // Conservatively might match
            }
        }
        PrimitiveLiteral::UInt128(v) => {
            // UUID: stored as FIXED_LEN_BYTE_ARRAY(16), big-endian
            let bytes = v.to_be_bytes();
            sbbf.check(bytes.as_slice())
        }
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => true,
    }
}

impl BoundPredicateVisitor for BloomFilterEvaluator<'_> {
    type T = bool;

    fn always_true(&mut self) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn always_false(&mut self) -> Result<Self::T> {
        ROW_GROUP_CANT_MATCH
    }

    fn and(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        Ok(lhs && rhs)
    }

    fn or(&mut self, lhs: Self::T, rhs: Self::T) -> Result<Self::T> {
        Ok(lhs || rhs)
    }

    fn not(&mut self, _inner: Self::T) -> Result<Self::T> {
        // Bloom filters are not invertible — we cannot prove presence,
        // so NOT of any result must conservatively return "might match".
        ROW_GROUP_MIGHT_MATCH
    }

    fn is_null(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn not_null(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn is_nan(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn not_nan(
        &mut self,
        _reference: &BoundReference,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn less_than(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn less_than_or_eq(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn greater_than(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn greater_than_or_eq(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn eq(
        &mut self,
        reference: &BoundReference,
        literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        if self.check_datum(reference, literal) {
            ROW_GROUP_MIGHT_MATCH
        } else {
            ROW_GROUP_CANT_MATCH
        }
    }

    fn not_eq(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn starts_with(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn not_starts_with(
        &mut self,
        _reference: &BoundReference,
        _literal: &Datum,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }

    fn r#in(
        &mut self,
        reference: &BoundReference,
        literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        let field_id = reference.field().id;
        let Some(column) = self.bloom_filters.get(&field_id) else {
            return ROW_GROUP_MIGHT_MATCH;
        };

        // If ANY literal might be present, the row group might match
        for literal in literals {
            if check_in_bloom_filter(column, literal) {
                return ROW_GROUP_MIGHT_MATCH;
            }
        }

        // All literals are definitely absent
        ROW_GROUP_CANT_MATCH
    }

    fn not_in(
        &mut self,
        _reference: &BoundReference,
        _literals: &FnvHashSet<Datum>,
        _predicate: &BoundPredicate,
    ) -> Result<Self::T> {
        ROW_GROUP_MIGHT_MATCH
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use parquet::basic::Type as PhysicalType;
    use parquet::bloom_filter::Sbbf;

    use super::{BloomFilterEvaluator, ColumnBloomFilter};
    use crate::expr::{Bind, Reference};
    use crate::spec::{Datum, NestedField, PrimitiveType, Schema, Type};

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap()
    }

    fn filter(values: &[i32]) -> ColumnBloomFilter {
        let mut bloom = Sbbf::new_with_ndv_fpp(values.len() as u64, 0.01).unwrap();
        for value in values {
            bloom.insert(value);
        }
        ColumnBloomFilter::new(bloom, PhysicalType::INT32, 0)
    }

    #[test]
    fn equality_prunes_only_definitely_absent_values() {
        let schema = schema();
        let filters = HashMap::from([(1, filter(&[1, 2, 3]))]);
        let present = Reference::new("id")
            .equal_to(Datum::int(2))
            .bind(schema.clone().into(), true)
            .unwrap();
        let absent = Reference::new("id")
            .equal_to(Datum::int(999))
            .bind(schema.into(), true)
            .unwrap();

        assert!(BloomFilterEvaluator::eval(&present, &filters).unwrap());
        assert!(!BloomFilterEvaluator::eval(&absent, &filters).unwrap());
    }

    #[test]
    fn in_predicate_keeps_group_when_any_value_might_exist() {
        let schema = schema();
        let filters = HashMap::from([(1, filter(&[1, 2, 3]))]);
        let some_present = Reference::new("id")
            .is_in([Datum::int(2), Datum::int(999)])
            .bind(schema.clone().into(), true)
            .unwrap();
        let all_absent = Reference::new("id")
            .is_in([Datum::int(998), Datum::int(999)])
            .bind(schema.into(), true)
            .unwrap();

        assert!(BloomFilterEvaluator::eval(&some_present, &filters).unwrap());
        assert!(!BloomFilterEvaluator::eval(&all_absent, &filters).unwrap());
    }
}
