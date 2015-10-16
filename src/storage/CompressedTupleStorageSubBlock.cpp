/*
   This file copyright (c) 2011-2013, the Quickstep authors.
   See file CREDITS.txt for details.
  
   This file is part of Quickstep.

   Quickstep is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
  
   Quickstep is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Quickstep.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "storage/CompressedTupleStorageSubBlock.hpp"

#include <cmath>
#include <cstddef>
#include <limits>
#include <utility>

#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "expressions/ComparisonPredicate.hpp"
#include "expressions/Predicate.hpp"
#include "expressions/Scalar.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/CompressionDictionary.hpp"
#include "types/IntType.hpp"
#include "types/LongType.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"

using std::ceil;
using std::floor;
using std::int64_t;
using std::numeric_limits;
using std::pair;
using std::size_t;
using std::uint32_t;

namespace quickstep {

CompressedTupleStorageSubBlock::CompressedTupleStorageSubBlock(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description,
    const bool new_block,
    void *sub_block_memory,
    const std::size_t sub_block_memory_size)
    : TupleStorageSubBlock(relation,
                           description,
                           new_block,
                           sub_block_memory,
                           sub_block_memory_size) {
  if (new_block) {
    if (sub_block_memory_size_ < sizeof(tuple_id) + sizeof(int)) {
      throw BlockMemoryTooSmall("CompressedTupleStorageSubBlock",
                                sub_block_memory_size_);
    }

    *static_cast<tuple_id*>(sub_block_memory_) = 0;
    builder_.reset(new CompressedBlockBuilder(relation_, description_, sub_block_memory_size_));
    if (builder_->getMinimumRequiredBlockSize() > sub_block_memory_size_) {
      throw BlockMemoryTooSmall("CompressedTupleStorageSubBlock",
                                sub_block_memory_size_);
    }
  } else {
    if (sub_block_memory_size_ < sizeof(tuple_id) + sizeof(int)) {
      throw MalformedBlock();
    }
    if (*reinterpret_cast<const int*>(static_cast<const char*>(sub_block_memory_) + sizeof(tuple_id)) <= 0) {
      throw MalformedBlock();
    }
    if (*reinterpret_cast<const int*>(static_cast<const char*>(sub_block_memory_) + sizeof(tuple_id))
        + sizeof(int) + sizeof(tuple_id)
        > sub_block_memory_size_) {
      throw MalformedBlock();
    }

    if (*static_cast<tuple_id*>(sub_block_memory_) == 0) {
      builder_.reset(new CompressedBlockBuilder(relation_, description_, sub_block_memory_size_));
      if (builder_->getMinimumRequiredBlockSize() > sub_block_memory_size_) {
        throw MalformedBlock();
      }
    }
  }
}

std::int64_t CompressedTupleStorageSubBlock::GetEffectiveLiteralValueForComparisonWithTruncatedAttribute(
    const Comparison::ComparisonID comp,
    const TypeInstance &right_literal) {
  switch (right_literal.getType().getTypeID()) {
    case Type::kFloat:
    case Type::kDouble:
      if (right_literal.numericGetDoubleValue() != right_literal.numericGetLongValue()) {
        switch (comp) {
          case Comparison::kLess:
          case Comparison::kGreaterOrEqual:
            return ceil(right_literal.numericGetDoubleValue());
          case Comparison::kLessOrEqual:
          case Comparison::kGreater:
            return floor(right_literal.numericGetDoubleValue());
          default:
            FATAL_ERROR("Unexpected ComparisonID in CompressedTupleStorageSubBlock::"
                        "GetEffectiveLiteralValueForComparisonWithTruncatedAttribute()");
        }
      }
    default:
      return right_literal.numericGetLongValue();
  }
}

bool CompressedTupleStorageSubBlock::insertTupleInBatch(
    const Tuple &tuple,
    const AllowedTypeConversion atc) {
#ifdef QUICKSTEP_DEBUG
  paranoidInsertTypeCheck(tuple, atc);
#endif

  if (builder_.empty()) {
    return false;
  }

  if (atc == kNone) {
    return builder_->addTuple(tuple, false);
  } else {
    return builder_->addTuple(tuple, true);
  }
}

const void* CompressedTupleStorageSubBlock::getAttributeValue(
    const tuple_id tuple,
    const attribute_id attr) const {
  DEBUG_ASSERT(hasTupleWithID(tuple));
  DEBUG_ASSERT(supportsUntypedGetAttributeValue(attr));

  if (dictionary_coded_attributes_[attr]) {
    return dictionaries_.at(attr).getUntypedValueForCode(compressedGetCode(tuple, attr));
  } else {
    return getAttributePtr(tuple, attr);
  }
}

TypeInstance* CompressedTupleStorageSubBlock::getAttributeValueTyped(
    const tuple_id tuple,
    const attribute_id attr) const {
  DEBUG_ASSERT(hasTupleWithID(tuple));

  const Type &attr_type = relation_.getAttributeById(attr).getType();
  if (supportsUntypedGetAttributeValue(attr)) {
    return attr_type.makeReferenceTypeInstance(getAttributeValue(tuple, attr));
  } else {
    DEBUG_ASSERT(truncated_attributes_[attr]);
    DEBUG_ASSERT((attr_type.getTypeID() == Type::kInt) || (attr_type.getTypeID() == Type::kLong));

    if (attr_type.getTypeID() == Type::kInt) {
      return static_cast<const IntType&>(attr_type).makeLiteralTypeInstance(compressedGetCode(tuple, attr));
    } else {
      return static_cast<const LongType&>(attr_type).makeLiteralTypeInstance(compressedGetCode(tuple, attr));
    }
  }
}

TupleIdSequence* CompressedTupleStorageSubBlock::getMatchesForPredicate(
    const Predicate *predicate) const {
  DEBUG_ASSERT(builder_.empty());
  if (predicate == NULL) {
    // No predicate, so pass through to base version to get all tuples.
    return TupleStorageSubBlock::getMatchesForPredicate(predicate);
  }

  // Determine if the predicate is a comparison of a compressed attribute with
  // a literal.
  if (predicate->isAttributeLiteralComparisonPredicate()) {
    const ComparisonPredicate &comparison_predicate = *static_cast<const ComparisonPredicate*>(predicate);

    const CatalogAttribute *comparison_attribute = NULL;
    bool left_literal = false;
    if (comparison_predicate.getLeftOperand().hasStaticValue()) {
      DEBUG_ASSERT(comparison_predicate.getRightOperand().getDataSource() == Scalar::kAttribute);
      comparison_attribute
          = &(static_cast<const ScalarAttribute&>(comparison_predicate.getRightOperand()).getAttribute());
      left_literal = true;
    } else {
      DEBUG_ASSERT(comparison_predicate.getLeftOperand().getDataSource() == Scalar::kAttribute);
      comparison_attribute
          = &(static_cast<const ScalarAttribute&>(comparison_predicate.getLeftOperand()).getAttribute());
      left_literal = false;
    }
    const attribute_id comparison_attribute_id = comparison_attribute->getID();

    DEBUG_ASSERT(comparison_attribute->getParent().getID() == relation_.getID());
    if (dictionary_coded_attributes_[comparison_attribute_id]
        || truncated_attributes_[comparison_attribute_id]) {
      const LiteralTypeInstance* comparison_literal;
      if (left_literal) {
        comparison_literal = &(comparison_predicate.getLeftOperand().getStaticValue());
      } else {
        comparison_literal = &(comparison_predicate.getRightOperand().getStaticValue());
      }

      if (comparison_predicate.getComparison().getComparisonID() == Comparison::kEqual) {
        return evaluateEqualPredicateOnCompressedAttribute(comparison_attribute_id,
                                                           *comparison_literal);
      } else if (comparison_predicate.getComparison().getComparisonID() == Comparison::kNotEqual) {
        return evaluateNotEqualPredicateOnCompressedAttribute(comparison_attribute_id,
                                                              *comparison_literal);
      } else {
        if (left_literal) {
          switch (comparison_predicate.getComparison().getComparisonID()) {
            case Comparison::kLess:
              return evaluateOtherComparisonPredicateOnCompressedAttribute(Comparison::kGreater,
                                                                           comparison_attribute_id,
                                                                           *comparison_literal);
            case Comparison::kLessOrEqual:
              return evaluateOtherComparisonPredicateOnCompressedAttribute(Comparison::kGreaterOrEqual,
                                                                           comparison_attribute_id,
                                                                           *comparison_literal);
            case Comparison::kGreater:
              return evaluateOtherComparisonPredicateOnCompressedAttribute(Comparison::kLess,
                                                                           comparison_attribute_id,
                                                                           *comparison_literal);
            case Comparison::kGreaterOrEqual:
              return evaluateOtherComparisonPredicateOnCompressedAttribute(Comparison::kLessOrEqual,
                                                                           comparison_attribute_id,
                                                                           *comparison_literal);
            default:
              FATAL_ERROR("Unexpected ComparisonID in "
                          "CompressedTupleStorageSubBlock::getMatchesForPredicate()");
          }
        } else {
          return evaluateOtherComparisonPredicateOnCompressedAttribute(
              comparison_predicate.getComparison().getComparisonID(),
              comparison_attribute_id,
              *comparison_literal);
        }
      }
    } else {
      // Attribute is uncompressed, so pass through.
      return TupleStorageSubBlock::getMatchesForPredicate(predicate);
    }
  } else {
    // Can not evaluate a non-comparison predicate, so pass through.
    return TupleStorageSubBlock::getMatchesForPredicate(predicate);
  }
}

bool CompressedTupleStorageSubBlock::compressedComparisonIsAlwaysTrueForTruncatedAttribute(
    const Comparison::ComparisonID comp,
    const attribute_id left_attr_id,
    const TypeInstance &right_literal) const {
  DEBUG_ASSERT(truncated_attributes_[left_attr_id]);
  int64_t effective_literal = right_literal.numericGetLongValue();

  // First, check equality and inequality.
  switch (comp) {
    case Comparison::kEqual:
      return false;
    case Comparison::kNotEqual:
      switch (right_literal.getType().getTypeID()) {
        case Type::kFloat:
        case Type::kDouble:
          if (right_literal.numericGetDoubleValue() != effective_literal) {
            // Literal is a float or double with a fractional part.
            return true;
          }
        default:
          break;
      }
      if ((effective_literal < 0)
          || (effective_literal > GetMaxTruncatedValue(compression_info_.attribute_size(left_attr_id)))) {
        return true;
      }
      return false;
    default:
      effective_literal
          = GetEffectiveLiteralValueForComparisonWithTruncatedAttribute(comp,
                                                                        right_literal);
      break;
  }

  switch (comp) {
    case Comparison::kLess:
      if (effective_literal > GetMaxTruncatedValue(compression_info_.attribute_size(left_attr_id))) {
        return true;
      } else {
        return false;
      }
    case Comparison::kLessOrEqual:
      if (effective_literal >= GetMaxTruncatedValue(compression_info_.attribute_size(left_attr_id))) {
        return true;
      } else {
        return false;
      }
    case Comparison::kGreater:
      if (effective_literal < 0) {
        return true;
      } else {
        return false;
      }
    case Comparison::kGreaterOrEqual:
      if (effective_literal <= 0) {
        return true;
      } else {
        return false;
      }
    default:
      FATAL_ERROR("Unexpected ComparisonID in CompressedTupleStorageSubBlock::"
                  "comparisonIsAlwaysTrueForTruncatedAttribute()");
  }
}

bool CompressedTupleStorageSubBlock::compressedComparisonIsAlwaysFalseForTruncatedAttribute(
    const Comparison::ComparisonID comp,
    const attribute_id left_attr_id,
    const TypeInstance &right_literal) const {
  DEBUG_ASSERT(truncated_attributes_[left_attr_id]);
  int64_t effective_literal = right_literal.numericGetLongValue();

  // First, check equality and inequality.
  switch (comp) {
    case Comparison::kEqual:
      switch (right_literal.getType().getTypeID()) {
        case Type::kFloat:
        case Type::kDouble:
          if (right_literal.numericGetDoubleValue() != effective_literal) {
            // Literal is a float or double with a fractional part.
            return true;
          } else if ((effective_literal < 0)
                    || (effective_literal
                        > GetMaxTruncatedValue(compression_info_.attribute_size(left_attr_id)))) {
            return true;
          }
        default:
          break;
      }
      if ((effective_literal < 0)
          || (effective_literal > GetMaxTruncatedValue(compression_info_.attribute_size(left_attr_id)))) {
        return true;
      }
      return false;
    case Comparison::kNotEqual:
      return false;
    default:
      effective_literal
          = GetEffectiveLiteralValueForComparisonWithTruncatedAttribute(comp,
                                                                        right_literal);
      break;
  }

  switch (comp) {
    case Comparison::kLess:
      if (effective_literal <= 0) {
        return true;
      } else {
        return false;
      }
    case Comparison::kLessOrEqual:
      if (effective_literal < 0) {
        return true;
      } else {
        return false;
      }
    case Comparison::kGreater:
      if (effective_literal >= GetMaxTruncatedValue(compression_info_.attribute_size(left_attr_id))) {
        return true;
      } else {
        return false;
      }
    case Comparison::kGreaterOrEqual:
      if (effective_literal > GetMaxTruncatedValue(compression_info_.attribute_size(left_attr_id))) {
        return true;
      } else {
        return false;
      }
    default:
      FATAL_ERROR("Unexpected ComparisonID in CompressedTupleStorageSubBlock::"
                  "comparisonIsAlwaysFalseForTruncatedAttribute()");
  }
}

void* CompressedTupleStorageSubBlock::initializeCommon() {
  if (!compression_info_.ParseFromArray(
      static_cast<const char*>(sub_block_memory_) + sizeof(tuple_id) + sizeof(int),
      *reinterpret_cast<const int*>(static_cast<const char*>(sub_block_memory_) + sizeof(tuple_id)))) {
    throw MalformedBlock();
  }

  if ((relation_.getMaxAttributeId() + 1 != compression_info_.attribute_size_size())
      || (relation_.getMaxAttributeId() + 1 != compression_info_.dictionary_size_size())) {
    throw MalformedBlock();
  }

  dictionary_coded_attributes_.resize(relation_.getMaxAttributeId() + 1, false);
  truncated_attributes_.resize(relation_.getMaxAttributeId() + 1, false);
  size_t dictionary_offset =
      sizeof(tuple_id) + sizeof(int)
      + *reinterpret_cast<const int*>(static_cast<const char*>(sub_block_memory_) + sizeof(tuple_id));
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    const Type &attr_type = attr_it->getType();
    if (attr_type.isVariableLength()) {
      if (compression_info_.dictionary_size(attr_it->getID()) == 0) {
        throw MalformedBlock();
      }
    }

    if (compression_info_.dictionary_size(attr_it->getID()) > 0) {
      if (attr_type.isVariableLength()) {
        dictionaries_.insert(
            attr_it->getID(),
            new VariableLengthTypeCompressionDictionary(
                attr_type,
                static_cast<const char*>(sub_block_memory_) + dictionary_offset,
                compression_info_.dictionary_size(attr_it->getID())));
      } else {
        dictionaries_.insert(
            attr_it->getID(),
            new FixedLengthTypeCompressionDictionary(
                attr_type,
                static_cast<const char*>(sub_block_memory_) + dictionary_offset,
                compression_info_.dictionary_size(attr_it->getID())));
      }

      dictionary_coded_attributes_[attr_it->getID()] = true;
      dictionary_offset += compression_info_.dictionary_size(attr_it->getID());
    } else if (compression_info_.attribute_size(attr_it->getID())
               != attr_type.maximumByteLength()) {
      switch (attr_type.getTypeID()) {
        case Type::kInt:
        case Type::kLong:
          switch (compression_info_.attribute_size(attr_it->getID())) {
            case 1:
            case 2:
            case 4:
              truncated_attributes_[attr_it->getID()] = true;
              break;
            default:
              throw MalformedBlock();
          }
          break;
        default:
          throw MalformedBlock();
      }
    }
  }

  return static_cast<char*>(sub_block_memory_) + dictionary_offset;
}

TupleIdSequence* CompressedTupleStorageSubBlock::evaluateEqualPredicateOnCompressedAttribute(
    const attribute_id left_attr_id,
    const TypeInstance &right_literal) const {
  uint32_t match_code;
  if (dictionary_coded_attributes_[left_attr_id]) {
    const CompressionDictionary &dictionary = *(dictionaries_.find(left_attr_id)->second);
    match_code = dictionary.getCodeForTypedValue(right_literal);
    if (match_code == dictionary.numberOfCodes()) {
      return new TupleIdSequence();
    }
  } else {
    if (compressedComparisonIsAlwaysFalseForTruncatedAttribute(Comparison::kEqual,
                                                               left_attr_id,
                                                               right_literal)) {
      return new TupleIdSequence();
    }
    match_code = right_literal.numericGetLongValue();
  }

  return getEqualCodes(left_attr_id, match_code);
}

TupleIdSequence* CompressedTupleStorageSubBlock::evaluateNotEqualPredicateOnCompressedAttribute(
    const attribute_id left_attr_id,
    const TypeInstance &right_literal) const {
  uint32_t match_code;
  if (dictionary_coded_attributes_[left_attr_id]) {
    const CompressionDictionary &dictionary = *(dictionaries_.find(left_attr_id)->second);
    match_code = dictionary.getCodeForTypedValue(right_literal);
    if (match_code == dictionary.numberOfCodes()) {
      return TupleStorageSubBlock::getMatchesForPredicate(NULL);
    }
  } else {
    if (compressedComparisonIsAlwaysTrueForTruncatedAttribute(Comparison::kNotEqual,
                                                              left_attr_id,
                                                              right_literal)) {
      return TupleStorageSubBlock::getMatchesForPredicate(NULL);
    }
    match_code = right_literal.numericGetLongValue();
  }

  return getNotEqualCodes(left_attr_id, match_code);
}

TupleIdSequence* CompressedTupleStorageSubBlock::evaluateOtherComparisonPredicateOnCompressedAttribute(
    const Comparison::ComparisonID comp,
    const attribute_id left_attr_id,
    const TypeInstance &right_literal) const {
  pair<uint32_t, uint32_t> match_range;
  if (dictionary_coded_attributes_[left_attr_id]) {
    const CompressionDictionary &dictionary = *(dictionaries_.find(left_attr_id)->second);
    match_range = dictionary.getLimitCodesForComparisonTyped(comp,
                                                             right_literal);
    if (match_range.first == match_range.second) {
      // No matches.
      return new TupleIdSequence();
    }
    if (match_range.second == dictionary.numberOfCodes()) {
      // This trick lets us skip an unnecessary comparison.
      match_range.second = numeric_limits<uint32_t>::max();
    }
  } else {
    if (compressedComparisonIsAlwaysTrueForTruncatedAttribute(comp, left_attr_id, right_literal)) {
      return TupleStorageSubBlock::getMatchesForPredicate(NULL);
    }
    if (compressedComparisonIsAlwaysFalseForTruncatedAttribute(comp, left_attr_id, right_literal)) {
      return new TupleIdSequence();
    }

    int64_t effective_literal
        = GetEffectiveLiteralValueForComparisonWithTruncatedAttribute(comp,
                                                                      right_literal);
    switch (comp) {
      case Comparison::kLess:
        match_range.first = 0;
        match_range.second = effective_literal;
        break;
      case Comparison::kLessOrEqual:
        match_range.first = 0;
        match_range.second = effective_literal + 1;
        break;
      case Comparison::kGreater:
        match_range.first = effective_literal + 1;
        match_range.second = numeric_limits<uint32_t>::max();
        break;
      case Comparison::kGreaterOrEqual:
        match_range.first = effective_literal;
        match_range.second = numeric_limits<uint32_t>::max();
        break;
      default:
        FATAL_ERROR("Unexpected ComparisonID in CompressedTupleStorageSubBlock::"
                    "evaluateOtherComparisonPredicateOnCompressedAttribute()");
    }
  }

  if (match_range.first == 0) {
    if (match_range.second == numeric_limits<uint32_t>::max()) {
      return TupleStorageSubBlock::getMatchesForPredicate(NULL);
    } else {
      return getLessCodes(left_attr_id, match_range.second);
    }
  } else if (match_range.second == numeric_limits<uint32_t>::max()) {
    return getGreaterOrEqualCodes(left_attr_id, match_range.first);
  } else {
    return getCodesInRange(left_attr_id, match_range);
  }
}

}  // namespace quickstep
