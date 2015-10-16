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

#include "storage/CompressedColumnStoreTupleStorageSubBlock.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <functional>
#include <limits>
#include <utility>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "storage/ColumnStoreUtil.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/TupleIdSequence.hpp"
#include "types/Comparison.hpp"
#include "types/Type.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"

using std::equal_to;
using std::greater;
using std::less_equal;
using std::lower_bound;
using std::memmove;
using std::not_equal_to;
using std::numeric_limits;
using std::pair;
using std::size_t;
using std::uint8_t;
using std::uint16_t;
using std::uint32_t;

using quickstep::column_store_util::SortColumnPredicateEvaluator;

namespace quickstep {

CompressedColumnStoreTupleStorageSubBlock::CompressedColumnStoreTupleStorageSubBlock(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description,
    const bool new_block,
    void *sub_block_memory,
    const std::size_t sub_block_memory_size)
    : CompressedTupleStorageSubBlock(relation,
                                     description,
                                     new_block,
                                     sub_block_memory,
                                     sub_block_memory_size) {
  if (!DescriptionIsValid(relation_, description_)) {
    FATAL_ERROR("Attempted to construct a CompressedColumnStoreTupleStorageSubBlock "
                "from an invalid description.");
  }

  sort_column_id_ = description_.GetExtension(
      CompressedColumnStoreTupleStorageSubBlockDescription::sort_attribute_id);

  if ((!new_block) && (*static_cast<tuple_id*>(sub_block_memory_) != 0)) {
    initialize();
  }
}

bool CompressedColumnStoreTupleStorageSubBlock::DescriptionIsValid(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description) {
  // Make sure description is initialized and specifies CompressedColumnStore.
  if (!description.IsInitialized()) {
    return false;
  }
  if (description.sub_block_type() != TupleStorageSubBlockDescription::COMPRESSED_COLUMN_STORE) {
    return false;
  }

  // Make sure relation does not have nullable attributes.
  if (relation.hasNullableAttributes()) {
    return false;
  }

  const Comparison &less_comparison = Comparison::GetComparison(Comparison::kLess);

  // Make sure the specified sort attribute exists and can be ordered by
  // LessComparison.
  if (!description.HasExtension(CompressedColumnStoreTupleStorageSubBlockDescription::sort_attribute_id)) {
    return false;
  }
  attribute_id sort_attribute_id = description.GetExtension(
      CompressedColumnStoreTupleStorageSubBlockDescription::sort_attribute_id);
  if (!relation.hasAttributeWithId(sort_attribute_id)) {
    return false;
  }
  const Type &sort_attr_type = relation.getAttributeById(sort_attribute_id).getType();
  if (!less_comparison.canCompareTypes(sort_attr_type, sort_attr_type)) {
    return false;
  }

  // Make sure all the specified compressed attributes exist and can be ordered
  // by LessComparison.
  CompatUnorderedSet<attribute_id>::unordered_set compressed_variable_length_attributes;
  for (int compressed_attribute_num = 0;
       compressed_attribute_num < description.ExtensionSize(
           CompressedColumnStoreTupleStorageSubBlockDescription::compressed_attribute_id);
       ++compressed_attribute_num) {
    attribute_id compressed_attribute_id = description.GetExtension(
        CompressedColumnStoreTupleStorageSubBlockDescription::compressed_attribute_id,
        compressed_attribute_num);
    if (!relation.hasAttributeWithId(compressed_attribute_id)) {
      return false;
    }
    const Type &attr_type = relation.getAttributeById(compressed_attribute_id).getType();
    if (!less_comparison.canCompareTypes(attr_type, attr_type)) {
      return false;
    }
    if (attr_type.isVariableLength()) {
      compressed_variable_length_attributes.insert(compressed_attribute_id);
    }
  }

  // If the relation has variable-length attributes, make sure they are all
  // compressed.
  if (relation.isVariableLength()) {
    for (CatalogRelation::const_iterator attr_it = relation.begin();
         attr_it != relation.end();
         ++attr_it) {
      if (attr_it->getType().isVariableLength()) {
        if (compressed_variable_length_attributes.find(attr_it->getID())
            == compressed_variable_length_attributes.end()) {
          return false;
        }
      }
    }
  }

  return true;
}

// TODO(chasseur): Make this heuristic better.
std::size_t CompressedColumnStoreTupleStorageSubBlock::EstimateBytesPerTuple(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description) {
  DEBUG_ASSERT(DescriptionIsValid(relation, description));

  CompatUnorderedSet<attribute_id>::unordered_set compressed_attributes;
  for (int compressed_attribute_num = 0;
       compressed_attribute_num < description.ExtensionSize(
           CompressedColumnStoreTupleStorageSubBlockDescription::compressed_attribute_id);
       ++compressed_attribute_num) {
    compressed_attributes.insert(description.GetExtension(
        CompressedColumnStoreTupleStorageSubBlockDescription::compressed_attribute_id,
        compressed_attribute_num));
  }

  size_t total_size = 0;
  for (CatalogRelation::const_iterator attr_it = relation.begin();
       attr_it != relation.end();
       ++attr_it) {
    if (compressed_attributes.find(attr_it->getID()) == compressed_attributes.end()) {
      total_size += attr_it->getType().estimateAverageByteLength();
    } else {
      // For compressed attributes, estimate 1/3 space.
      total_size += attr_it->getType().estimateAverageByteLength() / 3;
    }
  }

  return total_size;
}

bool CompressedColumnStoreTupleStorageSubBlock::deleteTuple(const tuple_id tuple) {
  DEBUG_ASSERT(hasTupleWithID(tuple));

  if (tuple == *static_cast<const tuple_id*>(sub_block_memory_) - 1) {
    --(*static_cast<tuple_id*>(sub_block_memory_));
    return false;
  } else {
    // Shift subsequent tuples forward.
    shiftTuples(tuple, tuple + 1, *static_cast<const tuple_id*>(sub_block_memory_) - tuple - 1);
    --(*static_cast<tuple_id*>(sub_block_memory_));
    return true;
  }
}

TupleIdSequence* CompressedColumnStoreTupleStorageSubBlock::getMatchesForPredicate(
    const Predicate *predicate) const {
  if (predicate == NULL) {
    // Pass through all the way to the base version to get all tuples.
    return TupleStorageSubBlock::getMatchesForPredicate(predicate);
  }

  if (dictionary_coded_attributes_[sort_column_id_] || truncated_attributes_[sort_column_id_]) {
    // NOTE(chasseur): The version from CompressedTupleStorageSubBlock will in
    // turn call getEqualCodes(), getNotEqualCodes(), or getCodesInRange() as
    // necessary for this block, which will use a fast binary search if
    // evaluating a predicate on the sort column.
    return CompressedTupleStorageSubBlock::getMatchesForPredicate(predicate);
  } else {
    TupleIdSequence *matches = SortColumnPredicateEvaluator::EvaluatePredicateForUncompressedSortColumn(
        *predicate,
        relation_,
        sort_column_id_,
        column_stripes_[sort_column_id_],
        *static_cast<const tuple_id*>(sub_block_memory_));
    if (matches == NULL) {
      return CompressedTupleStorageSubBlock::getMatchesForPredicate(predicate);
    } else {
      return matches;
    }
  }
}

void CompressedColumnStoreTupleStorageSubBlock::rebuild() {
  if (!builder_.empty()) {
    builder_->buildCompressedColumnStoreTupleStorageSubBlock(sub_block_memory_);
    builder_.reset();
    initialize();
  }
}

std::uint32_t CompressedColumnStoreTupleStorageSubBlock::compressedGetCode(
    const tuple_id tid,
    const attribute_id attr_id) const {
  DEBUG_ASSERT(hasTupleWithID(tid));
  DEBUG_ASSERT((dictionary_coded_attributes_[attr_id]) || (truncated_attributes_[attr_id]));
  const void *code_location = static_cast<const char*>(column_stripes_[attr_id])
                              + tid * compression_info_.attribute_size(attr_id);
  switch (compression_info_.attribute_size(attr_id)) {
    case 1:
      return *static_cast<const uint8_t*>(code_location);
    case 2:
      return *static_cast<const uint16_t*>(code_location);
    case 4:
      return *static_cast<const uint32_t*>(code_location);
    default:
      FATAL_ERROR("Unexpected byte-length (not 1, 2, or 4) for compressed "
                  "attribute ID " << attr_id
                  << " in CompressedColumnStoreTupleStorageSubBlock::getCode()");
  }
}

TupleIdSequence* CompressedColumnStoreTupleStorageSubBlock::getEqualCodes(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  if (attr_id == sort_column_id_) {
    // Special (fast) case: do a binary search of the sort column.
    pair<uint32_t, uint32_t> code_range(code, code + 1);

    // Adjust the upper limit if doing so can avoid an extra binary search.
    if (dictionary_coded_attributes_[attr_id]) {
      if (code_range.second == compressedGetDictionary(attr_id).numberOfCodes()) {
        code_range.second = numeric_limits<uint32_t>::max();
      }
    } else if (code_range.first == GetMaxTruncatedValue(compression_info_.attribute_size(attr_id))) {
      code_range.second = numeric_limits<uint32_t>::max();
    }

    pair<tuple_id, tuple_id> tuple_range = getCompressedSortColumnRange(code_range);
    TupleIdSequence *matches = new TupleIdSequence();
    for (tuple_id tid = tuple_range.first;
         tid < tuple_range.second;
         ++tid) {
      matches->append(tid);
    }
    return matches;
  } else {
    return getCodesSatisfyingComparison<equal_to>(attr_id, code);
  }
}

TupleIdSequence* CompressedColumnStoreTupleStorageSubBlock::getNotEqualCodes(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  if (attr_id == sort_column_id_) {
    // Special (fast) case: do a binary search of the sort column.
    pair<uint32_t, uint32_t> code_range(code, code + 1);

    // Adjust the upper limit if doing so can avoid an extra binary search.
    if (dictionary_coded_attributes_[attr_id]) {
      if (code_range.second == compressedGetDictionary(attr_id).numberOfCodes()) {
        code_range.second = numeric_limits<uint32_t>::max();
      }
    } else if (code_range.first == GetMaxTruncatedValue(compression_info_.attribute_size(attr_id))) {
      code_range.second = numeric_limits<uint32_t>::max();
    }

    pair<tuple_id, tuple_id> tuple_range = getCompressedSortColumnRange(code_range);

    // We searched for the range of equal codes, so return its complement.
    TupleIdSequence *matches = new TupleIdSequence();
    for (tuple_id tid = 0;
         tid < tuple_range.first;
         ++tid) {
      matches->append(tid);
    }
    for (tuple_id tid = tuple_range.second;
         tid < *static_cast<const tuple_id*>(sub_block_memory_);
         ++tid) {
      matches->append(tid);
    }
    return matches;
  } else {
    return getCodesSatisfyingComparison<not_equal_to>(attr_id, code);
  }
}

TupleIdSequence* CompressedColumnStoreTupleStorageSubBlock::getLessCodes(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  if (attr_id == sort_column_id_) {
    // Special (fast) case: do a binary search of the sort column.
    TupleIdSequence *matches = new TupleIdSequence();
    pair<tuple_id, tuple_id> tuple_range
        = getCompressedSortColumnRange(pair<uint32_t, uint32_t>(0, code));
    for (tuple_id tid = tuple_range.first;
         tid < tuple_range.second;
         ++tid) {
      matches->append(tid);
    }
    return matches;
  } else {
    return getCodesSatisfyingComparison<greater>(attr_id, code);
  }
}

TupleIdSequence* CompressedColumnStoreTupleStorageSubBlock::getGreaterOrEqualCodes(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  if (attr_id == sort_column_id_) {
    // Special (fast) case: do a binary search of the sort column.
    TupleIdSequence *matches = new TupleIdSequence();
    pair<tuple_id, tuple_id> tuple_range
        = getCompressedSortColumnRange(pair<uint32_t, uint32_t>(code, numeric_limits<uint32_t>::max()));
    for (tuple_id tid = tuple_range.first;
         tid < tuple_range.second;
         ++tid) {
      matches->append(tid);
    }
    return matches;
  } else {
    return getCodesSatisfyingComparison<less_equal>(attr_id, code);
  }
}

TupleIdSequence* CompressedColumnStoreTupleStorageSubBlock::getCodesInRange(
    const attribute_id attr_id,
    const std::pair<std::uint32_t, std::uint32_t> range) const {
  TupleIdSequence *matches = new TupleIdSequence();
  if (attr_id == sort_column_id_) {
    // Special (fast) case: do a binary search of the sort column.
    pair<tuple_id, tuple_id> tuple_range = getCompressedSortColumnRange(range);
    for (tuple_id tid = tuple_range.first;
         tid < tuple_range.second;
         ++tid) {
      matches->append(tid);
    }
  } else {
    const void *attr_stripe = column_stripes_[attr_id];
    switch (compression_info_.attribute_size(attr_id)) {
      case 1:
        for (tuple_id tid = 0;
             tid < *static_cast<const tuple_id*>(sub_block_memory_);
             ++tid) {
          if (range.first <= (static_cast<const uint8_t*>(attr_stripe)[tid])
              && (static_cast<const uint8_t*>(attr_stripe)[tid] < range.second)) {
            matches->append(tid);
          }
        }
        break;
      case 2:
        for (tuple_id tid = 0;
             tid < *static_cast<const tuple_id*>(sub_block_memory_);
             ++tid) {
          if (range.first <= (static_cast<const uint16_t*>(attr_stripe)[tid])
              && (static_cast<const uint16_t*>(attr_stripe)[tid] < range.second)) {
            matches->append(tid);
          }
        }
        break;
      case 4:
        for (tuple_id tid = 0;
             tid < *static_cast<const tuple_id*>(sub_block_memory_);
             ++tid) {
          if (range.first <= (static_cast<const uint32_t*>(attr_stripe)[tid])
              && (static_cast<const uint32_t*>(attr_stripe)[tid] < range.second)) {
            matches->append(tid);
          }
        }
        break;
      default:
        FATAL_ERROR("Unexpected byte-length (not 1, 2, or 4) for compressed "
                    "attribute ID " << attr_id
                    << " in CompressedColumnStoreTupleStorageSubBlock::getCodesInRange()");
    }
  }

  return matches;
}

void CompressedColumnStoreTupleStorageSubBlock::initialize() {
  void *stripe_location = initializeCommon();

  size_t tuple_length = 0;
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    tuple_length += compression_info_.attribute_size(attr_it->getID());
  }

  size_t max_num_tuples = (static_cast<const char*>(sub_block_memory_)
                          + sub_block_memory_size_ - static_cast<const char*>(stripe_location))
                          / tuple_length;

  column_stripes_.resize(relation_.getMaxAttributeId() + 1, NULL);

  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    column_stripes_[attr_it->getID()] = stripe_location;
    stripe_location = static_cast<char*>(stripe_location)
                      + max_num_tuples * compression_info_.attribute_size(attr_it->getID());
  }
}

void CompressedColumnStoreTupleStorageSubBlock::shiftTuples(
    const tuple_id dest_position,
    const tuple_id src_tuple,
    const tuple_id num_tuples) {
  for (attribute_id attr_id = 0;
       attr_id < compression_info_.attribute_size_size();
       ++attr_id) {
    size_t attr_length = compression_info_.attribute_size(attr_id);
    if (attr_length > 0) {
      memmove(static_cast<char*>(column_stripes_[attr_id]) + dest_position * attr_length,
              static_cast<const char*>(column_stripes_[attr_id]) + src_tuple * attr_length,
              attr_length * num_tuples);
    }
  }
}

std::pair<tuple_id, tuple_id> CompressedColumnStoreTupleStorageSubBlock::getCompressedSortColumnRange(
    const std::pair<std::uint32_t, std::uint32_t> code_range) const {
  DEBUG_ASSERT(dictionary_coded_attributes_[sort_column_id_] || truncated_attributes_[sort_column_id_]);

  const void *attr_stripe = column_stripes_[sort_column_id_];
  pair<tuple_id, tuple_id> tuple_range;
  if (code_range.first == 0) {
    tuple_range.first = 0;
  } else {
    switch (compression_info_.attribute_size(sort_column_id_)) {
      case 1:
        tuple_range.first = lower_bound(static_cast<const uint8_t*>(attr_stripe),
                                        static_cast<const uint8_t*>(attr_stripe)
                                            + *static_cast<const tuple_id*>(sub_block_memory_),
                                        code_range.first)
                            - static_cast<const uint8_t*>(attr_stripe);
        break;
      case 2:
        tuple_range.first = lower_bound(static_cast<const uint16_t*>(attr_stripe),
                                        static_cast<const uint16_t*>(attr_stripe)
                                            + *static_cast<const tuple_id*>(sub_block_memory_),
                                        code_range.first)
                            - static_cast<const uint16_t*>(attr_stripe);
        break;
      case 4:
        tuple_range.first = lower_bound(static_cast<const uint32_t*>(attr_stripe),
                                        static_cast<const uint32_t*>(attr_stripe)
                                            + *static_cast<const tuple_id*>(sub_block_memory_),
                                        code_range.first)
                            - static_cast<const uint32_t*>(attr_stripe);
        break;
      default:
        FATAL_ERROR("Unexpected byte-length (not 1, 2, or 4) for compressed "
                    "attribute ID " << sort_column_id_
                    << " in CompressedColumnStoreTupleStorageSubBlock::getCompressedSortColumnRange()");
    }
  }

  if (code_range.second == numeric_limits<uint32_t>::max()) {
    tuple_range.second = *static_cast<const tuple_id*>(sub_block_memory_);
  } else {
    switch (compression_info_.attribute_size(sort_column_id_)) {
      case 1:
        tuple_range.second = lower_bound(static_cast<const uint8_t*>(attr_stripe),
                                         static_cast<const uint8_t*>(attr_stripe)
                                             + *static_cast<const tuple_id*>(sub_block_memory_),
                                         code_range.second)
                             - static_cast<const uint8_t*>(attr_stripe);
        break;
      case 2:
        tuple_range.second = lower_bound(static_cast<const uint16_t*>(attr_stripe),
                                         static_cast<const uint16_t*>(attr_stripe)
                                             + *static_cast<const tuple_id*>(sub_block_memory_),
                                         code_range.second)
                             - static_cast<const uint16_t*>(attr_stripe);
        break;
      case 4:
        tuple_range.second = lower_bound(static_cast<const uint32_t*>(attr_stripe),
                                         static_cast<const uint32_t*>(attr_stripe)
                                             + *static_cast<const tuple_id*>(sub_block_memory_),
                                         code_range.second)
                             - static_cast<const uint32_t*>(attr_stripe);
        break;
      default:
        FATAL_ERROR("Unexpected byte-length (not 1, 2, or 4) for compressed "
                    "attribute ID " << sort_column_id_
                    << " in CompressedColumnStoreTupleStorageSubBlock::getCompressedSortColumnRange()");
    }
  }

  return tuple_range;
}

template <template <typename T> class comparison_functor>
TupleIdSequence* CompressedColumnStoreTupleStorageSubBlock::getCodesSatisfyingComparison(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  comparison_functor<uint32_t> comp;
  TupleIdSequence *matches = new TupleIdSequence();
  const void *attr_stripe = column_stripes_[attr_id];
  switch (compression_info_.attribute_size(attr_id)) {
    case 1:
      for (tuple_id tid = 0;
           tid < *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid) {
        if (comp(code, static_cast<const uint8_t*>(attr_stripe)[tid])) {
          matches->append(tid);
        }
      }
      break;
    case 2:
      for (tuple_id tid = 0;
           tid < *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid) {
        if (comp(code, static_cast<const uint16_t*>(attr_stripe)[tid])) {
          matches->append(tid);
        }
      }
      break;
    case 4:
      for (tuple_id tid = 0;
           tid <= *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid) {
        if (comp(code, static_cast<const uint32_t*>(attr_stripe)[tid])) {
          matches->append(tid);
        }
      }
      break;
    default:
      FATAL_ERROR("Unexpected byte-length (not 1, 2, or 4) for compressed "
                  "attribute ID " << attr_id
                  << " in CompressedColumnStoreTupleStorageSubBlock::getCodesSatisfyingComparison()");
  }

  return matches;
}

}  // namespace quickstep
