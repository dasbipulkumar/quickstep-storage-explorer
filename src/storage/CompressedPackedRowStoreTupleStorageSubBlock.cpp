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

#include "storage/CompressedPackedRowStoreTupleStorageSubBlock.hpp"

#include <cstddef>
#include <cstring>
#include <functional>
#include <utility>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
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
using std::memmove;
using std::not_equal_to;
using std::pair;
using std::size_t;
using std::uint8_t;
using std::uint16_t;
using std::uint32_t;

namespace quickstep {

CompressedPackedRowStoreTupleStorageSubBlock::CompressedPackedRowStoreTupleStorageSubBlock(
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
    FATAL_ERROR("Attempted to construct a CompressedPackedRowStoreTupleStorageSubBlock "
                "from an invalid description.");
  }

  if ((!new_block) && (*static_cast<tuple_id*>(sub_block_memory_) != 0)) {
    initialize();
  }
}

bool CompressedPackedRowStoreTupleStorageSubBlock::DescriptionIsValid(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description) {
  // Make sure description is initialized and specifies
  // CompressedPackedRowStore.
  if (!description.IsInitialized()) {
    return false;
  }
  if (description.sub_block_type() != TupleStorageSubBlockDescription::COMPRESSED_PACKED_ROW_STORE) {
    return false;
  }

  // Make sure relation does not have nullable attributes.
  if (relation.hasNullableAttributes()) {
    return false;
  }

  // Make sure all the specified compressed attributes exist and can be ordered
  // by LessComparison.
  const Comparison &less_comparison = Comparison::GetComparison(Comparison::kLess);
  CompatUnorderedSet<attribute_id>::unordered_set compressed_variable_length_attributes;
  for (int compressed_attribute_num = 0;
       compressed_attribute_num < description.ExtensionSize(
           CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id);
       ++compressed_attribute_num) {
    attribute_id compressed_attribute_id = description.GetExtension(
        CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id,
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
std::size_t CompressedPackedRowStoreTupleStorageSubBlock::EstimateBytesPerTuple(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description) {
  DEBUG_ASSERT(DescriptionIsValid(relation, description));

  CompatUnorderedSet<attribute_id>::unordered_set compressed_attributes;
  for (int compressed_attribute_num = 0;
       compressed_attribute_num < description.ExtensionSize(
           CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id);
       ++compressed_attribute_num) {
    compressed_attributes.insert(description.GetExtension(
        CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id,
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

bool CompressedPackedRowStoreTupleStorageSubBlock::deleteTuple(const tuple_id tuple) {
  DEBUG_ASSERT(hasTupleWithID(tuple));

  if (tuple == *static_cast<const tuple_id*>(sub_block_memory_) - 1) {
    --(*static_cast<tuple_id*>(sub_block_memory_));
    return false;
  } else {
    // Shift subsequent tuples forward.
    memmove(static_cast<char*>(tuple_storage_) + tuple * tuple_length_bytes_,
            static_cast<const char*>(tuple_storage_) + (tuple + 1) * tuple_length_bytes_,
            (*static_cast<const tuple_id*>(sub_block_memory_) - tuple - 1) * tuple_length_bytes_);
    --(*static_cast<tuple_id*>(sub_block_memory_));
    return true;
  }
}

void CompressedPackedRowStoreTupleStorageSubBlock::rebuild() {
  if (!builder_.empty()) {
    builder_->buildCompressedPackedRowStoreTupleStorageSubBlock(sub_block_memory_);
    builder_.reset();
    initialize();
  }
}

std::uint32_t CompressedPackedRowStoreTupleStorageSubBlock::compressedGetCode(
    const tuple_id tid,
    const attribute_id attr_id) const {
  DEBUG_ASSERT(hasTupleWithID(tid));
  DEBUG_ASSERT((dictionary_coded_attributes_[attr_id]) || (truncated_attributes_[attr_id]));
  const void *code_location = static_cast<const char*>(tuple_storage_)
                              + tid * tuple_length_bytes_
                              + attribute_offsets_[attr_id];
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
                  << " in CompressedPackedRowStoreTupleStorageSubBlock::getCode()");
  }
}

TupleIdSequence* CompressedPackedRowStoreTupleStorageSubBlock::getEqualCodes(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  return getCodesSatisfyingComparison<equal_to>(attr_id, code);
}

TupleIdSequence* CompressedPackedRowStoreTupleStorageSubBlock::getNotEqualCodes(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  return getCodesSatisfyingComparison<not_equal_to>(attr_id, code);
}

TupleIdSequence* CompressedPackedRowStoreTupleStorageSubBlock::getLessCodes(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  return getCodesSatisfyingComparison<greater>(attr_id, code);
}

TupleIdSequence* CompressedPackedRowStoreTupleStorageSubBlock::getGreaterOrEqualCodes(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  return getCodesSatisfyingComparison<less_equal>(attr_id, code);
}

TupleIdSequence* CompressedPackedRowStoreTupleStorageSubBlock::getCodesInRange(
    const attribute_id attr_id,
    const std::pair<std::uint32_t, std::uint32_t> range) const {
  TupleIdSequence *matches = new TupleIdSequence();
  const char *attr_location = static_cast<const char*>(tuple_storage_)
                              + attribute_offsets_[attr_id];
  switch (compression_info_.attribute_size(attr_id)) {
    case 1:
      for (tuple_id tid = 0;
           tid < *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid, attr_location += tuple_length_bytes_) {
        if (range.first <= (*reinterpret_cast<const uint8_t*>(attr_location))
            && (*reinterpret_cast<const uint8_t*>(attr_location) < range.second)) {
          matches->append(tid);
        }
      }
      break;
    case 2:
      for (tuple_id tid = 0;
           tid < *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid, attr_location += tuple_length_bytes_) {
        if (range.first <= (*reinterpret_cast<const uint16_t*>(attr_location))
            && (*reinterpret_cast<const uint16_t*>(attr_location) < range.second)) {
          matches->append(tid);
        }
      }
      break;
    case 4:
      for (tuple_id tid = 0;
           tid < *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid, attr_location += tuple_length_bytes_) {
        if (range.first <= (*reinterpret_cast<const uint32_t*>(attr_location))
            && (*reinterpret_cast<const uint32_t*>(attr_location) < range.second)) {
          matches->append(tid);
        }
      }
      break;
    default:
      FATAL_ERROR("Unexpected byte-length (not 1, 2, or 4) for compressed "
                  "attribute ID " << attr_id
                  << " in CompressedPackedRowStoreTupleStorageSubBlock::getCodesInRange()");
  }
  return matches;
}

void CompressedPackedRowStoreTupleStorageSubBlock::initialize() {
  tuple_storage_ = initializeCommon();

  tuple_length_bytes_ = 0;
  attribute_offsets_.resize(relation_.getMaxAttributeId() + 1, 0);

  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    attribute_offsets_[attr_it->getID()] = tuple_length_bytes_;
    tuple_length_bytes_ += compression_info_.attribute_size(attr_it->getID());
  }
}

template <template <typename T> class comparison_functor>
TupleIdSequence* CompressedPackedRowStoreTupleStorageSubBlock::getCodesSatisfyingComparison(
    const attribute_id attr_id,
    const std::uint32_t code) const {
  comparison_functor<uint32_t> comp;
  TupleIdSequence *matches = new TupleIdSequence();
  const char *attr_location = static_cast<const char*>(tuple_storage_)
                              + attribute_offsets_[attr_id];
  switch (compression_info_.attribute_size(attr_id)) {
    case 1:
      for (tuple_id tid = 0;
           tid < *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid, attr_location += tuple_length_bytes_) {
        if (comp(code, *reinterpret_cast<const uint8_t*>(attr_location))) {
          matches->append(tid);
        }
      }
      break;
    case 2:
      for (tuple_id tid = 0;
           tid < *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid, attr_location += tuple_length_bytes_) {
        if (comp(code, *reinterpret_cast<const uint16_t*>(attr_location))) {
          matches->append(tid);
        }
      }
      break;
    case 4:
      for (tuple_id tid = 0;
           tid < *static_cast<const tuple_id*>(sub_block_memory_);
           ++tid, attr_location += tuple_length_bytes_) {
        if (comp(code, *reinterpret_cast<const uint32_t*>(attr_location))) {
          matches->append(tid);
        }
      }
      break;
    default:
      FATAL_ERROR("Unexpected byte-length (not 1, 2, or 4) for compressed "
                  "attribute ID " << attr_id
                  << " in CompressedPackedRowStoreTupleStorageSubBlock::getCodesSatisfyingComparison()");
  }
  return matches;
}

}  // namespace quickstep
