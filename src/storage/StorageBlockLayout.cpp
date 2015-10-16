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

#include "storage/StorageBlockLayout.hpp"

#include <cstring>
#include <string>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "storage/BasicColumnStoreTupleStorageSubBlock.hpp"
#include "storage/CompressedColumnStoreTupleStorageSubBlock.hpp"
#include "storage/CompressedPackedRowStoreTupleStorageSubBlock.hpp"
#include "storage/CSBTreeIndexSubBlock.hpp"
#include "storage/PackedRowStoreTupleStorageSubBlock.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageConstants.hpp"
#include "storage/StorageErrors.hpp"
#include "utility/Macros.hpp"

using std::size_t;
using std::string;
using std::strlen;
using std::vector;

namespace quickstep {

void StorageBlockLayout::finalize() {
  if (!DescriptionIsValid(relation_, layout_description_)) {
    FATAL_ERROR("Called StorageBlockLayout::finalize() with incomplete or invalid layout.");
  }

  // Reset the header and copy the layout from this StorageBlockLayout.
  block_header_.Clear();
  block_header_.mutable_layout()->CopyFrom(layout_description_);

  // Temporarily set all sub-block sizes to zero, and set all indices as
  // consistent.
  block_header_.set_tuple_store_size(0);
  for (int index_num = 0;
       index_num < layout_description_.index_description_size();
       ++index_num) {
    block_header_.add_index_size(0);
    block_header_.add_index_consistent(true);
  }

  DEBUG_ASSERT(block_header_.IsInitialized());

  size_t header_size = getBlockHeaderSize();
  if (header_size > layout_description_.num_slots() * kSlotSizeBytes) {
    throw BlockMemoryTooSmall("StorageBlockLayout", layout_description_.num_slots() * kSlotSizeBytes);
  }

  // Assign block space to sub-blocks in proportion to the estimated number of
  // bytes needed for each tuple in each sub-block.
  size_t total_size_factor = 0;

  // Bytes for tuple store.
  size_t tuple_store_size_factor = 0;
  const TupleStorageSubBlockDescription &tuple_store_description
      = layout_description_.tuple_store_description();
  switch (tuple_store_description.sub_block_type()) {
    case TupleStorageSubBlockDescription::PACKED_ROW_STORE:
      tuple_store_size_factor
          = PackedRowStoreTupleStorageSubBlock::EstimateBytesPerTuple(relation_,
                                                                      tuple_store_description);
      break;
    case TupleStorageSubBlockDescription::BASIC_COLUMN_STORE:
      tuple_store_size_factor
          = BasicColumnStoreTupleStorageSubBlock::EstimateBytesPerTuple(relation_,
                                                                        tuple_store_description);
      break;
    case TupleStorageSubBlockDescription::COMPRESSED_PACKED_ROW_STORE:
      tuple_store_size_factor
          = CompressedPackedRowStoreTupleStorageSubBlock::EstimateBytesPerTuple(relation_,
                                                                                tuple_store_description);
      break;
    case TupleStorageSubBlockDescription::COMPRESSED_COLUMN_STORE:
      tuple_store_size_factor
          = CompressedColumnStoreTupleStorageSubBlock::EstimateBytesPerTuple(relation_,
                                                                             tuple_store_description);
      break;
    default:
      FATAL_ERROR("Unknown TupleStorageSubBlockType encountered in StorageBlockLayout::finalize()");
  }
  total_size_factor += tuple_store_size_factor;

  // Bytes for each index.
  vector<size_t> index_size_factors;
  index_size_factors.reserve(layout_description_.index_description_size());
  for (int index_num = 0;
       index_num < layout_description_.index_description_size();
       ++index_num) {
    size_t index_size_factor = 0;
    const IndexSubBlockDescription &index_description = layout_description_.index_description(index_num);
    switch (index_description.sub_block_type()) {
      case IndexSubBlockDescription::CSB_TREE:
        index_size_factor = CSBTreeIndexSubBlock::EstimateBytesPerTuple(relation_, index_description);
        break;
      default:
        FATAL_ERROR("Unknown IndexSubBlockType encountered in StorageBlockLayout::finalize()");
    }
    index_size_factors.push_back(index_size_factor);
    total_size_factor += index_size_factor;
  }

  size_t allocated_sub_block_space = 0;
  size_t sub_block_space = (layout_description_.num_slots() * kSlotSizeBytes) - header_size;

  for (int index_num = 0;
       index_num < layout_description_.index_description_size();
       ++index_num) {
    size_t index_size = (sub_block_space * index_size_factors[index_num]) / total_size_factor;
    block_header_.set_index_size(index_num, index_size);
    allocated_sub_block_space += index_size;
  }

  block_header_.set_tuple_store_size(sub_block_space - allocated_sub_block_space);

  DEBUG_ASSERT(block_header_.IsInitialized());
  DEBUG_ASSERT(header_size == getBlockHeaderSize());
}

void StorageBlockLayout::copyHeaderTo(void *dest) const {
  DEBUG_ASSERT(DescriptionIsValid(relation_, layout_description_));
  DEBUG_ASSERT(block_header_.IsInitialized());

  *static_cast<int*>(dest) = block_header_.ByteSize();
  if (!block_header_.SerializeToArray(static_cast<char*>(dest) + sizeof(int),
                                      block_header_.ByteSize())) {
    FATAL_ERROR("Failed to do binary serialization of StorageBlockHeader in StorageBlockLayout::copyHeaderTo()");
  }
}

StorageBlockLayout* StorageBlockLayout::GenerateDefaultLayout(const CatalogRelation &relation) {
  StorageBlockLayout *layout = new StorageBlockLayout(relation);

  StorageBlockLayoutDescription *description = layout->getDescriptionMutable();
  description->set_num_slots(1);

  TupleStorageSubBlockDescription *tuple_store_description = description->mutable_tuple_store_description();
  tuple_store_description->set_sub_block_type(TupleStorageSubBlockDescription::PACKED_ROW_STORE);

  layout->finalize();
  return layout;
}

bool StorageBlockLayout::DescriptionIsValid(const CatalogRelation &relation,
                                            const StorageBlockLayoutDescription &description) {
  // Check that layout is fully initialized.
  if (!description.IsInitialized()) {
    return false;
  }

  // Check that the number of slots is positive but small enough to actually
  // be allocated contiguously by the StorageManager.
  if ((description.num_slots() == 0)
      || (description.num_slots() > kAllocationChunkSizeSlots)) {
    return false;
  }

  // Check that the tuple_store_description is valid.
  const TupleStorageSubBlockDescription &tuple_store_description = description.tuple_store_description();
  if (!tuple_store_description.IsInitialized()) {
    return false;
  }
  switch (tuple_store_description.sub_block_type()) {
    case TupleStorageSubBlockDescription::PACKED_ROW_STORE:
      if (!PackedRowStoreTupleStorageSubBlock::DescriptionIsValid(relation, tuple_store_description)) {
        return false;
      }
      break;
    case TupleStorageSubBlockDescription::BASIC_COLUMN_STORE:
      if (!BasicColumnStoreTupleStorageSubBlock::DescriptionIsValid(relation, tuple_store_description)) {
        return false;
      }
      break;
    case TupleStorageSubBlockDescription::COMPRESSED_PACKED_ROW_STORE:
      if (!CompressedPackedRowStoreTupleStorageSubBlock::DescriptionIsValid(relation, tuple_store_description)) {
        return false;
      }
      break;
    case TupleStorageSubBlockDescription::COMPRESSED_COLUMN_STORE:
      if (!CompressedColumnStoreTupleStorageSubBlock::DescriptionIsValid(relation, tuple_store_description)) {
        return false;
      }
      break;
    default:
      return false;
  }

  // Check that each index_description is valid.
  for (int index_description_num = 0;
       index_description_num < description.index_description_size();
       ++index_description_num) {
    const IndexSubBlockDescription &index_description = description.index_description(index_description_num);
    if (!index_description.IsInitialized()) {
      return false;
    }
    switch (index_description.sub_block_type()) {
      case IndexSubBlockDescription::CSB_TREE:
        if (!CSBTreeIndexSubBlock::DescriptionIsValid(relation, index_description)) {
          return false;
        }
        break;
      default:
        return false;
    }
  }

  return true;
}

}  // namespace quickstep
