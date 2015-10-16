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

#include "storage/PackedRowStoreTupleStorageSubBlock.hpp"

#include <cstddef>
#include <cstring>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageErrors.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Tuple.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

using std::vector;
using std::memcpy;
using std::size_t;

namespace quickstep {

PackedRowStoreTupleStorageSubBlock::PackedRowStoreTupleStorageSubBlock(
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
  if (!DescriptionIsValid(relation_, description_)) {
    FATAL_ERROR("Attempted to construct a PackedRowStoreTupleStorageSubBlock from an invalid description.");
  }

  if (sub_block_memory_size < sizeof(PackedRowStoreHeader)) {
    throw BlockMemoryTooSmall("PackedRowStoreTupleStorageSubBlock", sub_block_memory_size);
  }

  if (new_block) {
    getHeaderPtr()->num_tuples = 0;
  }
}

bool PackedRowStoreTupleStorageSubBlock::DescriptionIsValid(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description) {
  // Make sure description is initialized and specifies PackedRowStore.
  if (!description.IsInitialized()) {
    return false;
  }
  if (description.sub_block_type() != TupleStorageSubBlockDescription::PACKED_ROW_STORE) {
    return false;
  }

  // Make sure relation is not variable-length and contains no nullable attributes.
  if (relation.isVariableLength()) {
    return false;
  }
  if (relation.hasNullableAttributes()) {
    return false;
  }

  return true;
}

std::size_t PackedRowStoreTupleStorageSubBlock::EstimateBytesPerTuple(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description) {
  DEBUG_ASSERT(DescriptionIsValid(relation, description));

  return relation.getFixedByteLength();
}

TupleStorageSubBlock::InsertResult PackedRowStoreTupleStorageSubBlock::insertTuple(
    const Tuple &tuple,
    const AllowedTypeConversion atc) {
#ifdef QUICKSTEP_DEBUG
  paranoidInsertTypeCheck(tuple, atc);
#endif
  if (!hasSpaceToInsert(1)) {
    return InsertResult(-1, false);
  }

  char *base_addr = static_cast<char*>(sub_block_memory_)                           // Start of the SubBlock.
                    + sizeof(PackedRowStoreHeader)                                  // Space taken by the header.
                    + getHeaderPtr()->num_tuples * relation_.getFixedByteLength();  // Existing tuples.

  Tuple::const_iterator value_it = tuple.begin();
  CatalogRelation::const_iterator attr_it = relation_.begin();

  switch (atc) {
    case kNone:
      while (value_it != tuple.end()) {
        value_it->copyInto(base_addr);
        base_addr += attr_it->getType().maximumByteLength();

        ++value_it;
        ++attr_it;
      }
      break;
    case kSafe:
    case kUnsafe:
      while (value_it != tuple.end()) {
        if (value_it->getType().equals(attr_it->getType())) {
          value_it->copyInto(base_addr);
        } else {
          ScopedPtr<TypeInstance> converted_temp(value_it->makeCoercedCopy(attr_it->getType()));
          converted_temp->copyInto(base_addr);
        }

        base_addr += attr_it->getType().maximumByteLength();

        ++value_it;
        ++attr_it;
      }
      break;
  }

  ++(getHeaderPtr()->num_tuples);

  return InsertResult(getHeaderPtr()->num_tuples - 1, false);
}

const void* PackedRowStoreTupleStorageSubBlock::getAttributeValue(const tuple_id tuple,
                                                                  const attribute_id attr) const {
  DEBUG_ASSERT(hasTupleWithID(tuple));
  DEBUG_ASSERT(relation_.hasAttributeWithId(attr));
  return static_cast<char*>(sub_block_memory_)             // SubBlock start.
         + sizeof(PackedRowStoreHeader)                    // Space taken by header.
         + (tuple * relation_.getFixedByteLength())        // Tuples prior to 'tuple'.
         + relation_.getFixedLengthAttributeOffset(attr);  // Attribute offset within tuple.
}

TypeInstance* PackedRowStoreTupleStorageSubBlock::getAttributeValueTyped(const tuple_id tuple,
                                                                         const attribute_id attr) const {
  return relation_.getAttributeById(attr).getType().makeReferenceTypeInstance(getAttributeValue(tuple, attr));
}

bool PackedRowStoreTupleStorageSubBlock::deleteTuple(const tuple_id tuple) {
  DEBUG_ASSERT(hasTupleWithID(tuple));

  PackedRowStoreHeader *header = getHeaderPtr();

  if (tuple == header->num_tuples - 1) {
    // If deleting the last tuple, simply truncate.
    --(header->num_tuples);
    return false;
  } else {
    const size_t tuple_length = relation_.getFixedByteLength();

    char *dest_addr = static_cast<char*>(sub_block_memory_)  // SubBlock start.
                      + sizeof(PackedRowStoreHeader)         // Space taken by header.
                      + (tuple * tuple_length);              // Prior tuples.
    char *src_addr = dest_addr + tuple_length;  // Start of subsequent tuples.
    const size_t copy_bytes = (header->num_tuples - tuple - 1) * tuple_length;  // Bytes in subsequent tuples.
    memmove(dest_addr, src_addr, copy_bytes);

    --(header->num_tuples);

    return true;
  }
}

PackedRowStoreTupleStorageSubBlock::PackedRowStoreHeader* PackedRowStoreTupleStorageSubBlock::getHeaderPtr() {
  return static_cast<PackedRowStoreHeader*>(sub_block_memory_);
}

const PackedRowStoreTupleStorageSubBlock::PackedRowStoreHeader*
    PackedRowStoreTupleStorageSubBlock::getHeaderPtr() const {
  return static_cast<const PackedRowStoreHeader*>(sub_block_memory_);
}

bool PackedRowStoreTupleStorageSubBlock::hasSpaceToInsert(const tuple_id num_tuples) const {
  if (sizeof(PackedRowStoreHeader) + (getHeaderPtr()->num_tuples + num_tuples) * relation_.getFixedByteLength()
      <= sub_block_memory_size_) {
    return true;
  } else {
    return false;
  }
}

}  // namespace quickstep
