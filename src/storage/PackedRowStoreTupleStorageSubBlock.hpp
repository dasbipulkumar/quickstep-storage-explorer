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

#ifndef QUICKSTEP_STORAGE_PACKED_ROW_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_PACKED_ROW_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_

#include <vector>

#include "storage/TupleStorageSubBlock.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class TupleStorageSubBlockDescription;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief An implementation of TupleStorageSubBlock as a packed row-store (i.e.
 *        an array of fixed-length values with no holes).
 * @warning This implementation does NOT support variable-length or nullable
 *          attributes. It is an error to attempt to construct a
 *          PackedRowStoreTupleStorageSubBlock for a relation with any
 *          variable-length or nullable attributes.
 **/
class PackedRowStoreTupleStorageSubBlock: public TupleStorageSubBlock {
 public:
  PackedRowStoreTupleStorageSubBlock(const CatalogRelation &relation,
                                     const TupleStorageSubBlockDescription &description,
                                     const bool new_block,
                                     void *sub_block_memory,
                                     const std::size_t sub_block_memory_size);

  ~PackedRowStoreTupleStorageSubBlock() {
  }

  /**
   * @brief Determine whether a TupleStorageSubBlockDescription is valid for
   *        this type of TupleStorageSubBlock.
   *
   * @param relation The relation a tuple store described by description would
   *        belong to.
   * @param description A description of the parameters for this type of
   *        TupleStorageSubBlock, which will be checked for validity.
   * @return Whether description is well-formed and valid for this type of
   *         TupleStorageSubBlock belonging to relation (i.e. whether a
   *         TupleStorageSubBlock of this type, belonging to relation, can be
   *         constructed according to description).
   **/
  static bool DescriptionIsValid(const CatalogRelation &relation,
                                 const TupleStorageSubBlockDescription &description);

  /**
   * @brief Estimate the average number of bytes (including any applicable
   *        overhead) used to store a single tuple in this type of
   *        TupleStorageSubBlock. Used by StorageBlockLayout::finalize() to
   *        divide block memory amongst sub-blocks.
   * @warning description must be valid. DescriptionIsValid() should be called
   *          first if necessary.
   *
   * @param relation The relation tuples belong to.
   * @param description A description of the parameters for this type of
   *        TupleStorageSubBlock.
   * @return The average/ammortized number of bytes used to store a single
   *         tuple of relation in a TupleStorageSubBlock of this type described
   *         by description.
   **/
  static std::size_t EstimateBytesPerTuple(const CatalogRelation &relation,
                                           const TupleStorageSubBlockDescription &description);

  bool supportsUntypedGetAttributeValue(const attribute_id attr) const {
    return true;
  }

  bool supportsAdHocInsert() const {
    return true;
  }

  bool adHocInsertIsEfficient() const {
    return true;
  }

  TupleStorageSubBlockType getTupleStorageSubBlockType() const {
    return kPackedRowStore;
  }

  bool isEmpty() const {
    return (getHeaderPtr()->num_tuples == 0);
  }

  bool isPacked() const {
    return true;
  }

  tuple_id getMaxTupleID() const {
    return getHeaderPtr()->num_tuples - 1;
  }

  bool hasTupleWithID(const tuple_id tuple) const {
    return ((tuple >=0) && (tuple < getHeaderPtr()->num_tuples));
  }

  InsertResult insertTuple(const Tuple &tuple, const AllowedTypeConversion atc);

  inline bool insertTupleInBatch(const Tuple &tuple, const AllowedTypeConversion atc) {
    const InsertResult result = insertTuple(tuple, atc);
    return (result.inserted_id >= 0);
  }

  const void* getAttributeValue(const tuple_id tuple, const attribute_id attr) const;
  TypeInstance* getAttributeValueTyped(const tuple_id tuple, const attribute_id attr) const;

  bool deleteTuple(const tuple_id tuple);

  void rebuild() {
  }

 private:
  struct PackedRowStoreHeader {
    tuple_id num_tuples;
  };

  PackedRowStoreHeader* getHeaderPtr();
  const PackedRowStoreHeader* getHeaderPtr() const;

  bool hasSpaceToInsert(const tuple_id num_tuples) const;

  DISALLOW_COPY_AND_ASSIGN(PackedRowStoreTupleStorageSubBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_PACKED_ROW_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_
