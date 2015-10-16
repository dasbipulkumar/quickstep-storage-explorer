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

#ifndef QUICKSTEP_STORAGE_BASIC_COLUMN_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_BASIC_COLUMN_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_

#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/Comparison.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

class TupleStorageSubBlockDescription;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief An implementation of TupleStorageSubBlock as a simple column store
 *        with a single sort column and no compression or holes.
 * @warning This implementation does NOT support variable-length or nullable
 *          attributes. It is an error to attempt to construct a
 *          BasicColumnStoreTupleStorageSubBlock for a relation with any
 *          variable-length or nullable attributes.
 **/
class BasicColumnStoreTupleStorageSubBlock : public TupleStorageSubBlock {
 public:
  BasicColumnStoreTupleStorageSubBlock(const CatalogRelation &relation,
                                       const TupleStorageSubBlockDescription &description,
                                       const bool new_block,
                                       void *sub_block_memory,
                                       const std::size_t sub_block_memory_size);

  ~BasicColumnStoreTupleStorageSubBlock() {
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
    return false;
  }

  TupleStorageSubBlockType getTupleStorageSubBlockType() const {
    return kBasicColumnStore;
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

  bool insertTupleInBatch(const Tuple &tuple, const AllowedTypeConversion atc);

  const void* getAttributeValue(const tuple_id tuple, const attribute_id attr) const;
  TypeInstance* getAttributeValueTyped(const tuple_id tuple, const attribute_id attr) const;

  bool deleteTuple(const tuple_id tuple);

  // This override can quickly evaluate comparisons between the sort column
  // and a literal value.
  TupleIdSequence* getMatchesForPredicate(const Predicate *predicate) const;

  void rebuild() {
    if (!sorted_) {
      rebuildInternal();
    }
  }

 private:
  struct BasicColumnStoreHeader {
    tuple_id num_tuples;
  };

  BasicColumnStoreHeader* getHeaderPtr();
  const BasicColumnStoreHeader* getHeaderPtr() const;

  bool hasSpaceToInsert(const tuple_id num_tuples) const {
    return (num_tuples <= max_tuples_ - getHeaderPtr()->num_tuples);
  }

  // Copy attribute values from 'tuple' into the appropriate column stripes
  // at the offset specified by 'position'. If 'position' is not at the current
  // end of tuples in this block, subsequent tuples are shifted back to make
  // room for the new tuple.
  void insertTupleAtPosition(const Tuple &tuple,
                             const AllowedTypeConversion atc,
                             const tuple_id position);

  // Move 'num_tuples' values in each column from 'src_tuple' to
  // 'dest_position'.
  void shiftTuples(const tuple_id dest_position,
                   const tuple_id src_tuple,
                   const tuple_id num_tuples);

  // Sort all columns according to ascending order of values in the sort
  // column. Returns true if any reordering occured.
  bool rebuildInternal();

  tuple_id max_tuples_;
  bool sorted_;

  attribute_id sort_column_id_;
  ScopedPtr<UncheckedComparator> sort_column_comparator_;

  std::vector<void*> column_stripes_;

  DISALLOW_COPY_AND_ASSIGN(BasicColumnStoreTupleStorageSubBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_BASIC_COLUMN_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_
