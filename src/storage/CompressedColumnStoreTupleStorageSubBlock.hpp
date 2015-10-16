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

#ifndef QUICKSTEP_STORAGE_COMPRESSED_COLUMN_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_COMPRESSED_COLUMN_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_

#include <cstddef>
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/CompressedTupleStorageSubBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief An implementation of TupleStorageSubBlock as a column store with a
 *        single sort column, optional column compression (dictionary or
 *        truncation), and no holes.
 * @warning This implementation does NOT support nullable attributes. It is an
 *          error to attempt to construct a
 *          CompressedColumnStoreTupleStorageSubBlock for a relation with any
 *          nullable attributes.
 * @warning This implementation does support variable-length attributes, but
 *          they must all be compressed (specified with compressed_attribute_id
 *          in the TupleStorageSubBlockDescription).
 **/
class CompressedColumnStoreTupleStorageSubBlock : public CompressedTupleStorageSubBlock {
 public:
  CompressedColumnStoreTupleStorageSubBlock(
      const CatalogRelation &relation,
      const TupleStorageSubBlockDescription &description,
      const bool new_block,
      void *sub_block_memory,
      const std::size_t sub_block_memory_size);

  ~CompressedColumnStoreTupleStorageSubBlock() {
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

  TupleStorageSubBlockType getTupleStorageSubBlockType() const {
    return kCompressedColumnStore;
  }

  bool deleteTuple(const tuple_id tuple);

  // This override can quickly evaluate comparisons between the sort column
  // and a literal value.
  TupleIdSequence* getMatchesForPredicate(const Predicate *predicate) const;

  void rebuild();

  std::uint32_t compressedGetCode(const tuple_id tid,
                                  const attribute_id attr_id) const;

 protected:
  const void* getAttributePtr(const tuple_id tid,
                              const attribute_id attr_id) const {
    return static_cast<const char*>(column_stripes_[attr_id])
           + tid * compression_info_.attribute_size(attr_id);
  }

  TupleIdSequence* getEqualCodes(const attribute_id attr_id,
                                 const std::uint32_t code) const;

  TupleIdSequence* getNotEqualCodes(const attribute_id attr_id,
                                    const std::uint32_t code) const;

  TupleIdSequence* getLessCodes(const attribute_id attr_id,
                                const std::uint32_t code) const;

  TupleIdSequence* getGreaterOrEqualCodes(const attribute_id attr_id,
                                          const std::uint32_t code) const;

  TupleIdSequence* getCodesInRange(const attribute_id attr_id,
                                   const std::pair<std::uint32_t, std::uint32_t> range) const;

 private:
  // Initialize this sub-block's runtime data structures after the physical
  // block has been built.
  void initialize();

  // Move 'num_tuples' values in each column from 'src_tuple' to
  // 'dest_position'.
  void shiftTuples(const tuple_id dest_position,
                   const tuple_id src_tuple,
                   const tuple_id num_tuples);

  std::pair<tuple_id, tuple_id> getCompressedSortColumnRange(
      const std::pair<std::uint32_t, std::uint32_t> code_range) const;

  // Note: order of application is
  // comparison_functor(literal_code, attribute_code).
  template <template <typename T> class comparison_functor>
  TupleIdSequence* getCodesSatisfyingComparison(const attribute_id attr_id,
                                                const std::uint32_t code) const;

  attribute_id sort_column_id_;

  std::vector<void*> column_stripes_;

  DISALLOW_COPY_AND_ASSIGN(CompressedColumnStoreTupleStorageSubBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_COMPRESSED_COLUMN_STORE_TUPLE_STORAGE_SUB_BLOCK_HPP_
