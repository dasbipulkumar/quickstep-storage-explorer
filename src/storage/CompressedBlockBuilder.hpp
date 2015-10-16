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

#ifndef QUICKSTEP_STORAGE_COMPRESSED_BLOCK_BUILDER_HPP_
#define QUICKSTEP_STORAGE_COMPRESSED_BLOCK_BUILDER_HPP_

#include <cstddef>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "types/CompressionDictionaryBuilder.hpp"
#include "types/Tuple.hpp"
#include "types/TypeInstance.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrMap.hpp"
#include "utility/PtrVector.hpp"

namespace quickstep {

class CatalogRelation;
class CompressionDictionary;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief A helper class which temporarily stores tuples during batch-insertion
 *        and builds the physical contents of either a
 *        CompressedPackedRowStoreTupleStorageSubBlock or a
 *        CompressedColumnStoreTupleStorageSubBlock, automatically selecting
 *        most efficient coding for each compressed column (dictionary coding,
 *        truncation, or none).
 **/
class CompressedBlockBuilder {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation which the compressed block being built
   *        belongs to.
   * @param description The description for the TupleStorageSubBlock being
   *        built. The description must be valid and specify either
   *        COMPRESSED_PACKED_ROW_STORE or COMPRESSED_COLUMN_STORE as the
   *        sub_block_type.
   * @param block_size The size, in bytes of the TupleStorageSubBlock being
   *        built.
   **/
  CompressedBlockBuilder(const CatalogRelation &relation,
                         const TupleStorageSubBlockDescription &description,
                         const std::size_t block_size);

  /**
   * @brief Destructor. A CompressedBlockBuilder should be deleted after the
   *        block is done being built to free any temporary memory.
   **/
  ~CompressedBlockBuilder() {
  }

  /**
   * @brief Check if an attribute may be compressed in the TupleStorageSubBlock
   *        ultimately build by this CompressedBlockBuilder.
   * @note Even if this method returns true, the attribute specified might
   *       still be uncompressed when the block is built if compression fails.
   *
   * @param attr_id The ID of the attribute to check for possible compression.
   * @return true if the attribute specified by attr_id might be compressed
   *         in a TupleStorageSubBlock built by this CompressedBlockBuilder,
   *         false if no compression will be attempted.
   **/
  bool attributeMayBeCompressed(const attribute_id attr_id) const {
    return dictionary_builders_.find(attr_id) != dictionary_builders_.end();
  }

  /**
   * @brief Add a Tuple to the block being built.
   *
   * @param tuple The Tuple to add. It will be copied.
   * @param coerce_types True if some of the types of the values in tuple may
   *        need to be coerced to match the types of attributes in the
   *        relation. False if the caller can guarantee that all value types
   *        exactly match and no coercion is needed.
   *
   * @return True if the tuple was successfully added, false if attempting to
   *         add the tuple failed because there would not be enough space to
   *         store it in the block being constructed. Typically,
   *         buildCompressedPackedRowStoreTupleStorageSubBlock() or
   *         buildCompressedColumnStoreTupleStorageSubBlock() would be called
   *         to actually construct the physical block after the first time this
   *         method returns false.
   **/
  bool addTuple(const Tuple &tuple, const bool coerce_types);

  /**
   * @brief Get the number of Tuples held by this CompressedBlockBuilder for
   *        eventual inclusion in the compressed block being built.
   *
   * @return The number of tuples in this CompressedBlockBuilder.
   **/
  inline std::size_t numTuples() const {
    return tuples_.size();
  }

  /**
   * @brief Get the bare-minimum number of bytes needed to store just the
   *        metadata of a compressed block under construction, without any
   *        actual tuples.
   *
   * @return The minimum required memory for any TupleStorageSubBlock built
   *         by this CompressedBlockBuilder.
   **/
  inline std::size_t getMinimumRequiredBlockSize() const {
    return sizeof(tuple_id) + sizeof(int) + compression_info_.ByteSize();
  }

  /**
   * @brief Build a physical CompressedPackedRowStoreTupleStorageSubBlock with
   *        the tuples in this CompressedStorageSubBlock, automatically using
   *        the most-efficient compression method for each compressed column.
   *
   * @param sub_block_memory The physical memory location where the block will
   *        be stored. Must be at least as large as the block_size given to the
   *        constructor.
   **/
  void buildCompressedPackedRowStoreTupleStorageSubBlock(void *sub_block_memory);

  /**
   * @brief Build a physical CompressedColumnStoreTupleStorageSubBlock with
   *        the tuples in this CompressedStorageSubBlock, automatically using
   *        the most-efficient compression method for each compressed column.
   *
   * @param sub_block_memory The physical memory location where the block will
   *        be stored. Must be at least as large as the block_size given to the
   *        constructor.
   **/
  void buildCompressedColumnStoreTupleStorageSubBlock(void *sub_block_memory);

 private:
  std::size_t computeRequiredStorage(const std::size_t num_tuples) const;
  std::size_t computeTruncatedByteLengthForAttribute(const attribute_id attr_id) const;

  void rollbackLastInsert(
      const std::vector<CompressionDictionaryBuilder*> &modified_dictionaries,
      const CompatUnorderedMap<attribute_id, const TypeInstance*>::unordered_map &previous_maximum_integers);

  std::size_t buildTupleStorageSubBlockHeader(void *sub_block_memory);
  void buildDictionaryMap(const void *sub_block_memory,
                          PtrMap<attribute_id, CompressionDictionary> *dictionary_map) const;

  void buildDictionaryCompressedColumnStripe(const attribute_id attr_id,
                                             const CompressionDictionary &dictionary,
                                             void *stripe_location) const;
  void buildTruncationCompressedColumnStripe(const attribute_id attr_id,
                                             void *stripe_location) const;
  void buildUncompressedColumnStripe(const attribute_id attr_id,
                                     void *stripe_location) const;

  const CatalogRelation &relation_;
  const std::size_t block_size_;
  attribute_id sort_attribute_id_;  // Only used for CompressedColumnStore.

  PtrVector<Tuple> tuples_;

  CompressedBlockInfo compression_info_;
  PtrMap<attribute_id, CompressionDictionaryBuilder> dictionary_builders_;
  CompatUnorderedMap<attribute_id, const TypeInstance*>::unordered_map maximum_integers_;

  DISALLOW_COPY_AND_ASSIGN(CompressedBlockBuilder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_COMPRESSED_BLOCK_BUILDER_HPP_
