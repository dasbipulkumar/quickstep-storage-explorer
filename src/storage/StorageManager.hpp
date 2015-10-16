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

#ifndef QUICKSTEP_STORAGE_STORAGE_MANAGER_HPP_
#define QUICKSTEP_STORAGE_STORAGE_MANAGER_HPP_

#include <vector>

#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageConstants.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogRelation;
class StorageBlock;
class StorageBlockLayout;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief A class which manages block storage in memory and is responsible for
 *        creating, saving, and loading StorageBlock instances.
 **/
class StorageManager {
 public:
  /**
   * @brief Constructor.
   **/
  StorageManager()
      : block_index_(0) {
  }

  /**
   * @brief Destructor which also destroys all managed blocks.
   **/
  ~StorageManager();

  /**
   * @brief Determine the size of the memory pool managed by this
   *        StorageManager.
   *
   * @return The amount of allocated memory managed by this StorageManager in
   *         bytes.
   **/
  std::size_t getMemorySize() const {
    return kSlotSizeBytes * kAllocationChunkSizeSlots * alloc_chunks_.size();
  }

  /**
   * @brief Create a new empty block.
   *
   * @param relation The relation which the new block will belong to (you must
   *        also call addBlock() on the relation).
   * @param layout The StorageBlockLayout to use for the new block. If NULL,
   *               the default layout from relation will be used.
   * @return The id of the newly-created block.
   **/
  block_id createBlock(const CatalogRelation &relation, const StorageBlockLayout *layout);

  /**
   * @brief Check whether a StorageBlock is loaded into memory.
   *
   * @param block The id of the block.
   * @return Whether the block with the specified id is in memory.
   **/
  bool blockIsLoaded(const block_id block) const;

  /**
   * @brief Evict a block from memory.
   * @note The block is NOT automatically saved, so call saveBlock() first if
   *       necessary.
   *
   * @param block The id of the block to evict.
   **/
  void evictBlock(const block_id block);

  /**
   * @brief Get an in-memory block.
   *
   * @param block The id of the block to get.
   * @return The block with the given id.
   **/
  const StorageBlock& getBlock(const block_id block) const {
    return *getBlockMutable(block);
  }

  /**
   * @brief Get a mutable pointer to an in-memory block.
   *
   * @param block The id of the block to get.
   * @return The block with the given id.
   **/
  StorageBlock* getBlockMutable(const block_id block) const;

 private:
  struct BlockHandle {
    std::size_t slot_index_low, slot_index_high;
    StorageBlock *block;
  };

  void* getSlotAddress(std::size_t slot_index) const;

  std::size_t getSlots(std::size_t num_slots);
  void allocChunk();

  block_id block_index_;

  CompatUnorderedMap<block_id, BlockHandle>::unordered_map blocks_;

  std::vector<bool> free_bitmap_;
  std::vector<void*> alloc_chunks_;

  DISALLOW_COPY_AND_ASSIGN(StorageManager);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_STORAGE_MANAGER_HPP_
