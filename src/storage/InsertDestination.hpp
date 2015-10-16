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

#ifndef QUICKSTEP_STORAGE_INSERT_DESTINATION_HPP_
#define QUICKSTEP_STORAGE_INSERT_DESTINATION_HPP_

#include <vector>

#include "storage/StorageBlockInfo.hpp"
#include "threading/Mutex.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogRelation;

class StorageBlock;
class StorageBlockLayout;
class StorageManager;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief Base class for different strategies for getting blocks to insert
 *        tuples into.
 **/
class InsertDestination {
 public:
  /**
   * @brief Constructor.
   *
   * @param storage_manager The StorageManager to use.
   * @param relation The relation to insert tuples into.
   * @param layout The layout to use for any newly-created blocks. If NULL,
   *        defaults to relation's default layout.
   **/
  InsertDestination(StorageManager *storage_manager,
                    CatalogRelation *relation,
                    const StorageBlockLayout *layout);

  /**
   * @brief Virtual destructor.
   **/
  virtual ~InsertDestination() {
  }

  /**
   * @brief Get the relation which tuples are inserted into.
   *
   * @return The relation which tuples are inserted into.
   **/
  const CatalogRelation& getRelation() const {
    return *relation_;
  }

  /**
   * @brief Get a block to use for insertion.
   *
   * @return A block to use for inserting tuples.
   **/
  virtual StorageBlock* getBlockForInsertion() = 0;

  /**
   * @brief Release a block after done using it for insertion.
   * @note This should ALWAYS be called when done inserting into a block.
   *
   * @param block A block, originally supplied by getBlockForInsertion(),
   *        which the client is finished using.
   * @param full If true, the client ran out of space when trying to insert
   *        into block. If false, all inserts were successful.
   **/
  virtual void returnBlock(StorageBlock *block, const bool full) = 0;

  /**
   * @brief Get the set of blocks that were used by clients of this
   *        InsertDestination for insertion.
   * @warning Should only be called AFTER this InsertDestination will no longer
   *          be used, and all blocks have been returned to it via
   *          returnBlock().
   *
   * @return A reference to a vector of block_ids of blocks that were used for
   *         insertion.
   **/
  const std::vector<block_id>& getTouchedBlocks() {
    MutexLock lock(mutex_);
    return getTouchedBlocksInternal();
  }

 protected:
  StorageBlock* createNewBlock();

  virtual const std::vector<block_id>& getTouchedBlocksInternal() = 0;

  StorageManager *storage_manager_;
  CatalogRelation *relation_;
  const StorageBlockLayout *layout_;

  // NOTE(chasseur): If contention is high, finer-grained locking of internal
  // data members in subclasses is possible.
  Mutex mutex_;

 private:
  DISALLOW_COPY_AND_ASSIGN(InsertDestination);
};

/**
 * @brief Implementation of InsertDestination that always creates new blocks,
 *        leaving some blocks potentially very underfull.
 **/
class AlwaysCreateBlockInsertDestination : public InsertDestination {
 public:
  AlwaysCreateBlockInsertDestination(StorageManager *storage_manager,
                                     CatalogRelation *relation,
                                     const StorageBlockLayout *layout)
      : InsertDestination(storage_manager, relation, layout) {
  }

  ~AlwaysCreateBlockInsertDestination() {
  }

  StorageBlock* getBlockForInsertion() {
    MutexLock lock(mutex_);
    return createNewBlock();
  }

  void returnBlock(StorageBlock *block, const bool full);

 protected:
  const std::vector<block_id>& getTouchedBlocksInternal() {
    return returned_block_ids_;
  }

 private:
  std::vector<block_id> returned_block_ids_;

  DISALLOW_COPY_AND_ASSIGN(AlwaysCreateBlockInsertDestination);
};

/**
 * @brief Implementation of InsertDestination that keeps a pool of
 *        partially-full blocks. Creates new blocks as necessary when
 *        getBlockForInsertion() is called and there are no partially-full
 *        blocks from the pool which are not "checked out" by workers.
 **/
class BlockPoolInsertDestination : public InsertDestination {
 public:
  BlockPoolInsertDestination(StorageManager *storage_manager,
                             CatalogRelation *relation,
                             const StorageBlockLayout *layout)
      : InsertDestination(storage_manager, relation, layout) {
  }

  ~BlockPoolInsertDestination() {
  }

  /**
   * @brief Manually add a block to the pool.
   * @warning Call only ONCE for each block to add to the pool.
   * @warning Do not use in combination with addAllBlocksFromRelation().
   *
   * @param bid The ID of the block to add to the pool.
   **/
  void addBlockToPool(const block_id bid) {
    MutexLock lock(mutex_);
    available_block_ids_.push_back(bid);
  }

  // TODO(chasseur): Once block fill statistics are available, replace this
  // with something smarter.
  /**
   * @brief Fill block pool with all the blocks belonging to the relation.
   * @warning Call only ONCE, before using getBlockForInsertion().
   * @warning Do not use in combination with addBlockToPool().
   **/
  void addAllBlocksFromRelation();

  StorageBlock* getBlockForInsertion();

  void returnBlock(StorageBlock *block, const bool full);

 protected:
  const std::vector<block_id>& getTouchedBlocksInternal();

 private:
  std::vector<block_id> available_block_ids_;
  std::vector<block_id> done_block_ids_;

  DISALLOW_COPY_AND_ASSIGN(BlockPoolInsertDestination);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_INSERT_DESTINATION_HPP_
