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

#ifndef QUICKSTEP_STORAGE_STORAGE_BLOCK_HPP_
#define QUICKSTEP_STORAGE_STORAGE_BLOCK_HPP_

#include <string>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/IndexSubBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/TupleStorageSubBlock.hpp"
#include "storage/BloomFilterSubBlock.hpp"
#include "types/AllowedTypeConversion.hpp"
#include "types/Tuple.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrList.hpp"
#include "utility/PtrVector.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

class CatalogRelation;
class IndexSubBlock;
class InsertDestination;
class LiteralTypeInstance;
class Predicate;
class Scalar;
class StorageBlockLayout;
class StorageManager;
class Tuple;
class TupleIdSequence;

namespace storage_explorer {
class BlockBasedQueryExecutor;
}

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief Top-level storage block, which contains exactly one
 *        TupleStorageSubBlock and any number of IndexSubBlocks.
 **/
class StorageBlock {
 public:
  /**
   * @brief The return value of a call to update().
   **/
  struct UpdateResult {
    /**
     * @brief Whether this StorageBlock's IndexSubBlocks remain consistent
     *        after the call to update().
     **/
    bool indices_consistent;

    /**
     * @brief Whether some tuples were moved to relocation_destination.
     **/
    bool relocation_destination_used;

    /**
     * @brief Whether all the blocks from relocation_destination have
     *        consistent IndexSubBlocks.
     **/
    bool relocation_destination_indices_consistent;
  };

  /**
   * @brief Constructor.
   *
   * @param relation The CatalogRelation this block belongs to.
   * @param id The ID of this block.
   * @param layout The StorageBlockLayout to use for this block. This is used
   *        ONLY for a newly-created block, and is ignored if new_block is
   *        false. If unsure, just use relation.getDefaultStorageBlockLayout().
   * @param new_block Whether this is a newly-created block.
   * @param block_memory The memory slot to use for the block's contents.
   * @param block_memory_size The size of the memory slot in bytes.
   * @exception MalformedBlock new_block is false and the provided block_memory
   *            appears corrupted.
   * @exception BlockMemoryTooSmall This StorageBlock or one of its subblocks
   *            hasn't been provided enough memory to store metadata.
   **/
  StorageBlock(const CatalogRelation &relation,
               const block_id id,
               const StorageBlockLayout &layout,
               const bool new_block,
               void *block_memory,
               const std::size_t block_memory_size);

  /**
   * @brief Destructor.
   **/
  ~StorageBlock() {
  }

  /**
   * @brief Determine whether this StorageBlock supports ad-hoc insertion of
   *        individual tuples via the insertTuple() method.
   * @note If this method returns false, then tuples can only be inserted via
   *       insertTupleInBatch(), which should be followed by a call to
   *       rebuild() when a batch is fully inserted.
   * @note Even if this method returns false, it is still legal to call
   *       insertTuple(), although it will always fail to actually insert.
   *
   * @return Whether the insertTuple() can be used on this StorageBlock.
   **/
  bool supportsAdHocInsert() const {
    return ad_hoc_insert_supported_;
  }

  /**
   * @brief Determine whether inserting tuples one-at-a-time via the
   *        insertTuple() method is efficient (i.e. has reasonable time and
   *        space costs and does not require expensive reorganization of other
   *        tuples or rebuilding indices).
   *
   * @return Whether insertTuple() is efficient for this StorageBlock.
   **/
  bool adHocInsertIsEfficient() const {
    return ad_hoc_insert_efficient_;
  }

  /**
   * @brief Get this block's block_id.
   *
   * @return This block's ID.
   **/
  block_id getID() const {
    return id_;
  }

  /**
   * @brief Check whether this block is dirty (whether it has been changed
   *        since being written to disk).
   *
   * @return Whether the block is dirty.
   **/
  bool isDirty() const {
    return dirty_;
  }

  /**
   * @brief Clear the dirty bit for this block, marking it as clean.
   **/
  void markClean() {
    dirty_ = false;
  }

  /**
   * @brief Determine whether the IndexSubBlocks in this StorageBlock (if any)
   *        are up-to-date and consistent with the complete contents of the
   *        TupleStorageSubBlock.
   * @note Indices are usually kept consistent, except during batch-insertion
   *       using calls to the insertTupleInBatch() method, in which case
   *       indices are inconsistent until rebuild() is called.
   * @warning If insufficient space is allocated for IndexSubBlocks, it is
   *          possible for indices to become inconsistent in a block which is
   *          used in a destination for select(), selectSimple(), or update().
   *          It is also possible for a StorageBlock which update() is called
   *          on to have its indices become inconsistent in some unusual edge
   *          cases. In all such cases, the TupleStorageSubBlock will remain
   *          consistent and accessible.
   *
   * @return Whether all IndexSubBlocks in this StorageBlock are consistent.
   **/
  bool indicesAreConsistent() const {
    return all_indices_consistent_;
  }

  /**
   * @brief Get the CatalogRelation this block belongs to.
   *
   * @return The relation this block belongs to.
   **/
  const CatalogRelation& getRelation() const {
    return relation_;
  }

  /**
   * @brief Get this block's TupleStorageSubBlock.
   *
   * @return This block's TupleStorageSubBlock.
   **/
  const TupleStorageSubBlock& getTupleStorageSubBlock() const {
    return *tuple_store_;
  }

  /**
   * @brief Insert a single tuple into this block.
   *
   * @param tuple The tuple to insert.
   * @param atc What level of type conversion to perform to make the values in
   *        tuple compatible with the attributes of this StorageBlock's
   *        relation, if necessary. Safety checks will only actually happen in
   *        debug builds, and in general should happen elsewhere (i.e in the
   *        query optimizer).
   * @return true if the tuple was successfully inserted, false if insertion
   *         failed (e.g. because of not enough space).
   * @exception TupleTooLargeForBlock Even though this block was initially
   *            empty, the tuple was too large to insert. Only thrown if block
   *            is initially empty, otherwise failure to insert simply returns
   *            false.
   **/
  bool insertTuple(const Tuple &tuple, const AllowedTypeConversion atc);

  /**
   * @brief Insert a single tuple into this block as part of a batch.
   * @warning A tuple inserted via this method may be placed in an "incorrect"
   *          or sub-optimal location in this block's TupleStorageSubBlock, and
   *          the IndexSubBlocks in this block are not updated to account for
   *          the new tuple. rebuild() MUST be called on this block after calls
   *          to this method to put the block back into a consistent state.
   * @warning Depending on the relative sizes of sub-blocks allocated by this
   *          block's StorageBlockLayout, it is possible to over-fill a block
   *          with more tuples than can be stored in its indexes when rebuild()
   *          is called.
   *
   * @param tuple The tuple to insert.
   * @param atc What level of type conversion to perform to make the values in
   *        tuple compatible with the attributes of this StorageBlock's
   *        relation, if necessary. Safety checks will only actually happen in
   *        debug builds, and in general should happen elsewhere (i.e in the
   *        query optimizer).
   * @return true if the tuple was successfully inserted, false if insertion
   *         failed (e.g. because of not enough space).
   * @exception TupleTooLargeForBlock Even though this block was initially
   *            empty, the tuple was too large to insert. Only thrown if block
   *            is initially empty, otherwise failure to insert simply returns
   *            false.
   **/
  bool insertTupleInBatch(const Tuple &tuple, const AllowedTypeConversion atc);

  /**
   * @brief Perform a SELECT query on this StorageBlock.
   *
   * @param selection A list of scalars, which will be evaluated to obtain
   *        attribute values for each result tuple.
   * @param predicate A predicate for selection. NULL indicates that all tuples
   *        should be matched.
   * @param destination Where to insert the tuples resulting from the SELECT
   *        query.
   * @exception TupleTooLargeForBlock A tuple produced by this selection was
   *            too large to insert into an empty block provided by
   *            destination. Selection may be partially complete (with some
   *            tuples inserted into destination) when this exception is
   *            thrown, causing potential inconsistency.
   *
   * @return true if selection completed with no issues, false if one or more
   *         of the blocks provided by '*destination' was left with
   *         an inconsistent IndexSubBlock (see indicesAreConsistent()).
   *       
   **/
  bool select(const PtrList<Scalar> &selection,
              const Predicate *predicate,
              InsertDestination *destination) const;

  /**
   * @brief Perform a simple SELECT query on this StorageBlock which only
   *        projects attributes and does not evaluate expressions.
   *
   * @param destination Where to insert the tuples resulting from the SELECT
   *        query.
   * @param selection The attributes to project.
   * @param predicate A predicate for selection. NULL indicates that all tuples
   *        should be matched.
   * @exception TupleTooLargeForBlock A tuple produced by this selection was
   *            too large to insert into an empty block provided by
   *            destination. Selection may be partially complete (with some
   *            tuples inserted into destination) when this exception is
   *            thrown, causing potential inconsistency.
   *
   * @return true if selection completed with no issues, false if one or more
   *         of the blocks provided by '*destination' was left with
   *         an inconsistent IndexSubBlock (see indicesAreConsistent()).
   **/
  bool selectSimple(const std::vector<attribute_id> &selection,
                    const Predicate *predicate,
                    InsertDestination *destination) const;

  /**
   * @brief Rebuild all SubBlocks in this StorageBlock, compacting storage
   *        and reordering tuples where applicable and rebuilding indexes from
   *        scratch.
   * @note This method may use an unbounded amount of out-of-band memory.
   * @note Even when rebuilding fails, the TupleStorageSubBlock will be
   *       consistent, and all tuples can be accessed via
   *       getTupleStorageSubBlock().
   *
   * @return True if rebuilding succeeded, false if one of the IndexSubBlocks
   *         ran out of space.
   **/
  bool rebuild() {
    tuple_store_->rebuild();
    return rebuildIndexes(false);
  }

 private:
  static TupleStorageSubBlock* CreateTupleStorageSubBlock(
      const CatalogRelation &relation,
      const TupleStorageSubBlockDescription &description,
      const bool new_block,
      void *sub_block_memory,
      const std::size_t sub_block_memory_size);

  static IndexSubBlock* CreateIndexSubBlock(
      const TupleStorageSubBlock &tuple_store,
      const IndexSubBlockDescription &description,
      const bool new_block,
      void *sub_block_memory,
      const std::size_t sub_block_memory_size);

  static BloomFilterSubBlock* CreateBloomFilterSubBlock(
	  const CatalogRelation &relation,
      const TupleStorageSubBlock &tuple_store,
      const BloomFilterSubBlockDescription &description,
      const bool new_block,
      void *sub_block_memory,
      const std::size_t sub_block_memory_size);


  // Attempt to add an entry for 'new_tuple' to all of the IndexSubBlocks in
  // this StorageBlock. Returns true if entries were successfully added, false
  // otherwise. Removes 'new_tuple' from the TupleStorageSubBlock if entries
  // could not be added.
  bool insertEntryInIndexes(const tuple_id new_tuple);

  // Rebuild all IndexSubBlocks in this StorageBlock. Returns true if all
  // indexes were successfully rebuilt, false if any failed. If 'short_circuit'
  // is true, immediately stops and returns when an IndexSubBlock fails to
  // rebuild, without rebuilding any subsequent IndexSubBlocks or updating this
  // StorageBlock's header.
  bool rebuildIndexes(bool short_circuit);

  TupleIdSequence* getMatchesForPredicate(const Predicate *predicate) const;

  void updateHeader();
  void invalidateAllIndexes();

  StorageBlockHeader block_header_;
  bool all_indices_consistent_;
  bool all_indices_inconsistent_;

  const CatalogRelation &relation_;
  const block_id id_;
  bool dirty_;

  void *block_memory_;
  const std::size_t block_memory_size_;

  ScopedPtr<TupleStorageSubBlock> tuple_store_;
  PtrVector<IndexSubBlock> indices_;
  ScopedPtr<BloomFilterSubBlock> bloom_filter_;

  bool ad_hoc_insert_supported_;
  bool ad_hoc_insert_efficient_;

  friend class storage_explorer::BlockBasedQueryExecutor;

  DISALLOW_COPY_AND_ASSIGN(StorageBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_STORAGE_BLOCK_HPP_
