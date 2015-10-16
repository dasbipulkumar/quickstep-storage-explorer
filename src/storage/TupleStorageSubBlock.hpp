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

#ifndef QUICKSTEP_STORAGE_TUPLE_STORAGE_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_TUPLE_STORAGE_SUB_BLOCK_HPP_

#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/AllowedTypeConversion.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogRelation;
class LiteralTypeInstance;
class Predicate;
class Tuple;
class TupleStorageSubBlockDescription;
class TypeInstance;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief SubBlock which stores complete tuples.
 **/
class TupleStorageSubBlock {
 public:
  /**
   * @brief Structure describing the result of an insertion of a single tuple.
   **/
  struct InsertResult {
    InsertResult(const tuple_id inserted_id_arg, const bool ids_mutated_arg)
        : inserted_id(inserted_id_arg),
          ids_mutated(ids_mutated_arg) {
    }

    /**
     * @brief The ID of the inserted tuple, or -1 if unable to insert.
     **/
    tuple_id inserted_id;

    /**
     * @brief True if other tuples in the TupleStorageSubBlock had their ids
     *        mutated (requiring that indexes be rebuilt), false otherwise.
     **/
    bool ids_mutated;
  };

  /**
   * @brief Constructor.
   *
   * @param relation The CatalogRelation which this SubBlock belongs to.
   * @param description A description containing any parameters needed to
   *        construct this SubBlock. Implementation-specific parameters are
   *        defined as extensions in StorageBlockLayout.proto.
   * @param new_block Whether this is a newly-created block.
   * @param sub_block_memory The memory slot to use for the block's contents.
   * @param sub_block_memory_size The size of the memory slot in bytes.
   * @exception BlockMemoryTooSmall This TupleStorageSubBlock hasn't been
   *            provided enough memory to store metadata.
   **/
  TupleStorageSubBlock(const CatalogRelation &relation,
                       const TupleStorageSubBlockDescription &description,
                       const bool new_block,
                       void *sub_block_memory,
                       const std::size_t sub_block_memory_size)
                       : relation_(relation),
                         description_(description),
                         sub_block_memory_(sub_block_memory),
                         sub_block_memory_size_(sub_block_memory_size) {
  }

  /**
   * @brief Virtual destructor.
   **/
  virtual ~TupleStorageSubBlock() {
  }

  /**
   * @brief Identify the type of this TupleStorageSubBlock.
   *
   * @return This TupleStorageSubBlock's type.
   **/
  virtual TupleStorageSubBlockType getTupleStorageSubBlockType() const = 0;

  /**
   * @brief Determine whether this SubBlock supports the getAttributeValue()
   *        method to get an untyped pointer to a value for a particular
   *        attribute.
   *
   * @param attr The ID of the attribute which getAttributeValue() would be
   *             used with.
   * @return Whether the getAttributeValue() method can be used on this
   *         SubBlock with the specified attr.
   **/
  virtual bool supportsUntypedGetAttributeValue(const attribute_id attr) const = 0;

  /**
   * @brief Determine whether this SubBlock supports ad-hoc insertion of
   *        individual tuples via the insertTuple() method.
   * @note If this method returns false, then tuples can only be inserted via
   *       insertTupleInBatch(), which should be followed by a call to
   *       rebuild() when a batch is fully inserted.
   * @note Even if this method returns false, it is still legal to call
   *       insertTuple(), although it will always fail to actually insert.
   *
   * @return Whether the insertTuple() can be used on this SubBlock.
   **/
  virtual bool supportsAdHocInsert() const = 0;

  /**
   * @brief Determine whether inserting tuples one-at-a-time via the
   *        insertTuple() method is efficient (i.e. has constant time and space
   *        costs and does not require expensive reorganization of other tuples
   *        in this SubBlock).
   *
   * @return Whether insertTuple() is efficient for this SubBlock.
   **/
  virtual bool adHocInsertIsEfficient() const = 0;

  const CatalogRelation& getRelation() const {
    return relation_;
  }

  /**
   * @brief Determine whether this block has any tuples in it.
   *
   * @return True if this SubBlock is empty, false otherwise.
   **/
  virtual bool isEmpty() const = 0;

  /**
   * @brief Determine whether this block is packed, i.e. there are no holes in
   *        the tuple-id sequence.
   *
   * @return True if this SubBlock is packed, false otherwise.
   **/
  virtual bool isPacked() const = 0;

  /**
   * @brief Get the highest tuple-id of a valid tuple in this SubBlock.
   *
   * @return The highest tuple-id of a tuple stored in this SubBlock (-1 if
   *         this SubBlock is empty).
   **/
  virtual tuple_id getMaxTupleID() const = 0;

  /**
   * @brief Get the number of tuples contained in this SubBlock.
   * @note The default implementation is O(1) for packed TupleStorageSubBlocks,
   *       but TupleStorageSubBlock implementations which may be non-packed
   *       should override this where possible, as the default version of this
   *       method is O(N) when the SubBlock is non-packed.
   *
   * @return The number of tuples contained in this TupleStorageSubBlock.
   **/
  virtual tuple_id numTuples() const;

  /**
   * @brief Determine whether a tuple with the given id exists in this
   *        SubBlock.
   *
   * @param tuple The ID to check.
   * @return True if tuple exists, false if it does not.
   **/
  virtual bool hasTupleWithID(const tuple_id tuple) const = 0;

  /**
   * @brief Insert a single tuple into this TupleStorageSubBlock.
   *
   * @param tuple The tuple to insert, whose values must be in the correct
   *        order.
   * @param atc What level of type conversion to perform to make the values in
   *        tuple compatible with the attributes of this StorageBlock's
   *        relation, if necessary. Safety checks will only actually happen in
   *        debug builds, and in general should happen elsewhere (i.e in the
   *        query optimizer).
   * @return The result of the insertion.
   **/
  virtual InsertResult insertTuple(const Tuple &tuple, const AllowedTypeConversion atc) = 0;

  /**
   * @brief Insert a single tuple as part of a batch.
   * @note This method is intended to allow a large number of tuples to be
   *       loaded without incurring the full cost of maintaining a "clean"
   *       internal block structure. Once a batch of tuples have been inserted,
   *       rebuild() should be called to put this TupleStorageSubBlock into a
   *       consistent state.
   * @warning The inserted tuple may be placed in an "incorrect" or sub-optimal
   *          location in this TupleStorageSubBlock. The only method which is
   *          safe to call between insertTupleInBatch() and rebuild() is
   *          another invocation of insertTupleInBatch() itself.
   *
   * @param tuple The tuple to insert, whose values must be in the correct
   *        order.
   * @param atc What level of type conversion to perform to make the values in
   *        tuple compatible with the attributes of this StorageBlock's
   *        relation, if necessary. Safety checks will only actually happen in
   *        debug builds, and in general should happen elsewhere (i.e in the
   *        query optimizer).
   * @return True if the insertion was successful, false if out of space.
   **/
  virtual bool insertTupleInBatch(const Tuple &tuple, const AllowedTypeConversion atc) = 0;

  /**
   * @brief Get the (untyped) value of an attribute in a tuple in this buffer.
   * @warning This method may not be supported for all implementations of
   *          TupleStorageSubBlock. supportsUntypedGetAttributeValue() MUST be
   *          called first to determine if this method is usable.
   * @warning For debug builds, an assertion checks whether the specified tuple
   *          actually exists. In general, this method should only be called
   *          for tuples which are known to exist (from hasTupleWithID(),
   *          isPacked() and getMaxTupleID(), or presence in an index).
   *
   * @param tuple The desired tuple in this SubBlock.
   * @param attr The attribute id of the desired attribute.
   * @return An untyped pointer to the value of the specified attribute in the
   *         specified tuple.
   **/
  virtual const void* getAttributeValue(const tuple_id tuple, const attribute_id attr) const = 0;

  /**
   * @brief Get the value of the specified attribute of the specified tuple as
   *        a TypeInstance.
   * @note If supportsUntypedGetAttributeValue() is true for this
   *       TupleStorageSubBlock, then a ReferenceTypeInstance will be returned,
   *       otherwise a LiteralTypeInstance will be returned.
   * @warning For debug builds, an assertion checks whether the specified tuple
   *          actually exists. In general, this method should only be called
   *          for tuples which are known to exist (from hasTupleWithID(),
   *          isPacked() and getMaxTupleID(), or presence in an index).
   *
   * @param tuple The desired tuple in this SubBlock.
   * @param attr The attribute id of the desired attribute.
   * @return The data as a TypeInstance.
   **/
  virtual TypeInstance* getAttributeValueTyped(const tuple_id tuple, const attribute_id attr) const = 0;

  /**
   * @brief Delete a single tuple from this TupleStorageSubBlock.
   * @warning For debug builds, an assertion checks whether the specified tuple
   *          actually exists. In general, this method should only be called
   *          for tuples which are known to exist (from hasTupleWithID(),
   *          isPacked() and getMaxTupleID(), or presence in an index).
   * @warning Always check the return value of this call to see whether indexes
   *          must be totally rebuilt.
   *
   * @param tuple The tuple to delete.
   * @return True if other tuples have had their ids mutated (requiring indexes
   *         to be rebuilt), false if other tuple IDs are stable.
   **/
  virtual bool deleteTuple(const tuple_id tuple) = 0;

  /**
   * @brief Get the IDs of tuples in this SubBlock which match a given
   *        predicate (or all tuples if no predicate is specified).
   * @note A default, dumb implementation of this method is supplied in the
   *       base class TupleStorageSubBlock. Implementations whose structure
   *       permits a more efficient implementation should override it.
   *
   * @param predicate The predicate to match (all tuples will match if
   *        predicate is NULL).
   * @return The IDs of tuples matching the specified predicate.
   **/
  virtual TupleIdSequence* getMatchesForPredicate(const Predicate *predicate) const;

  /**
   * @brief Rebuild this TupleStorageSubBlock, compacting storage and
   *        reordering tuples where applicable.
   * @note This method may use an unbounded amount of out-of-band memory.
   **/
  virtual void rebuild() = 0;

  /**
   * @brief Check if this TupleStorageSubBlock is compressed, i.e. whether it
   *        can safely be cast to CompressedTupleStorageSubBlock.
   *
   * @return true if this is a CompressedTupleStorageSubBlock, false otherwise.
   **/
  virtual bool isCompressed() const {
    return false;
  }

 protected:
  /**
   * @brief In debug builds, performs assertions to make sure that the values
   *        in tuple can be inserted into this TupleStorageSubBlock at the
   *        specified type conversion level.
   *
   * @param tuple The tuple to check.
   * @param atc The allowed level of type conversion.
   **/
  void paranoidInsertTypeCheck(const Tuple &tuple, AllowedTypeConversion atc);

  const CatalogRelation &relation_;
  const TupleStorageSubBlockDescription &description_;

  void *sub_block_memory_;
  const std::size_t sub_block_memory_size_;

 private:
  DISALLOW_COPY_AND_ASSIGN(TupleStorageSubBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_TUPLE_STORAGE_SUB_BLOCK_HPP_
