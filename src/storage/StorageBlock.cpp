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

#include "storage/StorageBlock.hpp"

#include <climits>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "expressions/Predicate.hpp"
#include "expressions/Scalar.hpp"
#include "storage/BasicColumnStoreTupleStorageSubBlock.hpp"
#include "storage/CompressedColumnStoreTupleStorageSubBlock.hpp"
#include "storage/CompressedPackedRowStoreTupleStorageSubBlock.hpp"
#include "storage/CSBTreeIndexSubBlock.hpp"
#include "storage/IndexSubBlock.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/PackedRowStoreTupleStorageSubBlock.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageConfig.h"
#include "storage/StorageErrors.hpp"
#include "storage/StorageManager.hpp"
#include "storage/TupleIdSequence.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/Tuple.hpp"
#include "types/TypeInstance.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrList.hpp"
#include "utility/ScopedPtr.hpp"

using std::size_t;
using std::vector;

namespace quickstep {

StorageBlock::StorageBlock(const CatalogRelation &relation,
                           const block_id id,
                           const StorageBlockLayout &layout,
                           const bool new_block,
                           void *block_memory,
                           const std::size_t block_memory_size)
    : all_indices_consistent_(true),
      all_indices_inconsistent_(false),
      relation_(relation),
      id_(id),
      dirty_(new_block),
      block_memory_(block_memory),
      block_memory_size_(block_memory_size) {
  if (new_block) {
    if (block_memory_size_ < layout.getBlockHeaderSize()) {
      throw BlockMemoryTooSmall("StorageBlock", block_memory_size_);
    }

    layout.copyHeaderTo(block_memory_);
    DEBUG_ASSERT(*static_cast<const int*>(block_memory_) > 0);

    if (!block_header_.ParseFromArray(static_cast<char*>(block_memory_) + sizeof(int),
                                      *static_cast<const int*>(block_memory_))) {
      FATAL_ERROR("A StorageBlockLayout created a malformed StorageBlockHeader.");
    }

    DEBUG_ASSERT(block_header_.IsInitialized());
    DEBUG_ASSERT(StorageBlockLayout::DescriptionIsValid(relation_, block_header_.layout()));
    DEBUG_ASSERT(block_header_.index_size_size() == block_header_.layout().index_description_size());
    DEBUG_ASSERT(block_header_.index_size_size() == block_header_.index_consistent_size());
  } else {
    if (block_memory_size < sizeof(int)) {
      throw MalformedBlock();
    }
    if (*static_cast<const int*>(block_memory_) <= 0) {
      throw MalformedBlock();
    }
    if (*static_cast<const int*>(block_memory_) + sizeof(int) > block_memory_size_) {
      throw MalformedBlock();
    }

    if (!block_header_.ParseFromArray(static_cast<char*>(block_memory_) + sizeof(int),
                                      *static_cast<const int*>(block_memory_))) {
      throw MalformedBlock();
    }
    if (!block_header_.IsInitialized()) {
      throw MalformedBlock();
    }
    if (!StorageBlockLayout::DescriptionIsValid(relation_, block_header_.layout())) {
      throw MalformedBlock();
    }
    if (block_header_.index_size_size() != block_header_.layout().index_description_size()) {
      throw MalformedBlock();
    }
    if (block_header_.index_size_size() != block_header_.index_consistent_size()) {
      throw MalformedBlock();
    }
  }

  size_t block_size_from_metadata = *static_cast<const int*>(block_memory_) + sizeof(int);
  block_size_from_metadata += block_header_.tuple_store_size();
  for (int index_num = 0;
       index_num < block_header_.index_size_size();
       ++index_num) {
    block_size_from_metadata += block_header_.index_size(index_num);
  }

  if (block_size_from_metadata > block_memory_size_) {
    throw MalformedBlock();
  } else if (block_size_from_metadata < block_memory_size_) {
    // WARNING: this isn't strictly an error, but it does indicate that there
    // is unallocated space in the block.
  }

  char *sub_block_address = static_cast<char*>(block_memory_)
                            + sizeof(int)
                            + *static_cast<const int*>(block_memory_);
  tuple_store_.reset(CreateTupleStorageSubBlock(
      relation_,
      block_header_.layout().tuple_store_description(),
      new_block,
      sub_block_address,
      block_header_.tuple_store_size()));
  sub_block_address += block_header_.tuple_store_size();
  ad_hoc_insert_supported_ = tuple_store_->supportsAdHocInsert();
  ad_hoc_insert_efficient_ = tuple_store_->adHocInsertIsEfficient();

  if (block_header_.index_size_size() > 0) {
    all_indices_inconsistent_ = true;
  }
  for (int index_num = 0;
       index_num < block_header_.index_size_size();
       ++index_num) {
    indices_.push_back(CreateIndexSubBlock(*tuple_store_,
                                           block_header_.layout().index_description(index_num),
                                           new_block,
                                           sub_block_address,
                                           block_header_.index_size(index_num)));
    sub_block_address += block_header_.index_size(index_num);
    if (!indices_.back().supportsAdHocAdd()) {
      ad_hoc_insert_efficient_ = false;
    }

    if (block_header_.index_consistent(index_num)) {
      all_indices_inconsistent_ = false;
    } else {
      all_indices_consistent_ = false;
    }
  }

  if (block_header_.layout().bloom_filter_description().IsInitialized()) {
	  // create a bloom filter, if "use_bloom_filter" was set to true
	  bloom_filter_.reset(CreateBloomFilterSubBlock(
	        *tuple_store_,
	        block_header_.layout().bloom_filter_description(),
	        new_block,
	        sub_block_address,
	        block_header_.bloom_filter_size()));
	  sub_block_address += block_header_.bloom_filter_size();
  }
}

bool StorageBlock::insertTuple(const Tuple &tuple, const AllowedTypeConversion atc) {
  if (!ad_hoc_insert_supported_) {
    return false;
  }

  const bool empty_before = tuple_store_->isEmpty();

  TupleStorageSubBlock::InsertResult tuple_store_insert_result = tuple_store_->insertTuple(tuple, atc);
  if (tuple_store_insert_result.inserted_id < 0) {
    DEBUG_ASSERT(tuple_store_insert_result.ids_mutated == false);
    if (empty_before) {
      throw TupleTooLargeForBlock(tuple.getByteSize());
    } else {
      return false;
    }
  }

  bool update_succeeded = true;

  if (tuple_store_insert_result.ids_mutated) {
    update_succeeded = rebuildIndexes(true);
    if (!update_succeeded) {
      tuple_store_->deleteTuple(tuple_store_insert_result.inserted_id);
      if (!rebuildIndexes(true)) {
        // It should always be possible to rebuild an index with the tuples
        // which it originally contained.
        FATAL_ERROR("Rebuilding an IndexSubBlock failed after removing tuples.");
      }
    }
  } else {
    update_succeeded = insertEntryInIndexes(tuple_store_insert_result.inserted_id);
  }

  if (update_succeeded) {
    dirty_ = true;
    return true;
  } else {
    if (empty_before) {
      throw TupleTooLargeForBlock(tuple.getByteSize());
    } else {
      return false;
    }
  }
}

bool StorageBlock::insertTupleInBatch(const Tuple &tuple, const AllowedTypeConversion atc) {
  if (tuple_store_->insertTupleInBatch(tuple, atc)) {
    invalidateAllIndexes();
    return true;
  } else {
    if (tuple_store_->isEmpty()) {
      throw TupleTooLargeForBlock(tuple.getByteSize());
    } else {
      return false;
    }
  }
}

bool StorageBlock::select(const PtrList<Scalar> &selection,
                          const Predicate *predicate,
                          InsertDestination *destination) const {
  // NOTE(chasseur): When the set of matches is small, using the batch-insert
  // path may be suboptimal.
  ScopedPtr<TupleIdSequence> matches(getMatchesForPredicate(predicate));
  bool all_rebuilds_succeeded = true;
  if (matches->size() > 0) {
    StorageBlock *result_block = destination->getBlockForInsertion();
    for (TupleIdSequence::const_iterator it = matches->begin(); it != matches->end(); ++it) {
      Tuple matched_tuple(*tuple_store_, *it, selection);

      while (!result_block->insertTupleInBatch(matched_tuple, kNone)) {
        if (!result_block->rebuild()) {
          all_rebuilds_succeeded = false;
        }
        destination->returnBlock(result_block, true);
        result_block = destination->getBlockForInsertion();
      }
    }

    // NOTE(chasseur): If 'result_block' may get re-used, then it would be
    // more efficient to defer rebuilding until it is finally done being
    // inserted into.
    if (result_block->rebuild()) {
      destination->returnBlock(result_block, false);
    } else {
      all_rebuilds_succeeded = false;
      destination->returnBlock(result_block, true);
    }
  }

  return all_rebuilds_succeeded;
}

bool StorageBlock::selectSimple(const std::vector<attribute_id> &selection,
                                const Predicate *predicate,
                                InsertDestination *destination) const {
  ScopedPtr<TupleIdSequence> matches(getMatchesForPredicate(predicate));
  bool all_rebuilds_succeeded = true;
  if (matches->size() > 0) {
    StorageBlock *result_block = destination->getBlockForInsertion();

    for (TupleIdSequence::const_iterator it = matches->begin(); it != matches->end(); ++it) {
      Tuple matched_tuple(*tuple_store_, *it, selection);

      // FIXME(chasseur): Deal with TupleTooLargeForBlock exception.
      while (!result_block->insertTupleInBatch(matched_tuple, kNone)) {
        if (!result_block->rebuild()) {
          all_rebuilds_succeeded = false;
        }
        destination->returnBlock(result_block, true);
        result_block = destination->getBlockForInsertion();
      }
    }

    if (result_block->rebuild()) {
      destination->returnBlock(result_block, false);
    } else {
      all_rebuilds_succeeded = false;
      destination->returnBlock(result_block, true);
    }
  }

  return all_rebuilds_succeeded;
}

TupleStorageSubBlock* StorageBlock::CreateTupleStorageSubBlock(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description,
    const bool new_block,
    void *sub_block_memory,
    const std::size_t sub_block_memory_size) {
  DEBUG_ASSERT(description.IsInitialized());
  switch (description.sub_block_type()) {
    case TupleStorageSubBlockDescription::PACKED_ROW_STORE:
      return new PackedRowStoreTupleStorageSubBlock(relation,
                                                    description,
                                                    new_block,
                                                    sub_block_memory,
                                                    sub_block_memory_size);
    case TupleStorageSubBlockDescription::BASIC_COLUMN_STORE:
      return new BasicColumnStoreTupleStorageSubBlock(relation,
                                                      description,
                                                      new_block,
                                                      sub_block_memory,
                                                      sub_block_memory_size);
    case TupleStorageSubBlockDescription::COMPRESSED_PACKED_ROW_STORE:
      return new CompressedPackedRowStoreTupleStorageSubBlock(relation,
                                                              description,
                                                              new_block,
                                                              sub_block_memory,
                                                              sub_block_memory_size);
    case TupleStorageSubBlockDescription::COMPRESSED_COLUMN_STORE:
      return new CompressedColumnStoreTupleStorageSubBlock(relation,
                                                           description,
                                                           new_block,
                                                           sub_block_memory,
                                                           sub_block_memory_size);
    default:
      if (new_block) {
        FATAL_ERROR("A StorageBlockLayout provided an unknown TupleStorageSubBlockType.");
      } else {
        throw MalformedBlock();
      }
  }
}

IndexSubBlock* StorageBlock::CreateIndexSubBlock(
    const TupleStorageSubBlock &tuple_store,
    const IndexSubBlockDescription &description,
    const bool new_block,
    void *sub_block_memory,
    const std::size_t sub_block_memory_size) {
  DEBUG_ASSERT(description.IsInitialized());
  switch (description.sub_block_type()) {
    case IndexSubBlockDescription::CSB_TREE:
      return new CSBTreeIndexSubBlock(tuple_store,
                                      description,
                                      new_block,
                                      sub_block_memory,
                                      sub_block_memory_size);
    default:
      if (new_block) {
        FATAL_ERROR("A StorageBlockLayout provided an unknown IndexBlockType.");
      } else {
        throw MalformedBlock();
      }
  }
}

BloomFilterSubBlock* StorageBlock::CreateBloomFilterSubBlock(
		const TupleStorageSubBlock &tuple_store,
        const BloomFilterSubBlockDescription &description,
        const bool new_block,
        void *sub_block_memory,
        const std::size_t sub_block_memory_size) {
	DEBUG_ASSERT(description.IsInitialized());
	switch (description.sub_block_type()) {
		case BloomFilterSubBlockDescription::DEFAULT:
			return new DefaultBloomFilterSubBlock(tuple_store,
												  description,
												  new_block,
												  sub_block_memory,
												  sub_block_memory_size);
		default:
			if (new_block) {
				FATAL_ERROR("A StorageBlockLayout provided an unknown BloomFilterBlockType.");
			} else {
				throw MalformedBlock();
			}
	}
}

bool StorageBlock::insertEntryInIndexes(const tuple_id new_tuple) {
  DEBUG_ASSERT(ad_hoc_insert_supported_);
  DEBUG_ASSERT(new_tuple >= 0);
  DEBUG_ASSERT(all_indices_consistent_);

  for (PtrVector<IndexSubBlock>::iterator it = indices_.begin();
       it != indices_.end();
       ++it) {
    bool entry_added;
    if (it->supportsAdHocAdd()) {
      entry_added = it->addEntry(new_tuple);
    } else {
      entry_added = it->rebuild();
    }
    if (!entry_added) {
      // Roll back if index is full.
      //
      // NOTE(chasseur): For fragmented indexes, rebuilding might allow
      // success.
      bool rebuild_some_indices = false;
      for (PtrVector<IndexSubBlock>::iterator fixer_it = indices_.begin();
           fixer_it != it;
           ++fixer_it) {
        // Do ad-hoc removal for those indices which support it. Those that
        // don't are rebuilt after the entry is removed from the tuple_store_
        // below.
        if (fixer_it->supportsAdHocRemove()) {
          fixer_it->removeEntry(new_tuple);
        } else {
          rebuild_some_indices = true;
        }
      }

      if (tuple_store_->deleteTuple(new_tuple)) {
        // The tuple-ID sequence was mutated, so rebuild all indices.
        if (!rebuildIndexes(true)) {
          FATAL_ERROR("Rebuilding an IndexSubBlock failed after removing tuples.");
        }
      } else if (rebuild_some_indices) {
        // Rebuild those indices that were modified that don't support ad-hoc
        // removal.
        for (PtrVector<IndexSubBlock>::iterator fixer_it = indices_.begin();
             fixer_it != it;
             ++fixer_it) {
          if (!fixer_it->supportsAdHocRemove()) {
            if (!fixer_it->rebuild()) {
              // It should always be possible to rebuild an index with the
              // tuples which it originally contained.
              FATAL_ERROR("Rebuilding an IndexSubBlock failed after removing tuples.");
            }
          }
        }
      }

      return false;
    }
  }

  return true;
}

bool StorageBlock::rebuildIndexes(bool short_circuit) {
  if (indices_.empty()) {
    return true;
  }

  all_indices_consistent_ = true;
  all_indices_inconsistent_ = true;

  int index_num = 0;
  for (PtrVector<IndexSubBlock>::iterator it = indices_.begin();
       it != indices_.end();
       ++it, ++index_num) {
    if (it->rebuild()) {
      all_indices_inconsistent_ = false;
      block_header_.set_index_consistent(index_num, true);
    } else {
      all_indices_consistent_ = false;
      block_header_.set_index_consistent(index_num, false);
      if (short_circuit) {
        return false;
      }
    }
  }
  updateHeader();

  return all_indices_consistent_;
}

TupleIdSequence* StorageBlock::getMatchesForPredicate(const Predicate *predicate) const {
  // TODO(chasseur): Use indexes where possible.
  return tuple_store_->getMatchesForPredicate(predicate);
}

void StorageBlock::updateHeader() {
  DEBUG_ASSERT(*static_cast<const int*>(block_memory_) == block_header_.ByteSize());

  if (!block_header_.SerializeToArray(static_cast<char*>(block_memory_) + sizeof(int),
                                      block_header_.ByteSize())) {
    FATAL_ERROR("Failed to do binary serialization of StorageBlockHeader in StorageBlock::updateHeader()");
  }
}

void StorageBlock::invalidateAllIndexes() {
  if ((!indices_.empty()) && (!all_indices_inconsistent_)) {
    for (unsigned int index_num = 0;
         index_num < indices_.size();
         ++index_num) {
      block_header_.set_index_consistent(index_num, false);
    }
    all_indices_consistent_ = false;
    all_indices_inconsistent_ = true;

    updateHeader();
  }
}

}  // namespace quickstep
