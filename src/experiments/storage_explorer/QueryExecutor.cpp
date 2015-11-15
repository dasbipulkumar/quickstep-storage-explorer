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

#include "experiments/storage_explorer/QueryExecutor.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <vector>

#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogRelation.hpp"
#include "experiments/storage_explorer/ThreadAffinity.hpp"
#include "expressions/Predicate.hpp"
#include "storage/IndexSubBlock.hpp"
#include "storage/PackedRowStoreTupleStorageSubBlock.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "storage/StorageManager.hpp"
#include "storage/TupleIdSequence.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "threading/Mutex.hpp"
#include "threading/Thread.hpp"
#include "types/Tuple.hpp"
#include "utility/ScopedPtr.hpp"

using std::random_shuffle;
using std::size_t;
using std::sort;
using std::vector;

namespace quickstep {
namespace storage_explorer {

namespace query_execution_threads {

class BlockBasedPredicateEvaluationThread : public Thread {
 public:
  BlockBasedPredicateEvaluationThread(
      BlockBasedPredicateEvaluationQueryExecutor *parent_executor,
      const int bound_cpu_id = -1)
      : parent_executor_(parent_executor),
        bound_cpu_id_(bound_cpu_id) {
  }

 protected:
  void run() {
    if (bound_cpu_id_ >= 0) {
      ThreadAffinity::BindThisThreadToCPU(bound_cpu_id_);
    }
    block_id current_block_id = parent_executor_->getNextInputBlock();
    while (current_block_id >= 0) {
      if (parent_executor_->use_index_) {
        const StorageBlock &block = parent_executor_->storage_manager_->getBlock(current_block_id);
        ScopedPtr<TupleIdSequence> matches(parent_executor_->evaluatePredicateWithIndex(
            parent_executor_->getIndex(block, parent_executor_->use_index_num_),
            block.getTupleStorageSubBlock()));

        if (parent_executor_->sort_index_matches_) {
          matches->sort();
        }
      } else {
        ScopedPtr<TupleIdSequence> matches(parent_executor_->evaluatePredicateOnBlock(
                    parent_executor_->storage_manager_->getBlock(current_block_id)));
      }

      current_block_id = parent_executor_->getNextInputBlock();
    }
  }

 private:
  BlockBasedPredicateEvaluationQueryExecutor *parent_executor_;
  const int bound_cpu_id_;

  DISALLOW_COPY_AND_ASSIGN(BlockBasedPredicateEvaluationThread);
};

class BlockBasedSelectionThread : public Thread {
 public:
  BlockBasedSelectionThread(BlockBasedSelectionQueryExecutor *parent_executor,
                            const int bound_cpu_id = -1)
      : parent_executor_(parent_executor),
        bound_cpu_id_(bound_cpu_id) {
  }

 protected:
  void run() {
    if (bound_cpu_id_ >= 0) {
      ThreadAffinity::BindThisThreadToCPU(bound_cpu_id_);
    }
    block_id current_block_id = parent_executor_->getNextInputBlock();
    while (current_block_id >= 0) {
      const StorageBlock &block = parent_executor_->storage_manager_->getBlock(current_block_id);

      ScopedPtr<TupleIdSequence> matches;
      if (parent_executor_->use_index_) {
        matches.reset(parent_executor_->evaluatePredicateWithIndex(
            parent_executor_->getIndex(block, parent_executor_->use_index_num_),
            block.getTupleStorageSubBlock()));

        if (parent_executor_->sort_index_matches_) {
          matches->sort();
        }
      } else {
        matches.reset(parent_executor_->evaluatePredicateOnBlock(block));
      }

      parent_executor_->doProjection(block, matches.get());
      current_block_id = parent_executor_->getNextInputBlock();
    }
  }

 private:
  BlockBasedSelectionQueryExecutor *parent_executor_;
  const int bound_cpu_id_;

  DISALLOW_COPY_AND_ASSIGN(BlockBasedSelectionThread);
};

class FileBasedPredicateEvaluationThread : public Thread {
 public:
  FileBasedPredicateEvaluationThread(FileBasedPredicateEvaluationQueryExecutor *parent_executor,
                                     const size_t partition_number,
                                     const int bound_cpu_id = -1)
      : parent_executor_(parent_executor),
        partition_number_(partition_number),
        bound_cpu_id_(bound_cpu_id) {
  }

 protected:
  void run() {
    if (bound_cpu_id_ >= 0) {
      ThreadAffinity::BindThisThreadToCPU(bound_cpu_id_);
    }
    if (parent_executor_->use_index_) {
      ScopedPtr<TupleIdSequence> matches(parent_executor_->evaluatePredicateWithIndex(
          *(parent_executor_->indices_[parent_executor_->use_index_num_][partition_number_]),
          *(parent_executor_->tuple_stores_[partition_number_])));
      if (parent_executor_->sort_index_matches_) {
        matches->sort();
      }
    } else {
      ScopedPtr<TupleIdSequence> matches(parent_executor_->evaluatePredicateOnTupleStore(
          *(parent_executor_->tuple_stores_[partition_number_])));
    }
  }

 private:
  FileBasedPredicateEvaluationQueryExecutor *parent_executor_;
  const size_t partition_number_;
  const int bound_cpu_id_;

  DISALLOW_COPY_AND_ASSIGN(FileBasedPredicateEvaluationThread);
};

class FileBasedSelectionThread : public Thread {
 public:
  FileBasedSelectionThread(FileBasedSelectionQueryExecutor *parent_executor,
                           const size_t partition_number,
                           const int bound_cpu_id = -1)
      : parent_executor_(parent_executor),
        partition_number_(partition_number),
        bound_cpu_id_(bound_cpu_id) {
  }

 protected:
  void run() {
    if (bound_cpu_id_ >= 0) {
      ThreadAffinity::BindThisThreadToCPU(bound_cpu_id_);
    }

    ScopedPtr<TupleIdSequence> matches;
    if (parent_executor_->use_index_) {
      matches.reset(parent_executor_->evaluatePredicateWithIndex(
          *(parent_executor_->indices_[parent_executor_->use_index_num_][partition_number_]),
          *(parent_executor_->tuple_stores_[partition_number_])));
      if (parent_executor_->sort_index_matches_) {
        matches->sort();
      }
    } else {
      matches.reset(parent_executor_->evaluatePredicateOnTupleStore(
          *(parent_executor_->tuple_stores_[partition_number_])));
    }

    if (matches->size() > 0) {
      ScopedBuffer result_buffer(parent_executor_->result_buffer_size_bytes_);
      ScopedPtr<TupleStorageSubBlock> result_store;
      result_store.reset(new PackedRowStoreTupleStorageSubBlock(
          *(parent_executor_->result_relation_),
          parent_executor_->result_store_description_,
          true,
          result_buffer.get(),
          parent_executor_->result_buffer_size_bytes_));

      for (TupleIdSequence::const_iterator it = matches->begin(); it != matches->end(); ++it) {
        Tuple matched_tuple(*(parent_executor_->tuple_stores_[partition_number_]),
                            *it,
                            parent_executor_->projection_attributes_);

        if (!result_store->insertTupleInBatch(matched_tuple, kNone)) {
          FATAL_ERROR("Ran out of space in result buffer.\n");
        }
      }

      result_store->rebuild();
    }
  }

 private:
  FileBasedSelectionQueryExecutor *parent_executor_;
  const size_t partition_number_;
  const int bound_cpu_id_;

  DISALLOW_COPY_AND_ASSIGN(FileBasedSelectionThread);
};

}  // namespace query_execution_threads

TupleIdSequence* QueryExecutor::evaluatePredicateOnTupleStore(const TupleStorageSubBlock &tuple_store) const {
  switch (predicate_.getPredicateType()) {
    case Predicate::kTrue:
      return tuple_store.getMatchesForPredicate(NULL);
    case Predicate::kFalse:
      return new TupleIdSequence();
    default:
      return tuple_store.getMatchesForPredicate(&predicate_);
  }
}

TupleIdSequence* QueryExecutor::evaluatePredicateWithIndex(const IndexSubBlock &index,
                                                           const TupleStorageSubBlock &tuple_store) const {
  switch (predicate_.getPredicateType()) {
    case Predicate::kTrue:
      return tuple_store.getMatchesForPredicate(NULL);
    case Predicate::kFalse:
      return new TupleIdSequence();
    default:
      {
        IndexSearchResult result = index.getMatchesForPredicate(predicate_);
        return result.sequence;
      }
  }
}

TupleIdSequence* QueryExecutor::evaluatePredicateOnBlock(const StorageBlock &block) const {
  switch (predicate_.getPredicateType()) {
    case Predicate::kTrue:
      return block.getTupleStorageSubBlock().getMatchesForPredicate(NULL);
    case Predicate::kFalse:
      return new TupleIdSequence();
    default:
      return block.getMatchesForPredicate(&predicate_);
  }
}

const IndexSubBlock& BlockBasedQueryExecutor::getIndex(const StorageBlock &block,
                                                       const std::size_t index_num) const {
  return block.indices_[index_num];
}

BlockBasedPredicateEvaluationQueryExecutor::BlockBasedPredicateEvaluationQueryExecutor(
    const CatalogRelation &relation,
    const Predicate &predicate,
    const attribute_id predicate_attribute_id,
    const std::vector<int> &thread_affinities,
    const std::size_t num_threads,
    StorageManager *storage_manager)
    : BlockBasedQueryExecutor(relation,
                              predicate,
                              predicate_attribute_id,
                              thread_affinities,
                              num_threads,
                              storage_manager),
      next_block_iterator_(relation.begin_blocks()) {
  if (thread_affinities_.empty()) {
    for (size_t thread_num = 0; thread_num < num_threads; ++thread_num) {
      threads_.push_back(new query_execution_threads::BlockBasedPredicateEvaluationThread(this));
    }
  } else {
    for (vector<int>::const_iterator cpu_it = thread_affinities.begin();
         cpu_it != thread_affinities.end();
         ++cpu_it) {
      threads_.push_back(new query_execution_threads::BlockBasedPredicateEvaluationThread(this, *cpu_it));
    }
  }
}

block_id BlockBasedPredicateEvaluationQueryExecutor::getNextInputBlock() {
  MutexLock lock(mutex_);
  if (next_block_iterator_ == relation_.end_blocks()) {
    return -1;
  } else {
    block_id next_block = *next_block_iterator_;
    ++next_block_iterator_;
    return next_block;
  }
}

block_id PartitionedBlockBasedPredicateEvaluationQueryExecutor::getNextInputBlock() {
  MutexLock lock(mutex_);
  if (partition_blocks_iterator_ == partition_blocks_.end()) {
    return -1;
  } else {
    block_id next_block = *partition_blocks_iterator_;
    ++partition_blocks_iterator_;
    return next_block;
  }
}

BlockBasedSelectionQueryExecutor::BlockBasedSelectionQueryExecutor(
    const CatalogRelation &relation,
    const Predicate &predicate,
    const attribute_id predicate_attribute_id,
    const std::vector<int> &thread_affinities,
    const std::size_t num_threads,
    StorageManager *storage_manager,
    const attribute_id projection_attributes_num,
    const std::size_t result_block_size_slots,
    CatalogDatabase *database)
    : BlockBasedQueryExecutor(relation,
                              predicate,
                              predicate_attribute_id,
                              thread_affinities,
                              num_threads,
                              storage_manager),
      database_(database),
      next_block_iterator_(relation.begin_blocks()) {
  // Choose attributes to project.
  assert(projection_attributes_num > 0);
  assert(static_cast<CatalogRelation::size_type>(projection_attributes_num) <= relation_.size());
  // Include the attribute which the predicate selects on.
  projection_attributes_.push_back(predicate_attribute_id_);
  // If projecting >1 attribute, randomly choose the rest.
  if (projection_attributes_num > 1) {
    vector<attribute_id> relation_attributes;
    for (CatalogRelation::const_iterator attr_it = relation_.begin();
         attr_it != relation_.end();
         ++attr_it) {
      if (attr_it->getID() != predicate_attribute_id_) {
        relation_attributes.push_back(attr_it->getID());
      }
    }
    random_shuffle(relation_attributes.begin(), relation_attributes.end());
    for (attribute_id projected_num = 0;
         projected_num < projection_attributes_num - 1;
         ++projected_num) {
      projection_attributes_.push_back(relation_attributes[projected_num]);
    }
    sort(projection_attributes_.begin(), projection_attributes_.end());
  }

  // Create the temporary result relation.
  ScopedPtr<CatalogRelation> result_relation_tmp(new CatalogRelation(database_,
                                                                     "_QSTEMP_SELECT_RESULT",
                                                                     -1,
                                                                     true));
  for (vector<attribute_id>::const_iterator projection_it = projection_attributes_.begin();
       projection_it != projection_attributes_.end();
       ++projection_it) {
    const CatalogAttribute &original_attribute = relation.getAttributeById(*projection_it);

    result_relation_tmp->addAttribute(new CatalogAttribute(result_relation_tmp.get(),
                                                           original_attribute.getName(),
                                                           original_attribute.getType()));
  }
  result_relation_ = result_relation_tmp.get();
  database_->addRelation(result_relation_tmp.release());

  // Create the result layout.
  result_layout_.reset(new StorageBlockLayout(*result_relation_));
  result_layout_->getDescriptionMutable()->set_num_slots(result_block_size_slots);
  result_layout_->getDescriptionMutable()->mutable_tuple_store_description()
      ->set_sub_block_type(TupleStorageSubBlockDescription::PACKED_ROW_STORE);
  result_layout_->finalize();

  // Create the InsertDestination.
  result_destination_.reset(new BlockPoolInsertDestination(storage_manager_,
                                                           result_relation_,
                                                           result_layout_.get()));

  // Create execution threads.
  if (thread_affinities_.empty()) {
    for (size_t thread_num = 0; thread_num < num_threads; ++thread_num) {
      threads_.push_back(new query_execution_threads::BlockBasedSelectionThread(this));
    }
  } else {
    for (vector<int>::const_iterator cpu_it = thread_affinities_.begin();
         cpu_it != thread_affinities_.end();
         ++cpu_it) {
      threads_.push_back(new query_execution_threads::BlockBasedSelectionThread(this, *cpu_it));
    }
  }
}

BlockBasedSelectionQueryExecutor::~BlockBasedSelectionQueryExecutor() {
  for (CatalogRelation::const_iterator_blocks it = result_relation_->begin_blocks();
       it != result_relation_->end_blocks();
       ++it) {
    storage_manager_->evictBlock(*it);
  }

  database_->dropRelationById(result_relation_->getID());
}

block_id BlockBasedSelectionQueryExecutor::getNextInputBlock() {
  MutexLock lock(mutex_);
  if (next_block_iterator_ == relation_.end_blocks()) {
    return -1;
  } else {
    block_id next_block = *next_block_iterator_;
    ++next_block_iterator_;
    return next_block;
  }
}

block_id PartitionedBlockBasedSelectionQueryExecutor::getNextInputBlock() {
  MutexLock lock(mutex_);
  if (partition_blocks_iterator_ == partition_blocks_.end()) {
    return -1;
  } else {
    block_id next_block = *partition_blocks_iterator_;
    ++partition_blocks_iterator_;
    return next_block;
  }
}

void BlockBasedSelectionQueryExecutor::doProjection(const StorageBlock &block,
                                                    TupleIdSequence *matches) {
  if (matches->size() > 0) {
    const TupleStorageSubBlock &tuple_store = block.getTupleStorageSubBlock();
    StorageBlock *result_block = result_destination_->getBlockForInsertion();

    for (TupleIdSequence::const_iterator it = matches->begin(); it != matches->end(); ++it) {
      Tuple matched_tuple(tuple_store, *it, projection_attributes_);

      while (!result_block->insertTuple(matched_tuple, kNone)) {
        result_destination_->returnBlock(result_block, true);
        result_block = result_destination_->getBlockForInsertion();
      }
    }

    result_destination_->returnBlock(result_block, false);
  }
}

FileBasedPredicateEvaluationQueryExecutor::FileBasedPredicateEvaluationQueryExecutor(
    const CatalogRelation &relation,
    const Predicate &predicate,
    const attribute_id predicate_attribute_id,
    const std::vector<int> &thread_affinities,
    const std::vector<const TupleStorageSubBlock*> &tuple_stores,
    const std::vector<std::vector<const IndexSubBlock*> > &indices)
    : FileBasedQueryExecutor(relation,
                             predicate,
                             predicate_attribute_id,
                             thread_affinities,
                             tuple_stores,
                             indices) {
  for (size_t thread_num = 0;
       thread_num < tuple_stores_.size();
       ++thread_num) {
    if (thread_affinities_.empty()) {
      threads_.push_back(new query_execution_threads::FileBasedPredicateEvaluationThread(
          this,
          thread_num));
    } else {
      threads_.push_back(new query_execution_threads::FileBasedPredicateEvaluationThread(
          this,
          thread_num,
          thread_affinities[thread_num]));
    }
  }
}

FileBasedSelectionQueryExecutor::FileBasedSelectionQueryExecutor(
    const CatalogRelation &relation,
    const Predicate &predicate,
    const attribute_id predicate_attribute_id,
    const std::vector<int> &thread_affinities,
    const std::vector<const TupleStorageSubBlock*> &tuple_stores,
    const std::vector<std::vector<const IndexSubBlock*> > &indices,
    const attribute_id projection_attributes_num,
    const std::size_t result_buffer_size_bytes,
    CatalogDatabase *database)
    : FileBasedQueryExecutor(relation,
                             predicate,
                             predicate_attribute_id,
                             thread_affinities,
                             tuple_stores,
                             indices),
      database_(database),
      result_buffer_size_bytes_(result_buffer_size_bytes) {
  // Choose attributes to project.
  assert(projection_attributes_num > 0);
  assert(static_cast<CatalogRelation::size_type>(projection_attributes_num) <= relation_.size());
  // Include the attribute which the predicate selects on.
  projection_attributes_.push_back(predicate_attribute_id_);
  // If projecting >1 attribute, randomly choose the rest.
  if (projection_attributes_num > 1) {
    vector<attribute_id> relation_attributes;
    for (CatalogRelation::const_iterator attr_it = relation_.begin();
         attr_it != relation_.end();
         ++attr_it) {
      if (attr_it->getID() != predicate_attribute_id_) {
        relation_attributes.push_back(attr_it->getID());
      }
    }
    random_shuffle(relation_attributes.begin(), relation_attributes.end());
    for (attribute_id projected_num = 0;
         projected_num < projection_attributes_num - 1;
         ++projected_num) {
      projection_attributes_.push_back(relation_attributes[projected_num]);
    }
    sort(projection_attributes_.begin(), projection_attributes_.end());
  }

  // Create the temporary result relation.
  ScopedPtr<CatalogRelation> result_relation_tmp(new CatalogRelation(database_,
                                                                     "_QSTEMP_SELECT_RESULT",
                                                                     -1,
                                                                     true));
  for (vector<attribute_id>::const_iterator projection_it = projection_attributes_.begin();
       projection_it != projection_attributes_.end();
       ++projection_it) {
    const CatalogAttribute &original_attribute = relation.getAttributeById(*projection_it);

    result_relation_tmp->addAttribute(new CatalogAttribute(result_relation_tmp.get(),
                                                           original_attribute.getName(),
                                                           original_attribute.getType()));
  }
  result_relation_ = result_relation_tmp.get();
  database_->addRelation(result_relation_tmp.release());

  // Initialize the description for result tuple stores.
  result_store_description_.set_sub_block_type(TupleStorageSubBlockDescription::PACKED_ROW_STORE);

  for (size_t thread_num = 0;
       thread_num < tuple_stores_.size();
       ++thread_num) {
    if (thread_affinities_.empty()) {
      threads_.push_back(new query_execution_threads::FileBasedSelectionThread(this,
                                                                               thread_num));
    } else {
      threads_.push_back(new query_execution_threads::FileBasedSelectionThread(this,
                                                                               thread_num,
                                                                               thread_affinities_[thread_num]));
    }
  }
}

FileBasedSelectionQueryExecutor::~FileBasedSelectionQueryExecutor() {
  database_->dropRelationById(result_relation_->getID());
}

}  // namespace storage_explorer
}  // namespace quickstep
