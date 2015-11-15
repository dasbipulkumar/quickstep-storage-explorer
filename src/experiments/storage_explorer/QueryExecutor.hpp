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

#ifndef QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_QUERY_EXECUTOR_HPP_
#define QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_QUERY_EXECUTOR_HPP_

#include <cstddef>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/TupleStorageSubBlock.hpp"
#include "threading/Mutex.hpp"
#include "threading/Thread.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"
#include "utility/ScopedBuffer.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

class CatalogDatabase;
class IndexSubBlock;
class InsertDestination;
class Predicate;
class StorageBlock;
class StorageManager;
class TupleIdSequence;

namespace storage_explorer {

namespace query_execution_threads {
class BlockBasedPredicateEvaluationThread;
class BlockBasedSelectionThread;
class FileBasedPredicateEvaluationThread;
class FileBasedSelectionThread;
}

/**
 * @brief Object which executes a test query once.
 **/
class QueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   **/
  QueryExecutor(const CatalogRelation &relation,
                const Predicate &predicate,
                const attribute_id predicate_attribute_id,
                const std::vector<int> &thread_affinities)
      : relation_(relation),
        predicate_(predicate),
        predicate_attribute_id_(predicate_attribute_id),
        thread_affinities_(thread_affinities),
        use_index_(false),
        use_index_num_(0),
        sort_index_matches_(false) {
  }

  virtual ~QueryExecutor() {
  }

  /**
   * @brief Run the query on the base table (don't use an index).
   **/
  void executeOnTupleStore() {
    use_index_ = false;

    runThreads();
  }

  /**
   * @brief Run the query using an index.
   *
   * @param index_num The number of the index to use for predicate evaluation.
   * @param sort_matches If true, the sequence of matching tuple IDs will be
   *        sorted into order before projection is performed.
   **/
  void executeWithIndex(const std::size_t index_num, const bool sort_matches) {
    use_index_ = true;
    use_index_num_ = index_num;
    sort_index_matches_ = sort_matches;

    runThreads();
  }

 protected:
  TupleIdSequence* evaluatePredicateOnTupleStore(const TupleStorageSubBlock &tuple_store) const;
  TupleIdSequence* evaluatePredicateWithIndex(const IndexSubBlock &index,
                                              const TupleStorageSubBlock &tuple_store) const;
  TupleIdSequence* evaluatePredicateOnBlock(const StorageBlock &block) const;

  const CatalogRelation &relation_;
  const Predicate &predicate_;
  const attribute_id predicate_attribute_id_;

  const std::vector<int> &thread_affinities_;
  PtrVector<Thread> threads_;

  bool use_index_;
  std::size_t use_index_num_;
  bool sort_index_matches_;

 private:
  inline void runThreads() {
    for (PtrVector<Thread>::iterator thread_it = threads_.begin();
         thread_it != threads_.end();
         ++thread_it) {
      thread_it->start();
    }

    for (PtrVector<Thread>::iterator thread_it = threads_.begin();
         thread_it != threads_.end();
         ++thread_it) {
      thread_it->join();
    }
  }

  DISALLOW_COPY_AND_ASSIGN(QueryExecutor);
};

/**
 * @brief Intermediate class containing common functionality for queries in
 *        block-based organization.
 **/
class BlockBasedQueryExecutor : public QueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   * @param num_threads The number of execution threads to use.
   * @param storage_manager The global StorageManager instance.
   **/
  BlockBasedQueryExecutor(const CatalogRelation &relation,
                          const Predicate &predicate,
                          const attribute_id predicate_attribute_id,
                          const std::vector<int> &thread_affinities,
                          const std::size_t num_threads,
                          StorageManager *storage_manager)
      : QueryExecutor(relation, predicate, predicate_attribute_id, thread_affinities),
        storage_manager_(storage_manager) {
    if (num_threads == 0) {
      FATAL_ERROR("Attempted to construct BlockBasedQueryExecutor with num_threads = 0");
    }
  }

  virtual ~BlockBasedQueryExecutor() {
  }

 protected:
  const IndexSubBlock& getIndex(const StorageBlock &block,
                                const std::size_t index_num) const;

  StorageManager *storage_manager_;

  Mutex mutex_;

  virtual block_id getNextInputBlock() = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(BlockBasedQueryExecutor);
};

/**
 * @brief Block-based QueryExecutor which only evaluates a predicate and does
 *        not actually perform a projection.
 **/
class BlockBasedPredicateEvaluationQueryExecutor : public BlockBasedQueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   * @param num_threads The number of execution threads to use.
   * @param storage_manager The global StorageManager instance.
   **/
  BlockBasedPredicateEvaluationQueryExecutor(const CatalogRelation &relation,
                                             const Predicate &predicate,
                                             const attribute_id predicate_attribute_id,
                                             const std::vector<int> &thread_affinities,
                                             const std::size_t num_threads,
                                             StorageManager *storage_manager);

  virtual ~BlockBasedPredicateEvaluationQueryExecutor() {
  }

 protected:
  virtual block_id getNextInputBlock();

 private:
  CatalogRelation::const_iterator_blocks next_block_iterator_;

  friend class query_execution_threads::BlockBasedPredicateEvaluationThread;

  DISALLOW_COPY_AND_ASSIGN(BlockBasedPredicateEvaluationQueryExecutor);
};

/**
 * @brief Block-based QueryExecutor which only evaluates a predicate and does
 *        not actually perform a projection. Partitioned version.
 **/
class PartitionedBlockBasedPredicateEvaluationQueryExecutor
    : public BlockBasedPredicateEvaluationQueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   * @param num_threads The number of execution threads to use.
   * @param storage_manager The global StorageManager instance.
   * @param partition_blocks The IDs of blocks in the relevant partition(s) for
   *        this query.
   **/
  PartitionedBlockBasedPredicateEvaluationQueryExecutor(const CatalogRelation &relation,
                                                        const Predicate &predicate,
                                                        const attribute_id predicate_attribute_id,
                                                        const std::vector<int> &thread_affinities,
                                                        const std::size_t num_threads,
                                                        StorageManager *storage_manager,
                                                        const std::vector<block_id> &partition_blocks)
    : BlockBasedPredicateEvaluationQueryExecutor(relation,
                                                 predicate,
                                                 predicate_attribute_id,
                                                 thread_affinities,
                                                 num_threads,
                                                 storage_manager),
      partition_blocks_(partition_blocks),
      partition_blocks_iterator_(partition_blocks_.begin()) {
  }

  virtual ~PartitionedBlockBasedPredicateEvaluationQueryExecutor() {
  }

 protected:
  block_id getNextInputBlock();

 private:
  const std::vector<block_id> partition_blocks_;
  std::vector<block_id>::const_iterator partition_blocks_iterator_;

  DISALLOW_COPY_AND_ASSIGN(PartitionedBlockBasedPredicateEvaluationQueryExecutor);
};

/**
 * @brief Block-based QueryExecutor for a full selection-projection query.
 *
 * @note Running this creates temporary result blocks in a temporary relation
 *       to hold query output. These are deleted when this QueryExecutor is
 *       destroyed.
 **/
class BlockBasedSelectionQueryExecutor : public BlockBasedQueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   * @param num_threads The number of execution threads to use.
   * @param storage_manager The global StorageManager instance.
   * @param projection_attributes_num The number of attributes to project (will
   *        be randomly chosen from attributes in relation).
   * @param result_block_size_slots The number of slots in the StorageManager
   *        each block in the query output will take up.
   * @param database A CatalogDatabase to create the temporary result relation
   *        in.
   **/
  BlockBasedSelectionQueryExecutor(const CatalogRelation &relation,
                                   const Predicate &predicate,
                                   const attribute_id predicate_attribute_id,
                                   const std::vector<int> &thread_affinities,
                                   const std::size_t num_threads,
                                   StorageManager *storage_manager,
                                   const attribute_id projection_attributes_num,
                                   const std::size_t result_block_size_slots,
                                   CatalogDatabase *database);

  // Drops temporary result relation.
  virtual ~BlockBasedSelectionQueryExecutor();

 protected:
  virtual block_id getNextInputBlock();

  void doProjection(const StorageBlock &block, TupleIdSequence *matches);

  std::vector<attribute_id> projection_attributes_;

  CatalogDatabase *database_;

  CatalogRelation *result_relation_;
  ScopedPtr<StorageBlockLayout> result_layout_;
  ScopedPtr<InsertDestination> result_destination_;

 private:
  CatalogRelation::const_iterator_blocks next_block_iterator_;

  friend class query_execution_threads::BlockBasedSelectionThread;

  DISALLOW_COPY_AND_ASSIGN(BlockBasedSelectionQueryExecutor);
};

/**
 * @brief Block-based QueryExecutor for a full selection-projection query.
 *        Partitioned version.
 *
 * @note Running this creates temporary result blocks in a temporary relation
 *       to hold query output. These are deleted when this QueryExecutor is
 *       destroyed.
 **/
class PartitionedBlockBasedSelectionQueryExecutor : public BlockBasedSelectionQueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   * @param num_threads The number of execution threads to use.
   * @param storage_manager The global StorageManager instance.
   * @param projection_attributes_num The number of attributes to project (will
   *        be randomly chosen from attributes in relation).
   * @param result_block_size_slots The number of slots in the StorageManager
   *        each block in the query output will take up.
   * @param database A CatalogDatabase to create the temporary result relation
   *        in.
   * @param partition_blocks The IDs of blocks in the relevant partition(s) for
   *        this query.
   **/
  PartitionedBlockBasedSelectionQueryExecutor(const CatalogRelation &relation,
                                              const Predicate &predicate,
                                              const attribute_id predicate_attribute_id,
                                              const std::vector<int> &thread_affinities,
                                              const std::size_t num_threads,
                                              StorageManager *storage_manager,
                                              const attribute_id projection_attributes_num,
                                              const std::size_t result_block_size_slots,
                                              CatalogDatabase *database,
                                              const std::vector<block_id> &partition_blocks)
    : BlockBasedSelectionQueryExecutor(relation,
                                       predicate,
                                       predicate_attribute_id,
                                       thread_affinities,
                                       num_threads,
                                       storage_manager,
                                       projection_attributes_num,
                                       result_block_size_slots,
                                       database),
      partition_blocks_(partition_blocks),
      partition_blocks_iterator_(partition_blocks_.begin()) {
  }

  virtual ~PartitionedBlockBasedSelectionQueryExecutor() {
  }

 protected:
  block_id getNextInputBlock();

 private:
  const std::vector<block_id> partition_blocks_;
  std::vector<block_id>::const_iterator partition_blocks_iterator_;

  DISALLOW_COPY_AND_ASSIGN(PartitionedBlockBasedSelectionQueryExecutor);
};

/**
 * @brief Intermediate class containing common functionality for queries in
 *        file-based organization.
 **/
class FileBasedQueryExecutor : public QueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   * @param tuple_stores The base table files.
   * @param indices The index files (first dimension is index number, second is
   *        partition).
   **/
  FileBasedQueryExecutor(const CatalogRelation &relation,
                         const Predicate &predicate,
                         const attribute_id predicate_attribute_id,
                         const std::vector<int> &thread_affinities,
                         const std::vector<const TupleStorageSubBlock*> &tuple_stores,
                         const std::vector<std::vector<const IndexSubBlock*> > &indices)
      : QueryExecutor(relation, predicate, predicate_attribute_id, thread_affinities),
        tuple_stores_(tuple_stores),
        indices_(indices) {
  }

  virtual ~FileBasedQueryExecutor() {
  }

 protected:
  const std::vector<const TupleStorageSubBlock*> &tuple_stores_;
  const std::vector<std::vector<const IndexSubBlock*> > &indices_;

 private:
  DISALLOW_COPY_AND_ASSIGN(FileBasedQueryExecutor);
};

/**
 * @brief File-based QueryExecutor which only evaluates a predicate and does
 *        not actually perform a projection.
 **/
class FileBasedPredicateEvaluationQueryExecutor : public FileBasedQueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   * @param tuple_stores The base table files.
   * @param indices The index files (first dimension is index number, second is
   *        partition).
   **/
  FileBasedPredicateEvaluationQueryExecutor(const CatalogRelation &relation,
                                            const Predicate &predicate,
                                            const attribute_id predicate_attribute_id,
                                            const std::vector<int> &thread_affinities,
                                            const std::vector<const TupleStorageSubBlock*> &tuple_stores,
                                            const std::vector<std::vector<const IndexSubBlock*> > &indices);

 private:
  friend class query_execution_threads::FileBasedPredicateEvaluationThread;

  DISALLOW_COPY_AND_ASSIGN(FileBasedPredicateEvaluationQueryExecutor);
};

/**
 * @brief File-based QueryExecutor for a full selection-projection query.
 *
 * @note Running this creates temporary result files in a temporary relation
 *       to hold query output. These are deleted when this QueryExecutor is
 *       destroyed.
 **/
class FileBasedSelectionQueryExecutor : public FileBasedQueryExecutor {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation the query is over.
   * @param predicate The predicate for the selection query.
   * @param predicate_attribute_id The ID of the attribute the predicate
   *        selects on.
   * @param thread_affinities A sequence of CPU core IDs to pin each individual
   *        execution thread to. If empty, threads will not be pinned.
   * @param tuple_stores The base table files.
   * @param indices The index files (first dimension is index number, second is
   *        partition).
   * @param projection_attributes_num The number of attributes to project (will
   *        be randomly chosen from attributes in relation).
   * @param result_buffer_size_bytes The size, in bytes, of the result file to
   *        create for each execution thread.
   * @param database A CatalogDatabase to create the temporary result relation
   *        in.
   **/
  FileBasedSelectionQueryExecutor(const CatalogRelation &relation,
                                  const Predicate &predicate,
                                  const attribute_id predicate_attribute_id,
                                  const std::vector<int> &thread_affinities,
                                  const std::vector<const TupleStorageSubBlock*> &tuple_stores,
                                  const std::vector<std::vector<const IndexSubBlock*> > &indices,
                                  const attribute_id projection_attributes_num,
                                  const std::size_t result_buffer_size_bytes,
                                  CatalogDatabase *database);

  // Drops temporary result relation.
  ~FileBasedSelectionQueryExecutor();

 private:
  std::vector<attribute_id> projection_attributes_;

  CatalogDatabase *database_;

  CatalogRelation *result_relation_;
  const std::size_t result_buffer_size_bytes_;
  TupleStorageSubBlockDescription result_store_description_;

  friend class query_execution_threads::FileBasedSelectionThread;

  DISALLOW_COPY_AND_ASSIGN(FileBasedSelectionQueryExecutor);
};

}  // namespace storage_explorer
}  // namespace quickstep

#endif  // QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_QUERY_EXECUTOR_HPP_
