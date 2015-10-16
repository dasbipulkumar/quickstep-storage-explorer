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

#include "experiments/storage_explorer/TestRunner.hpp"

#include <cmath>
#include <cstddef>
#include <vector>

#include "experiments/storage_explorer/DataGenerator.hpp"
#include "experiments/storage_explorer/QueryExecutor.hpp"
#include "experiments/storage_explorer/Timer.hpp"

using std::ceil;
using std::size_t;
using std::sqrt;
using std::vector;

namespace quickstep {
namespace storage_explorer {

TestRunner::TestRunner(const CatalogRelation &relation,
                       const DataGenerator &generator,
                       const attribute_id select_column,
                       const int use_index,
                       const bool sort_matches,
                       const float selectivity,
                       const std::vector<int> &thread_affinities)
    : relation_(relation),
      select_column_(select_column),
      use_index_(use_index),
      sort_matches_(sort_matches),
      thread_affinities_(thread_affinities),
      predicate_(generator.generatePredicate(relation, select_column, selectivity)) {
}

void TestRunner::doRuns(const size_t num_runs, const bool measure_cache_misses) {
  for (size_t run = 0; run < num_runs; ++run) {
    run_stats_.push_back(runOnce(measure_cache_misses));
  }
}

double TestRunner::getRunTimeMean() const {
  if (run_stats_.empty()) {
    return 0;
  }

  double sum = 0;
  for (vector<Timer::RunStats>::const_iterator it = run_stats_.begin();
       it != run_stats_.end();
       ++it) {
    sum += it->elapsed_time;
  }

  return sum / static_cast<double>(run_stats_.size());
}

double TestRunner::getRunTimeStdDev() const {
  double mean = getRunTimeMean();
  double sum_of_variances = 0;
  for (vector<Timer::RunStats>::const_iterator it = run_stats_.begin();
       it != run_stats_.end();
       ++it) {
    double diff = mean - it->elapsed_time;
    sum_of_variances += (diff * diff);
  }

  return sqrt(sum_of_variances / static_cast<double>(run_stats_.size()));
}

double TestRunner::getL2MissMean() const {
  if (run_stats_.empty()) {
    return 0;
  }

  double sum = 0;
  for (vector<Timer::RunStats>::const_iterator it = run_stats_.begin();
       it != run_stats_.end();
       ++it) {
    sum += it->l2_misses;
  }

  return sum / static_cast<double>(run_stats_.size());
}

double TestRunner::getL2MissStdDev() const {
  double mean = getL2MissMean();
  double sum_of_variances = 0;
  for (vector<Timer::RunStats>::const_iterator it = run_stats_.begin();
       it != run_stats_.end();
       ++it) {
    double diff = mean - static_cast<double>(it->l2_misses);
    sum_of_variances += (diff * diff);
  }

  return sqrt(sum_of_variances / static_cast<double>(run_stats_.size()));
}

double TestRunner::getL3MissMean() const {
  if (run_stats_.empty()) {
    return 0;
  }

  double sum = 0;
  for (vector<Timer::RunStats>::const_iterator it = run_stats_.begin();
       it != run_stats_.end();
       ++it) {
    sum += it->l3_misses;
  }

  return sum / static_cast<double>(run_stats_.size());
}

double TestRunner::getL3MissStdDev() const {
  double mean = getL3MissMean();
  double sum_of_variances = 0;
  for (vector<Timer::RunStats>::const_iterator it = run_stats_.begin();
       it != run_stats_.end();
       ++it) {
    double diff = mean - static_cast<double>(it->l3_misses);
    sum_of_variances += (diff * diff);
  }

  return sqrt(sum_of_variances / static_cast<double>(run_stats_.size()));
}

BlockBasedTestRunner::BlockBasedTestRunner(
    const CatalogRelation &relation,
    const DataGenerator &generator,
    const attribute_id select_column,
    const int use_index,
    const bool sort_matches,
    const float selectivity,
    const std::vector<int> &thread_affinities,
    const std::size_t num_threads,
    StorageManager *storage_manager)
    : TestRunner(relation,
                 generator,
                 select_column,
                 use_index,
                 sort_matches,
                 selectivity,
                 thread_affinities),
      num_threads_(num_threads),
      storage_manager_(storage_manager) {
}

Timer::RunStats BlockBasedPredicateEvaluationTestRunner::runOnce(const bool measure_cache_misses) {
  BlockBasedPredicateEvaluationQueryExecutor executor(relation_,
                                                      *predicate_,
                                                      select_column_,
                                                      thread_affinities_,
                                                      num_threads_,
                                                      storage_manager_);

  Timer timer(measure_cache_misses);
  timer.start();
  if (use_index_ >= 0) {
    executor.executeWithIndex(use_index_, sort_matches_);
  } else {
    executor.executeOnTupleStore();
  }
  timer.stop();
  return timer.getRunStats();
}

PartitionedBlockBasedPredicateEvaluationTestRunner::PartitionedBlockBasedPredicateEvaluationTestRunner(
    const CatalogRelation &relation,
    const DataGenerator &generator,
    const attribute_id select_column,
    const int use_index,
    const bool sort_matches_,
    const float selectivity,
    const std::vector<int> &thread_affinities,
    const std::size_t num_threads,
    StorageManager *storage_manager,
    const std::vector<std::vector<block_id> > &partition_blocks)
    : BlockBasedPredicateEvaluationTestRunner(relation,
                                              generator,
                                              select_column,
                                              use_index,
                                              sort_matches_,
                                              selectivity,
                                              thread_affinities,
                                              num_threads,
                                              storage_manager) {
  size_t min_partition = partition_blocks.size()
                         - ceil(selectivity * static_cast<float>(partition_blocks.size()));
  for (vector<vector<block_id> >::const_iterator partition_it = partition_blocks.begin() + min_partition;
       partition_it != partition_blocks.end();
       ++partition_it) {
    relevant_partition_blocks_.insert(relevant_partition_blocks_.end(),
                                      partition_it->begin(),
                                      partition_it->end());
  }
}

Timer::RunStats PartitionedBlockBasedPredicateEvaluationTestRunner::runOnce(const bool measure_cache_misses) {
  PartitionedBlockBasedPredicateEvaluationQueryExecutor executor(relation_,
                                                                 *predicate_,
                                                                 select_column_,
                                                                 thread_affinities_,
                                                                 num_threads_,
                                                                 storage_manager_,
                                                                 relevant_partition_blocks_);

  Timer timer(measure_cache_misses);
  timer.start();
  if (use_index_ >= 0) {
    executor.executeWithIndex(use_index_, sort_matches_);
  } else {
    executor.executeOnTupleStore();
  }
  timer.stop();
  return timer.getRunStats();
}

Timer::RunStats BlockBasedSelectionTestRunner::runOnce(const bool measure_cache_misses) {
  BlockBasedSelectionQueryExecutor executor(relation_,
                                            *predicate_,
                                            select_column_,
                                            thread_affinities_,
                                            num_threads_,
                                            storage_manager_,
                                            projection_attributes_num_,
                                            result_block_size_slots_,
                                            database_);

  Timer timer(measure_cache_misses);
  timer.start();
  if (use_index_ >= 0) {
    executor.executeWithIndex(use_index_, sort_matches_);
  } else {
    executor.executeOnTupleStore();
  }
  timer.stop();
  return timer.getRunStats();
}

PartitionedBlockBasedSelectionTestRunner::PartitionedBlockBasedSelectionTestRunner(
    const CatalogRelation &relation,
    const DataGenerator &generator,
    const attribute_id select_column,
    const int use_index,
    const bool sort_matches,
    const float selectivity,
    const std::vector<int> &thread_affinities,
    const std::size_t num_threads,
    StorageManager *storage_manager,
    const attribute_id projection_attributes_num,
    const std::size_t result_block_size_slots,
    CatalogDatabase *database,
    const std::vector<std::vector<block_id> > &partition_blocks)
    : BlockBasedSelectionTestRunner(relation,
                                    generator,
                                    select_column,
                                    use_index,
                                    sort_matches,
                                    selectivity,
                                    thread_affinities,
                                    num_threads,
                                    storage_manager,
                                    projection_attributes_num,
                                    result_block_size_slots,
                                    database) {
  size_t min_partition = partition_blocks.size()
                         - ceil(selectivity * static_cast<float>(partition_blocks.size()));
  for (vector<vector<block_id> >::const_iterator partition_it = partition_blocks.begin() + min_partition;
       partition_it != partition_blocks.end();
       ++partition_it) {
    relevant_partition_blocks_.insert(relevant_partition_blocks_.end(),
                                      partition_it->begin(),
                                      partition_it->end());
  }
}

Timer::RunStats PartitionedBlockBasedSelectionTestRunner::runOnce(const bool measure_cache_misses) {
  PartitionedBlockBasedSelectionQueryExecutor executor(relation_,
                                                       *predicate_,
                                                       select_column_,
                                                       thread_affinities_,
                                                       num_threads_,
                                                       storage_manager_,
                                                       projection_attributes_num_,
                                                       result_block_size_slots_,
                                                       database_,
                                                       relevant_partition_blocks_);

  Timer timer(measure_cache_misses);
  timer.start();
  if (use_index_ >= 0) {
    executor.executeWithIndex(use_index_, sort_matches_);
  } else {
    executor.executeOnTupleStore();
  }
  timer.stop();
  return timer.getRunStats();
}

FileBasedTestRunner::FileBasedTestRunner(const CatalogRelation &relation,
                                         const DataGenerator &generator,
                                         const attribute_id select_column,
                                         const int use_index,
                                         const bool sort_matches,
                                         const float selectivity,
                                         const std::vector<int> &thread_affinities,
                                         const std::vector<const TupleStorageSubBlock*> &tuple_stores,
                                         const std::vector<std::vector<const IndexSubBlock*> > &indices)
    : TestRunner(relation,
                 generator,
                 select_column,
                 use_index,
                 sort_matches,
                 selectivity,
                 thread_affinities),
      tuple_stores_(tuple_stores),
      indices_(indices) {
}

Timer::RunStats FileBasedPredicateEvaluationTestRunner::runOnce(const bool measure_cache_misses) {
  FileBasedPredicateEvaluationQueryExecutor executor(relation_,
                                                     *predicate_,
                                                     select_column_,
                                                     thread_affinities_,
                                                     tuple_stores_,
                                                     indices_);

  Timer timer(measure_cache_misses);
  timer.start();
  if (use_index_ >= 0) {
    executor.executeWithIndex(use_index_, sort_matches_);
  } else {
    executor.executeOnTupleStore();
  }
  timer.stop();
  return timer.getRunStats();
}

Timer::RunStats FileBasedSelectionTestRunner::runOnce(const bool measure_cache_misses) {
  FileBasedSelectionQueryExecutor executor(relation_,
                                           *predicate_,
                                           select_column_,
                                           thread_affinities_,
                                           tuple_stores_,
                                           indices_,
                                           projection_attributes_num_,
                                           result_buffer_size_bytes_,
                                           database_);

  Timer timer(measure_cache_misses);
  timer.start();
  if (use_index_ >= 0) {
    executor.executeWithIndex(use_index_, sort_matches_);
  } else {
    executor.executeOnTupleStore();
  }
  timer.stop();
  return timer.getRunStats();
}

}  // namespace storage_explorer
}  // namespace quickstep
