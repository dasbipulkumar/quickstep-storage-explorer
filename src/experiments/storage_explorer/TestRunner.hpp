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

#ifndef QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_TEST_RUNNER_HPP_
#define QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_TEST_RUNNER_HPP_

#include <cstddef>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "experiments/storage_explorer/Timer.hpp"
#include "expressions/Predicate.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

class CatalogRelation;
class IndexSubBlock;
class StorageManager;
class TupleStorageSubBlock;

namespace storage_explorer {

class DataGenerator;

/**
 * @brief Class which runs a series of tests and reports results. Also see
 *        QueryExecutor, which does an individual test run.
 **/
class TestRunner {
 public:
  TestRunner(const CatalogRelation &relation,
             const DataGenerator &generator,
             const attribute_id select_column,
             const int use_index,  // -1 indicates no index
             const bool sort_matches,
             const float selectivity,
             const std::vector<int> &thread_affinities);

  virtual ~TestRunner() {
  }

  /**
   * @brief Run this TestRunner's test the specified number of times and
   *        collect statistics.
   *
   * @param num_runs The number of experimental runs to perform.
   * @param measure_cache_misses If true, also measure system-wide L2 and L3
   *        cache miss counts during query execution.
   **/
  void doRuns(const std::size_t num_runs, const bool measure_cache_misses);

  /**
   * @brief Get the mean of the run times for experimental runs conducted by
   *        this TestRunner.
   *
   * @return The mean run time in seconds.
   **/
  double getRunTimeMean() const;

  /**
   * @brief Get the standard deviation of the run times for experimental runs
   *        conducted by this TestRunner.
   *
   * @return The standard deviation of run times in seconds.
   **/
  double getRunTimeStdDev() const;

  /**
   * @brief Get the coefficient of variation of the run times for experimental
   *        runs conducted by this runner.
   *
   * @return The unitless coefficient of variation for run times.
   **/
  double getRunTimeCoV() const {
    return getRunTimeStdDev() / getRunTimeMean();
  }

  /**
   * @brief Get the mean count of L2 misses for experimental runs conducted by
   *        this TestRunner.
   *
   * @return The mean number of L2 misses.
   **/
  double getL2MissMean() const;

  /**
   * @brief Get the standard deviation of L2 miss counts for experimental runs
   *        conducted by this TestRunner.
   *
   * @return The standard deviation of L2 misses.
   **/
  double getL2MissStdDev() const;

  /**
   * @brief Get the coefficient of variation of L2 miss counts for experimental
   *        runs conducted by this runner.
   *
   * @return The unitless coefficient of variation for L2 miss counts.
   **/
  double getL2MissCoV() const {
    return getL2MissStdDev() / getL2MissMean();
  }

  /**
   * @brief Get the mean count of L3 misses for experimental runs conducted by
   *        this TestRunner.
   *
   * @return The mean number of L3 misses.
   **/
  double getL3MissMean() const;

  /**
   * @brief Get the standard deviation of L3 miss counts for experimental runs
   *        conducted by this TestRunner.
   *
   * @return The standard deviation of L3 misses.
   **/
  double getL3MissStdDev() const;

  /**
   * @brief Get the coefficient of variation of L3 miss counts for experimental
   *        runs conducted by this runner.
   *
   * @return The unitless coefficient of variation for L3 miss counts.
   **/
  double getL3MissCoV() const {
    return getL3MissStdDev() / getL3MissMean();
  }

 protected:
  virtual Timer::RunStats runOnce(const bool measure_cache_misses) = 0;

  const CatalogRelation &relation_;
  const attribute_id select_column_;
  const int use_index_;
  const bool sort_matches_;

  const std::vector<int> &thread_affinities_;

  StorageManager *storage_manager_;

  ScopedPtr<Predicate> predicate_;

 private:
  std::vector<Timer::RunStats> run_stats_;

  DISALLOW_COPY_AND_ASSIGN(TestRunner);
};

class BlockBasedTestRunner : public TestRunner {
 public:
  BlockBasedTestRunner(const CatalogRelation &relation,
                       const DataGenerator &generator,
                       const attribute_id select_column,
                       const int use_index,
                       const bool sort_matches_,
                       const float selectivity,
                       const std::vector<int> &thread_affinities,
                       const std::size_t num_threads,
                       StorageManager *storage_manager);

  virtual ~BlockBasedTestRunner() {
  }

 protected:
  const std::size_t num_threads_;
  StorageManager *storage_manager_;

 private:
  DISALLOW_COPY_AND_ASSIGN(BlockBasedTestRunner);
};

class BlockBasedPredicateEvaluationTestRunner : public BlockBasedTestRunner {
 public:
  BlockBasedPredicateEvaluationTestRunner(const CatalogRelation &relation,
                                          const DataGenerator &generator,
                                          const attribute_id select_column,
                                          const int use_index,
                                          const bool sort_matches_,
                                          const float selectivity,
                                          const std::vector<int> &thread_affinities,
                                          const std::size_t num_threads,
                                          StorageManager *storage_manager)
      : BlockBasedTestRunner(relation,
                             generator,
                             select_column,
                             use_index,
                             sort_matches_,
                             selectivity,
                             thread_affinities,
                             num_threads,
                             storage_manager) {
  }

  virtual ~BlockBasedPredicateEvaluationTestRunner() {
  }

 protected:
  virtual Timer::RunStats runOnce(const bool measure_cache_misses);

 private:
  DISALLOW_COPY_AND_ASSIGN(BlockBasedPredicateEvaluationTestRunner);
};

class PartitionedBlockBasedPredicateEvaluationTestRunner
    : public BlockBasedPredicateEvaluationTestRunner {
 public:
  PartitionedBlockBasedPredicateEvaluationTestRunner(
      const CatalogRelation &relation,
      const DataGenerator &generator,
      const attribute_id select_column,
      const int use_index,
      const bool sort_matches_,
      const float selectivity,
      const std::vector<int> &thread_affinities,
      const std::size_t num_threads,
      StorageManager *storage_manager,
      const std::vector<std::vector<block_id> > &partition_blocks);

  virtual ~PartitionedBlockBasedPredicateEvaluationTestRunner() {
  }

 protected:
  Timer::RunStats runOnce(const bool measure_cache_misses);

 private:
  std::vector<block_id> relevant_partition_blocks_;

  DISALLOW_COPY_AND_ASSIGN(PartitionedBlockBasedPredicateEvaluationTestRunner);
};

class BlockBasedSelectionTestRunner : public BlockBasedTestRunner {
 public:
  BlockBasedSelectionTestRunner(const CatalogRelation &relation,
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
                                CatalogDatabase *database)
      : BlockBasedTestRunner(relation,
                             generator,
                             select_column,
                             use_index,
                             sort_matches,
                             selectivity,
                             thread_affinities,
                             num_threads,
                             storage_manager),
        projection_attributes_num_(projection_attributes_num),
        result_block_size_slots_(result_block_size_slots),
        database_(database) {
  }

  virtual ~BlockBasedSelectionTestRunner() {
  }

 protected:
  virtual Timer::RunStats runOnce(const bool measure_cache_misses);

  const attribute_id projection_attributes_num_;
  const std::size_t result_block_size_slots_;

  CatalogDatabase *database_;

 private:
  DISALLOW_COPY_AND_ASSIGN(BlockBasedSelectionTestRunner);
};

class PartitionedBlockBasedSelectionTestRunner : public BlockBasedSelectionTestRunner {
 public:
  PartitionedBlockBasedSelectionTestRunner(const CatalogRelation &relation,
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
                                           const std::vector<std::vector<block_id> > &partition_blocks);

  virtual ~PartitionedBlockBasedSelectionTestRunner() {
  }

 protected:
  Timer::RunStats runOnce(const bool measure_cache_misses);

 private:
  std::vector<block_id> relevant_partition_blocks_;

  DISALLOW_COPY_AND_ASSIGN(PartitionedBlockBasedSelectionTestRunner);
};

class FileBasedTestRunner : public TestRunner {
 public:
  FileBasedTestRunner(const CatalogRelation &relation,
                      const DataGenerator &generator,
                      const attribute_id select_column,
                      const int use_index,
                      const bool sort_matches,
                      const float selectivity,
                      const std::vector<int> &thread_affinities,
                      const std::vector<const TupleStorageSubBlock*> &tuple_stores,
                      const std::vector<std::vector<const IndexSubBlock*> > &indices);

  virtual ~FileBasedTestRunner() {
  }

 protected:
  const std::vector<const TupleStorageSubBlock*> &tuple_stores_;
  const std::vector<std::vector<const IndexSubBlock*> > &indices_;

 private:
  DISALLOW_COPY_AND_ASSIGN(FileBasedTestRunner);
};

class FileBasedPredicateEvaluationTestRunner : public FileBasedTestRunner {
 public:
  FileBasedPredicateEvaluationTestRunner(const CatalogRelation &relation,
                                         const DataGenerator &generator,
                                         const attribute_id select_column,
                                         const int use_index,
                                         const bool sort_matches,
                                         const float selectivity,
                                         const std::vector<int> &thread_affinities,
                                         const std::vector<const TupleStorageSubBlock*> &tuple_stores,
                                         const std::vector<std::vector<const IndexSubBlock*> > &indices)
      : FileBasedTestRunner(relation,
                            generator,
                            select_column,
                            use_index,
                            sort_matches,
                            selectivity,
                            thread_affinities,
                            tuple_stores,
                            indices) {
  }

  virtual ~FileBasedPredicateEvaluationTestRunner() {
  }

 protected:
  Timer::RunStats runOnce(const bool measure_cache_misses);

 private:
  DISALLOW_COPY_AND_ASSIGN(FileBasedPredicateEvaluationTestRunner);
};

class FileBasedSelectionTestRunner : public FileBasedTestRunner {
 public:
  FileBasedSelectionTestRunner(const CatalogRelation &relation,
                               const DataGenerator &generator,
                               const attribute_id select_column,
                               const int use_index,
                               const bool sort_matches,
                               const float selectivity,
                               const std::vector<int> &thread_affinities,
                               const std::vector<const TupleStorageSubBlock*> &tuple_stores,
                               const std::vector<std::vector<const IndexSubBlock*> > &indices,
                               const attribute_id projection_attributes_num,
                               const std::size_t result_buffer_size_bytes,
                               CatalogDatabase *database)
      : FileBasedTestRunner(relation,
                            generator,
                            select_column,
                            use_index,
                            sort_matches,
                            selectivity,
                            thread_affinities,
                            tuple_stores,
                            indices),
        projection_attributes_num_(projection_attributes_num),
        result_buffer_size_bytes_(result_buffer_size_bytes),
        database_(database) {
  }

  virtual ~FileBasedSelectionTestRunner() {
  }

 protected:
  Timer::RunStats runOnce(const bool measure_cache_misses);

 private:
  const attribute_id projection_attributes_num_;
  const std::size_t result_buffer_size_bytes_;

  CatalogDatabase *database_;

  DISALLOW_COPY_AND_ASSIGN(FileBasedSelectionTestRunner);
};

}  // namespace storage_explorer
}  // namespace quickstep

#endif  // QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_TEST_RUNNER_HPP_
