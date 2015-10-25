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

#ifndef QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_EXPERIMENT_CONFIGURATION_HPP_
#define QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_EXPERIMENT_CONFIGURATION_HPP_

#include <cstddef>
#include <iostream>
#include <vector>

#include "utility/Macros.hpp"

typedef struct cJSON cJSON;

namespace quickstep {
namespace storage_explorer {

class DataGenerator;
class ExperimentDriver;
class BlockBasedExperimentDriver;
class FileBasedExperimentDriver;

/**
 * @brief Object representing the complete configuration of experiments to run,
 *        specified by a JSON configuration file.
 **/
class ExperimentConfiguration {
 public:
  /**
   * @brief The possible choices of test tables to use.
   **/
  enum TestTable {
    kNarrowE,
    kNarrowU,
    kWideE,
    kStrings
  };

  /**
   * @brief Parameters for an individual test in a series of experiments.
   **/
  struct TestParameters {
    int predicate_column;
    bool use_index;
    bool sort_matches;
    double selectivity;
    std::size_t projection_width;
  };

  virtual ~ExperimentConfiguration() {
  }

  /**
   * @brief Load a configuration from a parsed JSON file.
   *
   * @param json The parsed JSON configuration file.
   * @return A new ExperimentConfiguration based on json.
   **/
  static ExperimentConfiguration* LoadFromJSON(cJSON *json);

  /**
   * @brief Print out this configuration in human-readable format.
   *
   * @param output The stream to print to.
   **/
  void logConfiguration(std::ostream *output) const;

  /**
   * @brief Determine whether this configuration specifies that cache misses
   *        should be measured using the Intel PCM library.
   *
   * @return Whether cache misses should be measured.
   **/
  bool measureCacheMisses() const {
    return measure_cache_misses_;
  }

  /**
   * @brief Determine whether this configuration specifies a block-based or
   *        file-based organization.
   *
   * @return True if using blocks, false if using files.
   **/
  virtual bool isBlockBased() const = 0;

 protected:
  ExperimentConfiguration() {
  }

  virtual void loadAdditionalConfigurationFromJSON(cJSON *json) {
  }

  virtual void logAdditionalConfiguration(std::ostream *output) const = 0;

  TestTable table_choice_;
  std::size_t num_tuples_;

  bool use_column_store_;
  int column_store_sort_column_;
  bool use_compression_;
  bool use_index_;
  int index_column_;
  bool use_bloom_filter_;

  std::size_t num_runs_;
  bool measure_cache_misses_;
  std::size_t num_threads_;
  std::vector<int> thread_affinities_;

  std::vector<TestParameters> test_params_;

 private:
  void loadTestParametersFromJSON(cJSON *test_params_json);

  friend class ExperimentDriver;
  friend class BlockBasedExperimentDriver;
  friend class FileBasedExperimentDriver;

  DISALLOW_COPY_AND_ASSIGN(ExperimentConfiguration);
};

class BlockBasedExperimentConfiguration : public ExperimentConfiguration {
 public:
  virtual ~BlockBasedExperimentConfiguration() {
  }

  bool isBlockBased() const {
    return true;
  }

 protected:
  void loadAdditionalConfigurationFromJSON(cJSON *json);

  void logAdditionalConfiguration(std::ostream *output) const;

 private:
  BlockBasedExperimentConfiguration()
      : ExperimentConfiguration() {
  }

  std::size_t block_size_slots_;

  friend class ExperimentConfiguration;
  friend class ExperimentDriver;
  friend class BlockBasedExperimentDriver;

  DISALLOW_COPY_AND_ASSIGN(BlockBasedExperimentConfiguration);
};

class FileBasedExperimentConfiguration : public ExperimentConfiguration {
 public:
  virtual ~FileBasedExperimentConfiguration() {
  }

  bool isBlockBased() const {
    return false;
  }

 protected:
  void logAdditionalConfiguration(std::ostream *output) const;

 private:
  FileBasedExperimentConfiguration()
      : ExperimentConfiguration() {
  }

  friend class ExperimentConfiguration;
  friend class ExperimentDriver;
  friend class FileBasedExperimentDriver;

  DISALLOW_COPY_AND_ASSIGN(FileBasedExperimentConfiguration);
};

}  // namespace storage_explorer
}  // namespace quickstep

#endif  // QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_EXPERIMENT_CONFIGURATION_HPP_
