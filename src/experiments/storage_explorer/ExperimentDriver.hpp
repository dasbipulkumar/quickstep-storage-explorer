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

#ifndef QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_EXPERIMENT_DRIVER_HPP_
#define QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_EXPERIMENT_DRIVER_HPP_

#include <vector>

#include "catalog/Catalog.hpp"
#include "experiments/storage_explorer/DataGenerator.hpp"
#include "experiments/storage_explorer/ExperimentConfiguration.hpp"
#include "storage/IndexSubBlock.hpp"
#include "storage/StorageManager.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"
#include "utility/ScopedBuffer.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

class CatalogDatabase;
class CatalogRelation;

namespace storage_explorer {

class TestRunner;

/**
 * @brief Main program driver which encapsulates generating data and running
 *        all experiments.
 **/
class ExperimentDriver {
 public:
  virtual ~ExperimentDriver() {
  }

  /**
   * @brief Create a new driver based on the specified configuration.
   *
   * @param configuration Configuration which specifies the experiments to be
   *        run.
   * @return A new ExperimentDriver based on configuration.
   **/
  static ExperimentDriver* CreateDriverForConfiguration(
      const ExperimentConfiguration &configuration);

  /**
   * @brief Set up this ExperimentDriver by creating a DataGenerator and the
   *        test relation.
   **/
  void initialize();

  /**
   * @brief Generate all random test data and log progress to STDOUT.
   **/
  virtual void generateData() = 0;

  /**
   * @brief Run all experiments specified in configuration and log results to
   *        STDOUT.
   **/
  virtual void runExperiments() = 0;

 protected:
  explicit ExperimentDriver(const ExperimentConfiguration &configuration)
      : configuration_(configuration),
        relation_(NULL) {
  }

  void logTestParameters(const ExperimentConfiguration::TestParameters &params) const;
  void logTestResults(const TestRunner &runner) const;

  const ExperimentConfiguration &configuration_;
  ScopedPtr<DataGenerator> data_generator_;
  Catalog catalog_;
  CatalogDatabase *database_;
  CatalogRelation *relation_;

 private:
  DISALLOW_COPY_AND_ASSIGN(ExperimentDriver);
};

/**
 * @brief Implementation of ExperimentDriver for block-based organization.
 **/
class BlockBasedExperimentDriver : public ExperimentDriver {
 public:
  void generateData();
  void runExperiments();

 private:
  explicit BlockBasedExperimentDriver(const ExperimentConfiguration &configuration)
      : ExperimentDriver(configuration) {
  }

  StorageManager storage_manager_;

  friend class ExperimentDriver;

  DISALLOW_COPY_AND_ASSIGN(BlockBasedExperimentDriver);
};

/**
 * @brief Implementation of ExperimentDriver for file-based organization.
 **/
class FileBasedExperimentDriver : public ExperimentDriver {
 public:
  void generateData();
  void runExperiments();

 private:
  explicit FileBasedExperimentDriver(const ExperimentConfiguration &configuration)
      : ExperimentDriver(configuration) {
  }

  PtrVector<ScopedBuffer> tuple_store_buffers_;
  PtrVector<ScopedBuffer> index_buffers_;
  PtrVector<TupleStorageSubBlock> tuple_stores_;
  PtrVector<IndexSubBlock> indices_;

  std::vector<const TupleStorageSubBlock*> tuple_store_ptrs_;
  std::vector<std::vector<const IndexSubBlock*> > index_ptrs_;

  friend class ExperimentDriver;

  DISALLOW_COPY_AND_ASSIGN(FileBasedExperimentDriver);
};

}  // storage_explorer
}  // namespace quickstep

#endif  // QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_EXPERIMENT_DRIVER_HPP_
