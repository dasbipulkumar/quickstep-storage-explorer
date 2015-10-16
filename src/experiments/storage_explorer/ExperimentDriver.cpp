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

#include "experiments/storage_explorer/ExperimentDriver.hpp"

#include <cstddef>
#include <iostream>
#include <vector>

#include "catalog/Catalog.hpp"
#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "experiments/storage_explorer/DataGenerator.hpp"
#include "experiments/storage_explorer/ExperimentConfiguration.hpp"
#include "experiments/storage_explorer/TestRunner.hpp"
#include "experiments/storage_explorer/Timer.hpp"
#include "storage/BasicColumnStoreTupleStorageSubBlock.hpp"
#include "storage/CSBTreeIndexSubBlock.hpp"
#include "storage/CompressedColumnStoreTupleStorageSubBlock.hpp"
#include "storage/CompressedPackedRowStoreTupleStorageSubBlock.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/PackedRowStoreTupleStorageSubBlock.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageConstants.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

using std::cout;
using std::size_t;
using std::vector;

namespace quickstep {
namespace storage_explorer {

ExperimentDriver* ExperimentDriver::CreateDriverForConfiguration(
    const ExperimentConfiguration &configuration) {
  if (configuration.isBlockBased()) {
    return new BlockBasedExperimentDriver(configuration);
  } else {
    return new FileBasedExperimentDriver(configuration);
  }
}

void ExperimentDriver::initialize() {
  database_ = new CatalogDatabase(&catalog_, "default");
  catalog_.addDatabase(database_);

  switch (configuration_.table_choice_) {
    case ExperimentConfiguration::kNarrowE:
      data_generator_.reset(new NarrowEDataGenerator());
      break;
    case ExperimentConfiguration::kNarrowU:
      data_generator_.reset(new NarrowUDataGenerator());
      break;
    case ExperimentConfiguration::kWideE:
      data_generator_.reset(new WideEDataGenerator());
      break;
    case ExperimentConfiguration::kStrings:
      data_generator_.reset(new StringsDataGenerator());
      break;
    default:
      FATAL_ERROR("Unrecognized table_choice_ in configuration.");
  }

  relation_ = data_generator_->generateRelation();
  database_->addRelation(relation_);
}

void ExperimentDriver::logTestParameters(
    const ExperimentConfiguration::TestParameters &params) const {
  cout << "===== TEST QUERY =====\n";
  cout << "Predicate: " << params.selectivity << " selectivity on column " << params.predicate_column << "\n";
  cout << "Projection Width: " << params.projection_width << " columns\n";
  if (params.use_index) {
    cout << "Using Index";
    if (params.sort_matches) {
      cout << " (Sorting Results Before Projection)";
    }
    cout << "\n";
  } else if (configuration_.use_column_store_
             && (configuration_.column_store_sort_column_ == params.predicate_column)) {
    cout << "Using Binary Search On Sort Column\n";
  } else {
    cout << "Using Scan\n";
  }
}

void ExperimentDriver::logTestResults(const TestRunner &runner) const {
  cout << "Execution Time (seconds):";
  cout << " Mean: " << runner.getRunTimeMean();
  cout << " StdDev: " << runner.getRunTimeStdDev();
  cout << " CoV: " << runner.getRunTimeCoV() << "\n";
  if (configuration_.measure_cache_misses_) {
    cout << "L2 Misses:";
    cout << " Mean: " << runner.getL2MissMean();
    cout << " StdDev: " << runner.getL2MissStdDev();
    cout << " CoV: " << runner.getL2MissCoV() << "\n";
    cout << "L3 Misses:";
    cout << " Mean: " << runner.getL3MissMean();
    cout << " StdDev: " << runner.getL3MissStdDev();
    cout << " CoV: " << runner.getL3MissCoV() << "\n";
  }
  cout << "\n";
  cout.flush();
}

void BlockBasedExperimentDriver::generateData() {
  vector<attribute_id> index_columns;
  if (configuration_.use_index_) {
    index_columns.push_back(configuration_.index_column_);
  }

  ScopedPtr<StorageBlockLayout> layout;
  if (configuration_.use_column_store_) {
    if (configuration_.use_compression_) {
      layout.reset(data_generator_->generateCompressedColumnstoreLayout(
          *relation_,
          static_cast<const BlockBasedExperimentConfiguration&>(configuration_).block_size_slots_,
          configuration_.column_store_sort_column_,
          index_columns));
    } else {
      layout.reset(data_generator_->generateColumnstoreLayout(
          *relation_,
          static_cast<const BlockBasedExperimentConfiguration&>(configuration_).block_size_slots_,
          configuration_.column_store_sort_column_,
          index_columns));
    }
  } else {
    if (configuration_.use_compression_) {
      layout.reset(data_generator_->generateCompressedRowstoreLayout(
          *relation_,
          static_cast<const BlockBasedExperimentConfiguration&>(configuration_).block_size_slots_,
          index_columns));
    } else {
      layout.reset(data_generator_->generateRowstoreLayout(
          *relation_,
          static_cast<const BlockBasedExperimentConfiguration&>(configuration_).block_size_slots_,
          index_columns));
    }
  }

  cout << "Generating and organizing data in-memory... ";
  cout.flush();
  AlwaysCreateBlockInsertDestination destination(&storage_manager_, relation_, layout.get());
  Timer gen_timer(false);
  gen_timer.start();
  data_generator_->generateData(configuration_.num_tuples_, &destination);
  gen_timer.stop();
  cout << "Done (" << gen_timer.getElapsed() << " s)\n";

  size_t block_memory_size
      = relation_->size_blocks()
        * static_cast<const BlockBasedExperimentConfiguration&>(configuration_).block_size_slots_
        * kSlotSizeBytes;
  if (block_memory_size < 1024) {
    cout << "Total data size: " << block_memory_size << " bytes\n";
  } else if (block_memory_size < 1024 * 1024) {
    cout << "Total data size: " << (block_memory_size / 1024.0) << " kilobytes\n";
  } else {
    cout << "Total data size: " << (block_memory_size / (1024.0 * 1024.0)) << " megabytes\n";
  }
  cout.flush();
}

void BlockBasedExperimentDriver::runExperiments() {
  ScopedPtr<TestRunner> runner;

  for (vector<ExperimentConfiguration::TestParameters>::const_iterator
           test_it = configuration_.test_params_.begin();
       test_it != configuration_.test_params_.end();
       ++test_it) {
    logTestParameters(*test_it);

    int index_param = -1;
    if (test_it->use_index) {
      index_param = 0;
    }

    if (test_it->projection_width == 0) {
      runner.reset(new BlockBasedPredicateEvaluationTestRunner(*relation_,
                                                               *data_generator_,
                                                               test_it->predicate_column,
                                                               index_param,
                                                               test_it->sort_matches,
                                                               test_it->selectivity,
                                                               configuration_.thread_affinities_,
                                                               configuration_.num_threads_,
                                                               &storage_manager_));
    } else {
      runner.reset(new BlockBasedSelectionTestRunner(
          *relation_,
          *data_generator_,
          test_it->predicate_column,
          index_param,
          test_it->sort_matches,
          test_it->selectivity,
          configuration_.thread_affinities_,
          configuration_.num_threads_,
          &storage_manager_,
          test_it->projection_width,
          static_cast<const BlockBasedExperimentConfiguration&>(configuration_).block_size_slots_,
          database_));
    }

    runner->doRuns(configuration_.num_runs_, configuration_.measure_cache_misses_);
    logTestResults(*runner);
  }
}

void FileBasedExperimentDriver::generateData() {
  size_t main_file_size;
  size_t index_file_size;
  int num_columns;

  switch(configuration_.table_choice_) {
    case ExperimentConfiguration::kNarrowE:
    case ExperimentConfiguration::kNarrowU:
      main_file_size = 40 * configuration_.num_tuples_ + 4096;
      index_file_size = (main_file_size >> 3) + (main_file_size >> 2);
      num_columns = 10;
      break;
    case ExperimentConfiguration::kWideE:
      main_file_size = 200 * configuration_.num_tuples_ + 4096;
      index_file_size = main_file_size >> 4;
      num_columns = 50;
      break;
    case ExperimentConfiguration::kStrings:
      main_file_size = 200 * configuration_.num_tuples_ + 4096;
      index_file_size = (main_file_size >> 3) + (main_file_size >> 2);
      num_columns = 10;
      break;
  }

  if (main_file_size < 1024) {
    cout << "Main file size: " << main_file_size << " bytes\n";
  } else if (main_file_size < 1024 * 1024) {
    cout << "Main file size: " << (main_file_size / 1024.0) << " kilobytes\n";
  } else {
    cout << "Main file size: " << (main_file_size / (1024.0 * 1024.0)) << " megabytes\n";
  }
  if (index_file_size < 1024) {
    cout << "Index file size: " << index_file_size << " bytes\n";
  } else if (main_file_size < 1024 * 1024) {
    cout << "Index file size: " << (index_file_size / 1024.0) << " kilobytes\n";
  } else {
    cout << "Index file size: " << (index_file_size / (1024.0 * 1024.0)) << " megabytes\n";
  }
  cout.flush();

  TupleStorageSubBlockDescription tuple_store_description;
  if (configuration_.use_column_store_) {
    if (configuration_.use_compression_) {
      tuple_store_description.set_sub_block_type(
          TupleStorageSubBlockDescription::COMPRESSED_COLUMN_STORE);
      tuple_store_description.SetExtension(
          CompressedColumnStoreTupleStorageSubBlockDescription::sort_attribute_id,
          configuration_.column_store_sort_column_);
      for (int compressed_column_id = 0;
           compressed_column_id < num_columns;
           ++compressed_column_id) {
        tuple_store_description.AddExtension(
            CompressedColumnStoreTupleStorageSubBlockDescription::compressed_attribute_id,
            compressed_column_id);
      }
    } else {
      tuple_store_description.set_sub_block_type(
          TupleStorageSubBlockDescription::BASIC_COLUMN_STORE);
      tuple_store_description.SetExtension(
          BasicColumnStoreTupleStorageSubBlockDescription::sort_attribute_id,
          configuration_.column_store_sort_column_);
    }
  } else {
    if (configuration_.use_compression_) {
      tuple_store_description.set_sub_block_type(
          TupleStorageSubBlockDescription::COMPRESSED_PACKED_ROW_STORE);
      for (int compressed_column_id = 0;
           compressed_column_id < num_columns;
           ++compressed_column_id) {
        tuple_store_description.AddExtension(
            CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id,
            compressed_column_id);
      }
    } else {
      tuple_store_description.set_sub_block_type(
          TupleStorageSubBlockDescription::PACKED_ROW_STORE);
    }
  }

  IndexSubBlockDescription index_description;
  if (configuration_.use_index_) {
    index_description.set_sub_block_type(IndexSubBlockDescription::CSB_TREE);
    index_description.AddExtension(CSBTreeIndexSubBlockDescription::indexed_attribute_id,
                                   configuration_.index_column_);
  }

  for (size_t partition_num = 0;
       partition_num < configuration_.num_threads_;
       ++partition_num) {
    tuple_store_buffers_.push_back(new ScopedBuffer(main_file_size / configuration_.num_threads_));
    if (configuration_.use_column_store_) {
      if (configuration_.use_compression_) {
        tuple_stores_.push_back(new CompressedColumnStoreTupleStorageSubBlock(
            *relation_,
            tuple_store_description,
            true,
            tuple_store_buffers_.back().get(),
            main_file_size / configuration_.num_threads_));
      } else {
        tuple_stores_.push_back(new BasicColumnStoreTupleStorageSubBlock(
            *relation_,
            tuple_store_description,
            true,
            tuple_store_buffers_.back().get(),
            main_file_size / configuration_.num_threads_));
      }
    } else {
      if (configuration_.use_compression_) {
        tuple_stores_.push_back(new CompressedPackedRowStoreTupleStorageSubBlock(
            *relation_,
            tuple_store_description,
            true,
            tuple_store_buffers_.back().get(),
            main_file_size / configuration_.num_threads_));
      } else {
        tuple_stores_.push_back(new PackedRowStoreTupleStorageSubBlock(
            *relation_,
            tuple_store_description,
            true,
            tuple_store_buffers_.back().get(),
            main_file_size / configuration_.num_threads_));
      }
    }

    if (configuration_.use_index_) {
      index_buffers_.push_back(new ScopedBuffer(index_file_size / configuration_.num_threads_));
      indices_.push_back(new CSBTreeIndexSubBlock(tuple_stores_.back(),
                                                  index_description,
                                                  true,
                                                  index_buffers_.back().get(),
                                                  index_file_size / configuration_.num_threads_));
    }
  }

  // Make the vectors of pointers that will be passed to the test runners.
  for (PtrVector<TupleStorageSubBlock>::const_iterator tuple_store_it = tuple_stores_.begin();
       tuple_store_it != tuple_stores_.end();
       ++tuple_store_it) {
    tuple_store_ptrs_.push_back(&(*tuple_store_it));
  }

  index_ptrs_.resize(1);
  if (configuration_.use_index_) {
    for (PtrVector<IndexSubBlock>::iterator index_it = indices_.begin();
         index_it != indices_.end();
         ++index_it) {
      index_ptrs_[0].push_back(&(*index_it));
    }
  } else {
    for (size_t partition_num = 0;
       partition_num < configuration_.num_threads_;
       ++partition_num) {
      index_ptrs_[0].push_back(NULL);
    }
  }

  // Actually generate data.
  cout << "Generating and organizing data in-memory...\n";
  Timer gen_timer(false);
  gen_timer.start();
  for (PtrVector<TupleStorageSubBlock>::iterator tuple_store_it = tuple_stores_.begin();
       tuple_store_it != tuple_stores_.end();
       ++tuple_store_it) {
    data_generator_->generateDataIntoTupleStore(
        configuration_.num_tuples_ / configuration_.num_threads_,
        &(*tuple_store_it));
  }
  gen_timer.stop();
  cout << "Generated (" << gen_timer.getElapsed() << " s)\n";

  Timer sort_timer(false);
  sort_timer.start();
  for (PtrVector<TupleStorageSubBlock>::iterator tuple_store_it = tuple_stores_.begin();
       tuple_store_it != tuple_stores_.end();
       ++tuple_store_it) {
    tuple_store_it->rebuild();
  }
  sort_timer.stop();
  cout << "Built files (sort/compress/etc.) (" << sort_timer.getElapsed() << " s)\n";

  if (configuration_.use_index_) {
    Timer index_timer(false);
    index_timer.start();
    for (PtrVector<IndexSubBlock>::iterator index_it = indices_.begin();
         index_it != indices_.end();
         ++index_it) {
      if (!index_it->rebuild()) {
        FATAL_ERROR("Unable to build index.\n");
      }
    }
    index_timer.stop();
    cout << "Index built (" << index_timer.getElapsed() << " s)\n";
  }
}

void FileBasedExperimentDriver::runExperiments() {
  ScopedPtr<TestRunner> runner;

  for (vector<ExperimentConfiguration::TestParameters>::const_iterator
           test_it = configuration_.test_params_.begin();
       test_it != configuration_.test_params_.end();
       ++test_it) {
    logTestParameters(*test_it);

    int index_param = -1;
    if (test_it->use_index) {
      index_param = 0;
    }

    if (test_it->projection_width == 0) {
      runner.reset(new FileBasedPredicateEvaluationTestRunner(*relation_,
                                                              *data_generator_,
                                                              test_it->predicate_column,
                                                              index_param,
                                                              test_it->sort_matches,
                                                              test_it->selectivity,
                                                              configuration_.thread_affinities_,
                                                              tuple_store_ptrs_,
                                                              index_ptrs_));
    } else {
      size_t estimated_result_size_bytes;
      switch(configuration_.table_choice_) {
        case ExperimentConfiguration::kNarrowE:
        case ExperimentConfiguration::kNarrowU:
        case ExperimentConfiguration::kWideE:
          estimated_result_size_bytes = 4
                                        * configuration_.num_tuples_
                                        * test_it->projection_width
                                        * test_it->selectivity;
          break;
        case ExperimentConfiguration::kStrings:
          estimated_result_size_bytes = 20
                                        * configuration_.num_tuples_
                                        * test_it->projection_width
                                        * test_it->selectivity;
          break;
      }

      // Distribution should be uniform, but give 5% extra for accidental RNG
      // skew.
      size_t result_buffer_size_per_partition =
          ((estimated_result_size_bytes * 21) / 20) / configuration_.num_threads_;

      runner.reset(new FileBasedSelectionTestRunner(
          *relation_,
          *data_generator_,
          test_it->predicate_column,
          index_param,
          test_it->sort_matches,
          test_it->selectivity,
          configuration_.thread_affinities_,
          tuple_store_ptrs_,
          index_ptrs_,
          test_it->projection_width,
          result_buffer_size_per_partition,
          database_));
    }

    runner->doRuns(configuration_.num_runs_, configuration_.measure_cache_misses_);
    logTestResults(*runner);
  }
}

}  // namespace storage_explorer
}  // namespace quickstep
