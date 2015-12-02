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

#include "experiments/storage_explorer/ExperimentConfiguration.hpp"

#include <cmath>
#include <cstddef>
#include <cstring>
#include <iostream>
#include <vector>

#include "experiments/storage_explorer/StorageExplorerConfig.h"
#include "utility/Macros.hpp"

#include "third_party/cJSON/cJSON.h"

using std::floor;
using std::ostream;
using std::size_t;
using std::strcmp;
using std::vector;

namespace quickstep {
namespace storage_explorer {

ExperimentConfiguration* ExperimentConfiguration::LoadFromJSON(cJSON *json) {
  ExperimentConfiguration *configuration = NULL;

  if (json->type != cJSON_Object) {
    FATAL_ERROR("Experiment configuration is not a JSON object.");
  }

  cJSON *json_use_blocks = cJSON_GetObjectItem(json, "use_blocks");
  if (json_use_blocks == NULL) {
    FATAL_ERROR("No \"use_blocks\" attribute in experiment configuration.");
  }
  if ((json_use_blocks->type != cJSON_False) && (json_use_blocks->type != cJSON_True)) {
    FATAL_ERROR("\"use_blocks\" is not a boolean in experiment configuration.");
  }
  if (json_use_blocks->type == cJSON_True) {
    configuration = new BlockBasedExperimentConfiguration();
  } else {
    configuration = new FileBasedExperimentConfiguration();
  }

  cJSON *json_table = cJSON_GetObjectItem(json, "table");
  if (json_table == NULL) {
    FATAL_ERROR("No \"table\" attribute in experiment configuration.");
  }
  if (json_table->type != cJSON_String) {
    FATAL_ERROR("\"table\" is not a string in experiment configuration.");
  }
  if (strcmp("narrow_e", json_table->valuestring) == 0) {
    configuration->table_choice_ = kNarrowE;
  } else if (strcmp("narrow_u", json_table->valuestring) == 0) {
    configuration->table_choice_ = kNarrowU;
  } else if (strcmp("wide_e", json_table->valuestring) == 0) {
    configuration->table_choice_ = kWideE;
  } else if (strcmp("strings", json_table->valuestring) == 0) {
    configuration->table_choice_ = kStrings;
  } else {
    FATAL_ERROR("\"table\" in experiment configuration is not one of "
                "[\"narrow_e\", \"narrow_u\", \"wide_e\", \"strings\"]");
  }

  cJSON *json_num_tuples = cJSON_GetObjectItem(json, "num_tuples");
  if (json_num_tuples == NULL) {
    FATAL_ERROR("No \"num_tuples\" attribute in experiment configuration.");
  }
  if (json_num_tuples->type != cJSON_Number) {
    FATAL_ERROR("\"num_tuples\" is not a number in experiment configuration.");
  }
  if (json_num_tuples->valuedouble < 1.0) {
    FATAL_ERROR("\"num_tuples\" is not positive in experiment configuration.");
  }
  if (json_num_tuples->valuedouble != floor(json_num_tuples->valuedouble)) {
    FATAL_ERROR("\"num_tuples\" is not an integer (it has a fractional part) "
                "in experiment configuration.");
  }
  configuration->num_tuples_ = static_cast<size_t>(json_num_tuples->valuedouble);

  cJSON *json_layout_type = cJSON_GetObjectItem(json, "layout_type");
  if (json_layout_type == NULL) {
    FATAL_ERROR("No \"layout_type\" attribute in experiment configuration.");
  }
  if (json_layout_type->type != cJSON_String) {
    FATAL_ERROR("\"layout_type\" is not a string in experiment configuration.");
  }
  if (strcmp("rowstore", json_layout_type->valuestring) == 0) {
    configuration->use_column_store_ = false;
  } else if (strcmp("columnstore", json_layout_type->valuestring) == 0) {
    configuration->use_column_store_ = true;
    cJSON *json_sort_column = cJSON_GetObjectItem(json, "sort_column");
    if (json_sort_column == NULL) {
      FATAL_ERROR("experiment configuration specifies \"layout_type\" of "
                  "\"columnstore\" but no \"sort_column\".");
    }
    if (json_sort_column->type != cJSON_Number) {
      FATAL_ERROR("\"sort_column\" is not a number in experiment configuration.");
    }
    if (json_sort_column->valuedouble < 0.0) {
      FATAL_ERROR("\"sort_column\" is negative in experiment configuration.");
    }
    if (json_sort_column->valuedouble != floor(json_sort_column->valuedouble)) {
      FATAL_ERROR("\"sort_column\" is not an integer (it has a fractional part) "
                  "in experiment configuration.");
    }
    configuration->column_store_sort_column_ = static_cast<int>(json_sort_column->valuedouble);
    if ((configuration->table_choice_ == kWideE)
        && (configuration->column_store_sort_column_ > 49)) {
      FATAL_ERROR("\"sort_column\" in experiment configuration must be in the "
                  "range 0-49 for the specified table.");
    } else if (configuration->column_store_sort_column_ > 9) {
      FATAL_ERROR("\"sort_column\" in experiment configuration must be in the "
                  "range 0-9 for the specified table.");
    }
  } else {
    FATAL_ERROR("\"layout_type\" is not one of [\"rowstore\", \"columnstore\"] in experiment configuration.");
  }

  cJSON *json_use_compression = cJSON_GetObjectItem(json, "use_compression");
  if (json_use_compression == NULL) {
    FATAL_ERROR("No \"use_compression\" attribute in experiment configuration.");
  }
  if ((json_use_compression->type != cJSON_False)
      && (json_use_compression->type != cJSON_True)) {
    FATAL_ERROR("\"use_compression\" is not a boolean in experiment configuration.");
  }
  if (json_use_compression->type == cJSON_True) {
    configuration->use_compression_ = true;
  } else {
    configuration->use_compression_ = false;
  }

  cJSON *json_use_bloom_filter = cJSON_GetObjectItem(json, "use_bloom_filter");
  if (json_use_bloom_filter == NULL) {	// enable bloom filters by default
    configuration->use_bloom_filter_ = true;
  }
  if ((json_use_bloom_filter->type != cJSON_False)
      && (json_use_bloom_filter->type != cJSON_True)) {
    FATAL_ERROR("\"use_bloom_filter\" is not a boolean in experiment configuration.");
  }
  if (json_use_bloom_filter->type == cJSON_True) {
    configuration->use_bloom_filter_ = true;
  } else {
    configuration->use_bloom_filter_ = false;
  }


  cJSON *json_index_column = cJSON_GetObjectItem(json, "index_column");
  if (json_index_column == NULL) {
    configuration->use_index_ = false;
  } else {
    configuration->use_index_ = true;
    if (json_index_column->type != cJSON_Number) {
      FATAL_ERROR("\"index_column\" is not a number in experiment configuration.");
    }
    if (json_index_column->valuedouble < 0.0) {
      FATAL_ERROR("\"index_column\" is negative in experiment configuration.");
    }
    if (json_index_column->valuedouble != floor(json_index_column->valuedouble)) {
      FATAL_ERROR("\"index_column\" is not an integer (it has a fractional part) "
                  "in experiment configuration.");
    }
    configuration->index_column_ = static_cast<int>(json_index_column->valuedouble);
    if ((configuration->table_choice_ == kWideE)
        && (configuration->index_column_ > 49)) {
      FATAL_ERROR("\"index_column\" in experiment configuration must be in the "
                  "range 0-49 for the specified table.");
    } else if (configuration->index_column_ > 9) {
      FATAL_ERROR("\"index_column\" in experiment configuration must be in the "
                  "range 0-9 for the specified table.");
    }
  }

  cJSON *json_num_runs = cJSON_GetObjectItem(json, "num_runs");
  if (json_num_runs == NULL) {
    FATAL_ERROR("No \"num_runs\" attribute in experiment configuration.");
  }
  if (json_num_runs->type != cJSON_Number) {
    FATAL_ERROR("\"num_runs\" is not a number in experiment configuration.");
  }
  if (json_num_runs->valuedouble < 1.0) {
    FATAL_ERROR("\"num_runs\" is not positive in experiment configuration.");
  }
  if (json_num_runs->valuedouble != floor(json_num_runs->valuedouble)) {
    FATAL_ERROR("\"num_runs\" is not an integer (it has a fractional part) "
                "in experiment configuration.");
  }
  configuration->num_runs_ = static_cast<size_t>(json_num_runs->valuedouble);

  cJSON *json_measure_cache_misses = cJSON_GetObjectItem(json, "measure_cache_misses");
  if (json_measure_cache_misses == NULL) {
    FATAL_ERROR("No \"measure_cache_misses\" attribute in experiment configuration.");
  }
  if ((json_measure_cache_misses->type != cJSON_False)
      && (json_measure_cache_misses->type != cJSON_True)) {
    FATAL_ERROR("\"measure_cache_misses\" is not a boolean in experiment configuration.");
  }
  if (json_measure_cache_misses->type == cJSON_True) {
#ifndef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
    FATAL_ERROR("\"measure_cache_misses\" is true in experiment configuration, "
                "but this binary was built without Intel PCM support.");
#endif
    configuration->measure_cache_misses_ = true;
  } else {
    configuration->measure_cache_misses_ = false;
  }

  cJSON *json_num_threads = cJSON_GetObjectItem(json, "num_threads");
  if (json_num_threads == NULL) {
    FATAL_ERROR("No \"num_threads\" attribute in experiment configuration.");
  }
  if (json_num_threads->type != cJSON_Number) {
    FATAL_ERROR("\"num_threads\" is not a number in experiment configuration.");
  }
  if (json_num_threads->valuedouble < 1.0) {
    FATAL_ERROR("\"num_threads\" is not positive in experiment configuration.");
  }
  if (json_num_threads->valuedouble != floor(json_num_threads->valuedouble)) {
    FATAL_ERROR("\"num_threads\" is not an integer (it has a fractional part) "
                "in experiment configuration.");
  }
  configuration->num_threads_ = static_cast<size_t>(json_num_threads->valuedouble);

  cJSON *json_thread_affinity_array = cJSON_GetObjectItem(json, "thread_affinities");
  if (json_thread_affinity_array != NULL) {
#ifndef QUICKSTEP_STORAGE_EXPLORER_PTHREAD_SETAFFINITY_AVAILABLE
    FATAL_ERROR("Experiment configuration specifies \"thread_affinities\", but "
                "this binary does not support thread affinitization "
                "(pthread_setaffinity_np() was not available).");
#endif
    if (json_thread_affinity_array->type != cJSON_Array) {
      FATAL_ERROR("\"thread_affinities\" is not an array in experiment configuration.");
    }
    int num_thread_affinities = cJSON_GetArraySize(json_thread_affinity_array);
    if (static_cast<size_t>(num_thread_affinities) != configuration->num_threads_) {
      FATAL_ERROR("\"thread_affinities\" is not \"num_threads\" in length in experiment configuration.");
    }
    for (int thread_idx = 0; thread_idx < num_thread_affinities; ++thread_idx) {
      cJSON *json_thread_affinity_item = cJSON_GetArrayItem(json_thread_affinity_array, thread_idx);
      if (json_thread_affinity_item->type != cJSON_Number) {
        FATAL_ERROR("\"thread_affinities\" array in experiment configuration contains a non-number.");
      }
      if (json_thread_affinity_item->valuedouble < 0.0) {
        FATAL_ERROR("\"thread_affinities\" array in experiment configuration contains a negative number.");
      }
      if (json_thread_affinity_item->valuedouble != floor(json_thread_affinity_item->valuedouble)) {
        FATAL_ERROR("\"thread_affinities\" array in experiment configuration contains a non-integer.");
      }
      configuration->thread_affinities_.push_back(static_cast<int>(json_thread_affinity_item->valuedouble));
    }
  }

  cJSON *json_tests_array = cJSON_GetObjectItem(json, "tests");
  if (json_tests_array != NULL) {
    if (json_tests_array->type != cJSON_Array) {
      FATAL_ERROR("\"tests\" is not an array in experiment configuration.");
    }
    int num_tests = cJSON_GetArraySize(json_tests_array);
    for (int test_idx = 0; test_idx < num_tests; ++test_idx) {
      cJSON *json_test_item = cJSON_GetArrayItem(json_tests_array, test_idx);
      if (json_test_item->type != cJSON_Object) {
        FATAL_ERROR("\"tests\" array in experiment configuration contains a non-object.");
      }
      configuration->loadTestParametersFromJSON(json_test_item);
    }
  }

  configuration->loadAdditionalConfigurationFromJSON(json);

  return configuration;
}

void ExperimentConfiguration::logConfiguration(std::ostream *output) const {
  *output << "Experiment Configuration:\n";

  *output << "Table: ";
  switch (table_choice_) {
    case kNarrowE:
      *output << "narrow-e\n";
      break;
    case kNarrowU:
      *output << "narrow-u\n";
      break;
    case kWideE:
      *output << "wide-e\n";
      break;
    case kStrings:
      *output << "strings\n";
      break;
  }
  *output << "Tuples: " << num_tuples_ << "\n";

  logAdditionalConfiguration(output);

  *output << "Physical Layout:\n";
  *output << "    Tuple Storage: ";
  if (use_column_store_) {
    *output << "Column Store (Sort Column: " << column_store_sort_column_ << ")\n";
  } else {
    *output << "Row Store\n";
  }
  if (use_index_) {
    *output << "    CSBTree Index On Column: " << index_column_ << "\n";
  } else {
    *output << "    No Index\n";
  }
  if (use_compression_) {
    *output << "    Compression Enabled\n";
  } else {
    *output << "    Compression Not Enabled\n";
  }
  if (use_bloom_filter_) {
	*output << "    Bloom Filter Enabled\n";
  } else {
	*output << "    Bloom Filter Not Enabled\n";
  }

  *output << "Test Parameters:\n";
  *output << "    Runs Per Test: " << num_runs_ << "\n";
  *output << "    Execution Threads: " << num_threads_ << "\n";
  if (thread_affinities_.empty()) {
    *output << "    Thread Affinity Not Enabled\n";
  } else {
    *output << "    Thread Affinity List (CPU IDs): [";
    for (vector<int>::const_iterator it = thread_affinities_.begin();
         it != thread_affinities_.end();
         ++it) {
      if (it != thread_affinities_.begin()) {
        *output << ", ";
      }
      *output << *it;
    }
    *output << "]\n";
  }
  if (measure_cache_misses_) {
    *output << "    Cache Miss Measurement Enabled\n";
  } else {
    *output << "    Cache Miss Measurement Not Enabled\n";
  }
}

void ExperimentConfiguration::loadTestParametersFromJSON(cJSON *test_params_json) {
  TestParameters params;

  cJSON *json_predicate_column = cJSON_GetObjectItem(test_params_json, "predicate_column");
  if (json_predicate_column == NULL) {
    FATAL_ERROR("A test in experiment configuration did not specify \"predicate_column\"");
  }
  if (json_predicate_column->type != cJSON_Number) {
    FATAL_ERROR("\"predicate_column\" is not a number in a test in experiment configuration.");
  }
  if (json_predicate_column->valuedouble < 0.0) {
    FATAL_ERROR("\"predicate_column\" is negative in a test in experiment configuration.");
  }
  if (json_predicate_column->valuedouble != floor(json_predicate_column->valuedouble)) {
    FATAL_ERROR("\"predicate_column\" is not an integer (it has a fractional part) "
                "in a test in experiment configuration.");
  }
  params.predicate_column = static_cast<int>(json_predicate_column->valuedouble);
  if ((table_choice_ == kWideE) && (params.predicate_column > 49)) {
    FATAL_ERROR("\"predicate_column\" in tests in experiment configuration "
                "must be in the range 0-49 for the specified table.");
  } else if (params.predicate_column > 9) {
    FATAL_ERROR("\"predicate_column\" in tests in experiment configuration "
                "must be in the range 0-9 for the specified table.");
  }

  cJSON *json_use_index = cJSON_GetObjectItem(test_params_json, "use_index");
  if (json_use_index == NULL) {
    FATAL_ERROR("A test in experiment configuration did not specify \"use_index\"");
  }
  if ((json_use_index->type != cJSON_False)
      && (json_use_index->type != cJSON_True)) {
    FATAL_ERROR("\"use_index\" is not a boolean in a test in experiment configuration.");
  }
  if (json_use_index->type == cJSON_True) {
    params.use_index = true;
    if (!use_index_) {
      FATAL_ERROR("A test in experiment configuration specified \"use_index\" "
                  "as true, but no \"index_column\" was specified.");
    }
    if (index_column_ != params.predicate_column) {
      FATAL_ERROR("A test in experiment configuration specified \"use_index\" "
                  "as true, but \"predicate_column\" is different from \"index_column\".");
    }
  } else {
    params.use_index = false;
  }

  cJSON *json_sort_matches = cJSON_GetObjectItem(test_params_json, "sort_matches_before_projection");
  if (json_sort_matches == NULL) {
    FATAL_ERROR("A test in experiment configuration did not specify \"sort_matches_before_projection\"");
  }
  if ((json_sort_matches->type != cJSON_False)
      && (json_sort_matches->type != cJSON_True)) {
    FATAL_ERROR("\"sort_matches_before_projection\" is not a boolean in a test in experiment configuration.");
  }
  if (json_sort_matches->type == cJSON_True) {
    params.sort_matches = true;
  } else {
    params.sort_matches = false;
  }

  cJSON *json_selectivity = cJSON_GetObjectItem(test_params_json, "selectivity");
  if (json_selectivity == NULL) {
    FATAL_ERROR("\"selectivity\" is not specified for a test in experiment configuration.");
  }
  if (json_selectivity->type != cJSON_Number) {
    FATAL_ERROR("\"selectivity\" is not a number for a test in experiment configuration.");
  }
  if ((json_selectivity->valuedouble <= 0.0) || (json_selectivity->valuedouble > 1.0)) {
    FATAL_ERROR("\"selectivity\" must be in the range (0.0, 1.0] for all tests in experiment configuration.");
  }
  params.selectivity = json_selectivity->valuedouble;

  cJSON *json_projection_width = cJSON_GetObjectItem(test_params_json, "projection_width");
  if (json_projection_width == NULL) {
    FATAL_ERROR("\"projection_width\" is not specified for a test in experiment configuration.");
  }
  if (json_projection_width->type != cJSON_Number) {
    FATAL_ERROR("\"projection_width\" is not a number for a test in experiment configuration.");
  }
  if (json_projection_width->valuedouble < 0.0) {
    FATAL_ERROR("\"projection_width\" is negative for a test in experiment configuration.");
  }
  if (json_projection_width->valuedouble != floor(json_projection_width->valuedouble)) {
    FATAL_ERROR("\"projection_width\" is not an integer (it has a fractional part) "
                "in experiment configuration.");
  }
  params.projection_width = static_cast<size_t>(json_projection_width->valuedouble);
  if ((table_choice_ == kWideE) && (params.projection_width > 50)) {
    FATAL_ERROR("\"projection_width\" in tests in experiment configuration "
                "must be in the range 0-50 for the specified table.");
  } else if (params.projection_width > 10) {
    FATAL_ERROR("\"projection_width\" in tests in experiment configuration "
                "must be in the range 0-10 for the specified table.");
  }

  test_params_.push_back(params);
}

void BlockBasedExperimentConfiguration::loadAdditionalConfigurationFromJSON(cJSON *json) {
  cJSON *json_block_size = cJSON_GetObjectItem(json, "block_size_mb");
  if (json_block_size == NULL) {
    FATAL_ERROR("No \"block_size_mb\" attribute in experiment configuration.");
  }
  if (json_block_size->type != cJSON_Number) {
    FATAL_ERROR("\"block_size_mb\" is not a number in experiment configuration.");
  }
  if (json_block_size->valuedouble < 1.0) {
    FATAL_ERROR("\"block_size_mb\" must be positive in experiment configuration.");
  }
  if (json_block_size->valuedouble != floor(json_block_size->valuedouble)) {
    FATAL_ERROR("\"block_size_mb\" is not an integer (it has a fractional part) "
                "in experiment configuration.");
  }
  block_size_slots_ = static_cast<size_t>(json_block_size->valuedouble);
/*
  cJSON *json_num_partitions = cJSON_GetObjectItem(json, "num_partitions");
  if (json_num_partitions == NULL) {
    num_partitions_ = 1;
  } else {
    if (json_num_partitions->type != cJSON_Number) {
      FATAL_ERROR("\"num_partitions\" is not a number in experiment configuration.");
    }
    if (json_num_partitions->valuedouble < 1.0) {
      FATAL_ERROR("\"num_partitions\" must be positive in experiment configuration.");
    }
    if (json_num_partitions->valuedouble != floor(json_num_partitions->valuedouble)) {
      FATAL_ERROR("\"num_partitions\" is not an integer (it has a fractional part) "
                  "in experiment configuration.");
    }
    num_partitions_ = static_cast<size_t>(json_num_partitions->valuedouble);
  }
*/
}

void BlockBasedExperimentConfiguration::logAdditionalConfiguration(std::ostream *output) const {
  *output << "Using Block-Based Organization (Block Size: " << block_size_slots_ << " MB)\n";
}

void FileBasedExperimentConfiguration::logAdditionalConfiguration(std::ostream *output) const {
  *output << "Using File-Based Organization";
  if (num_threads_ > 1) {
    *output << " (" << num_threads_ << " Static Partitions)";
  }
  *output << "\n";
}

}  // namespace storage_explorer
}  // namespace quickstep
