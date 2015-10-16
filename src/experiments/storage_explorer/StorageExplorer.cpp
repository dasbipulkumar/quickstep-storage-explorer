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

#include <fstream>
#include <iostream>
#include <string>

#include "experiments/storage_explorer/ExperimentConfiguration.hpp"
#include "experiments/storage_explorer/ExperimentDriver.hpp"
#include "experiments/storage_explorer/StorageExplorerConfig.h"
#include "utility/ScopedPtr.hpp"

#include "third_party/cJSON/cJSON.h"
#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
#include "third_party/intel-pcm/cpucounters.h"
#endif

using quickstep::ScopedPtr;
using quickstep::storage_explorer::ExperimentConfiguration;
using quickstep::storage_explorer::ExperimentDriver;
using std::cerr;
using std::cout;
using std::ifstream;
using std::string;

int main(int argc, char *argv[]) {
  if (argc != 2) {
    cerr << "USAGE: " << argv[0] << " configuration_file.json\n";
    return 1;
  }

  ifstream config_file(argv[1]);
  if (!config_file.good()) {
    cerr << "ERROR: Unable to open configuration file: " << argv[1] << "\n";
    return 1;
  }
  string config_str((std::istreambuf_iterator<char>(config_file)),
                    std::istreambuf_iterator<char>());
  config_file.close();
  cJSON *config_json = cJSON_Parse(config_str.c_str());
  if (config_json == NULL) {
    cerr << "ERROR: configuration file " << argv[1]
         << " does not contain properly-formatted JSON.\n";
    return 1;
  }

  ScopedPtr<ExperimentConfiguration> configuration(
      ExperimentConfiguration::LoadFromJSON(config_json));
  cJSON_Delete(config_json);

  configuration->logConfiguration(&cout);

#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
  if (configuration->measureCacheMisses()) {
    cout << "Programming Intel CPU performance counters...\n";
    if (PCM::getInstance()->program() != PCM::Success) {
      cerr << "ERROR: Failed to initialize Intel CPU performance counters (make "
              "sure you are running as root on a system with a supported Intel CPU).\n";
      return 1;
    }
    cout << "Intel CPU performance counters successfully initialized.\n\n";
  }
#endif

  cout << "Setting up Experiment Driver... ";
  ScopedPtr<ExperimentDriver> driver(
      ExperimentDriver::CreateDriverForConfiguration(*configuration));
  driver->initialize();
  cout << "Done.\n";

  cout << "Starting data generation in main thread...\n";
  driver->generateData();
  cout << "Data generation complete.\n\n";

  cout << "Running experiments:\n";
  driver->runExperiments();
  cout << "\nAll Experiments Complete.\n";

#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
  if (configuration->measureCacheMisses()) {
    cout << "Cleaning up Intel CPU performance counters... ";
    PCM::getInstance()->cleanup();
    cout << "Done.\n";
  }
#endif

  return 0;
}
