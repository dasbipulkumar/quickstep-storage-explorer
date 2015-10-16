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

#include "experiments/storage_explorer/ThreadAffinity.hpp"

#include "experiments/storage_explorer/StorageExplorerConfig.h"
#ifdef QUICKSTEP_STORAGE_EXPLORER_PTHREAD_SETAFFINITY_AVAILABLE
// Required for non-portable thread-affinity functions.
#define _GNU_SOURCE
#include <pthread.h>
#endif

#include "utility/Macros.hpp"

namespace quickstep {
namespace storage_explorer {

void ThreadAffinity::BindThisThreadToCPU(const int cpu_id) {
#ifdef QUICKSTEP_STORAGE_EXPLORER_PTHREAD_SETAFFINITY_AVAILABLE
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu_id, &cpuset);

  if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
    FATAL_ERROR("Failed to pin thread to CPU " << cpu_id);
  }
#else
  FATAL_ERROR("ThreadAffinity::BindThisThreadToCPU() was called, but this "
              "binary was built without thread affinitization support.");
#endif
}

}  // namespace storage_explorer
}  // namespace quickstep
