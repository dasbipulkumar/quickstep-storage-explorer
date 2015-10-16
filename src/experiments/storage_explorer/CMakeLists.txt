# This file copyright (c) 2011-2013, the Quickstep authors.
# See file CREDITS.txt for details.
#
# This file is part of Quickstep.
#
# Quickstep is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Quickstep is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Quickstep.  If not, see <http://www.gnu.org/licenses/>.

if (USE_INTEL_PCM)
  set(QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM TRUE)
endif()

include(CheckCXXSourceCompiles)

set(original_required_libs ${CMAKE_REQUIRED_LIBRARIES})
set(CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
CHECK_CXX_SOURCE_COMPILES("
#define _GNU_SOURCE
#include <pthread.h>

int main() {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);

  if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) != 0) {
    return 1;
  }
  return 0;
}
" QUICKSTEP_STORAGE_EXPLORER_PTHREAD_SETAFFINITY_AVAILABLE)
if (NOT QUICKSTEP_STORAGE_EXPLORER_PTHREAD_SETAFFINITY_AVAILABLE)
  message(WARNING "Your system does not seem to have pthread_setaffinity_np(). "
                  "Thread affinitization/pinning will not be available.")
endif()
set(CMAKE_REQUIRED_LIBRARIES ${original_required_libs})

CHECK_CXX_SOURCE_COMPILES("
#include <ctime>

int main() {
  timespec timevar;
  clock_gettime(CLOCK_REALTIME, &timevar);
  return 0;
}
" POSIX_TIMERS_AVAILABLE)
if (NOT POSIX_TIMERS_AVAILABLE)
  set(CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES} -lrt)
  CHECK_CXX_SOURCE_COMPILES("
  #include <ctime>

  int main() {
    timespec timevar;
    clock_gettime(CLOCK_REALTIME, &timevar);
    return 0;
  }
  " POSIX_TIMERS_AVAILABLE_WITH_LRT)
  set(CMAKE_REQUIRED_LIBRARIES ${original_required_libs})
endif()
if (POSIX_TIMERS_AVAILABLE OR POSIX_TIMERS_AVAILABLE_WITH_LRT)
  set(QUICKSTEP_STORAGE_EXPLORER_POSIX_TIMERS_AVAILABLE TRUE)
endif()
if (NOT QUICKSTEP_STORAGE_EXPLORER_POSIX_TIMERS_AVAILABLE)
  CHECK_CXX_SOURCE_COMPILES("
  #include <windows.h>

  int main() {
    FILETIME timevar;
    GetSystemTimeAsFileTime(&timevar);
    return 0;
  }
  " QUICKSTEP_STORAGE_EXPLORER_WINDOWS_TIMERS_AVAILABLE)
endif()

if (NOT (QUICKSTEP_STORAGE_EXPLORER_POSIX_TIMERS_AVAILABLE OR QUICKSTEP_STORAGE_EXPLORER_WINDOWS_TIMERS_AVAILABLE))
  message(FATAL_ERROR "Neither POSIX nor Windows timers seem to be available on this system.")
endif()

configure_file (
  "${CMAKE_CURRENT_SOURCE_DIR}/StorageExplorerConfig.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/StorageExplorerConfig.h"
)

add_library(storageexplorer
            DataGenerator.cpp ExperimentConfiguration.cpp ExperimentDriver.cpp
            QueryExecutor.cpp TestRunner.cpp ThreadAffinity.cpp)
add_dependencies(storageexplorer storage_proto)
