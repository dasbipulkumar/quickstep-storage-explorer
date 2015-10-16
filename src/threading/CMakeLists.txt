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

include(CheckIncludeFileCXX)
include(CheckCXXSourceCompiles)

set(FOUND_THREADS FALSE)

if(NOT FOUND_THREADS)
  CHECK_CXX_SOURCE_COMPILES("
#include <thread>
#include <mutex>

int main() {
  std::thread t;
  std::mutex m;
  std::recursive_mutex rm;
  std::lock_guard<std::mutex> lock_m(m);
  std::lock_guard<std::recursive_mutex> lock_rm(rm);
  return 0;
}
" QUICKSTEP_HAVE_CPP11_THREADS)
  if (QUICKSTEP_HAVE_CPP11_THREADS)
    message("Using threading implementation: cpp11")
    set(FOUND_THREADS TRUE)
  endif()
endif()

if(NOT FOUND_THREADS)
  if(CMAKE_HAVE_PTHREAD_H)
    message("Using threading implementation: posix")
    set(QUICKSTEP_HAVE_POSIX_THREADS TRUE)
    set(FOUND_THREADS TRUE)
      add_subdirectory(posix)
  endif()
endif()

if(NOT FOUND_THREADS)
  CHECK_CXX_SOURCE_COMPILES("
#include <windows.h>

int main() {
  CRITICAL_SECTION cs;
  InitializeCriticalSection(&cs);
  EnterCriticalSection(&cs);
  LeaveCriticalSection(&cs);
  DeleteCriticalSection(&cs);
  return 0;
}
" QUICKSTEP_HAVE_WINDOWS_THREADS)
  if (QUICKSTEP_HAVE_WINDOWS_THREADS)
    message("Using threading implementation: windows")
    set(FOUND_THREADS TRUE)
    add_subdirectory(windows)
  endif()
endif()

if(NOT FOUND_THREADS)
  message(FATAL_ERROR "No viable threading library found.")
endif()

configure_file (
  "${CMAKE_CURRENT_SOURCE_DIR}/ThreadingConfig.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/ThreadingConfig.h"
)

add_library(threading Mutex.cpp Thread.cpp)
if(QUICKSTEP_HAVE_POSIX_THREADS)
  target_link_libraries(threading threading_posix)
endif()
if(QUICKSTEP_HAVE_WINDOWS_THREADS)
  target_link_libraries(threading threading_windows)
endif()
