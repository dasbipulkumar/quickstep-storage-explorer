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

#ifndef QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_TIMER_HPP_
#define QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_TIMER_HPP_

#include <cstddef>
#include <ctime>

#include "experiments/storage_explorer/StorageExplorerConfig.h"
#include "utility/CstdintCompat.hpp"
#include "utility/ScopedPtr.hpp"

#ifdef QUICKSTEP_STORAGE_EXPLORER_WINDOWS_TIMERS_AVAILABLE
#include <windows.h>
// Workaround for the fact that windows.h defines a macro max() which
// supersedes the standard library function.
#undef max
#endif

#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
#include "third_party/intel-pcm/cpucounters.h"
#endif

namespace quickstep {
namespace storage_explorer {

/**
 * @brief Object which measures an interval of real time with high precision,
 *        optionally also measuring CPU cache misses using the Intel PCM
 *        library.
 **/
class Timer {
 public:
  /**
   * @brief Structure containing all the data collected by a Timer.
   **/
  struct RunStats {
    double elapsed_time;
    std::uint64_t l2_misses;
    std::uint64_t l3_misses;
  };

  /**
   * @brief Constructor.
   *
   * @param measure_cache_misses If true, and this binary is built with Intel
   *        PCM support, system-wide cache misses will be measured, in addition
   *        to time. If this build does not have Intel PCM support, this has no
   *        effect.
   **/
  explicit Timer(const bool measure_cache_misses)
      : measure_cache_misses_(measure_cache_misses) {
#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
    if (measure_cache_misses_) {
      before_state_.reset(new SystemCounterState());
      after_state_.reset(new SystemCounterState());
    }
#endif
  }

  ~Timer() {
  }

  /**
   * @brief Start the timer.
   **/
  inline void start() {
#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
    if (measure_cache_misses_) {
      *before_state_ = getSystemCounterState();
    }
#endif
#ifdef QUICKSTEP_STORAGE_EXPLORER_POSIX_TIMERS_AVAILABLE
    clock_gettime(CLOCK_REALTIME, &start_time_);
#endif
#ifdef QUICKSTEP_STORAGE_EXPLORER_WINDOWS_TIMERS_AVAILABLE
    GetSystemTimeAsFileTime(&start_time_win_);
#endif
  }

  /**
   * @brief Stop the timer.
   **/
  inline void stop() {
#ifdef QUICKSTEP_STORAGE_EXPLORER_POSIX_TIMERS_AVAILABLE
    clock_gettime(CLOCK_REALTIME, &end_time_);
#endif
#ifdef QUICKSTEP_STORAGE_EXPLORER_WINDOWS_TIMERS_AVAILABLE
    GetSystemTimeAsFileTime(&end_time_win_);
#endif
#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
    if (measure_cache_misses_) {
      *after_state_ = getSystemCounterState();
    }
#endif
  }

  /**
   * @brief Get the time elapsed between calls to start() and stop().
   *
   * @return The elapsed time in seconds.
   **/
  double getElapsed() const {
#ifdef QUICKSTEP_STORAGE_EXPLORER_POSIX_TIMERS_AVAILABLE
    return static_cast<double>(end_time_.tv_sec - start_time_.tv_sec)
         + static_cast<double>(end_time_.tv_nsec - start_time_.tv_nsec) * 1.0e-9;
#endif
#ifdef QUICKSTEP_STORAGE_EXPLORER_WINDOWS_TIMERS_AVAILABLE
    std::uint64_t start_time_unified
        = (static_cast<std::uint64_t>(start_time_win_.dwHighDateTime) << 32)
          | static_cast<std::uint64_t>(start_time_win_.dwLowDateTime);
    std::uint64_t end_time_unified
        = (static_cast<std::uint64_t>(end_time_win_.dwHighDateTime) << 32)
          | static_cast<std::uint64_t>(end_time_win_.dwLowDateTime);
    return static_cast<double>(end_time_unified - start_time_unified) * 1.0e-7;
#endif
  }

  /**
   * @brief Get the number of system-wide L2 cache misses which occured between
   *        calls to start() and stop().
   *
   * @return The total number of L2 cache misses, or 0 if cache misses were not
   *         recorded.
   **/
  uint64_t getL2CacheMisses() const {
#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
    if (measure_cache_misses_) {
      return ::getL2CacheMisses(*before_state_, *after_state_);
    } else {
      return 0;
    }
#else
    return 0;
#endif
  }

  /**
   * @brief Get the number of system-wide L3 cache misses which occured between
   *        calls to start() and stop().
   *
   * @return The total number of L3 cache misses, or 0 if cache misses were not
   *         recorded.
   **/
  uint64_t getL3CacheMisses() const {
#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
    if (measure_cache_misses_) {
      return ::getL3CacheMisses(*before_state_, *after_state_);
    } else {
      return 0;
    }
#else
    return 0;
#endif
  }

  /**
   * @brief Get elapsed time and total L2/L3 cache misses between calls to
   *        start() and stop() in a single structure.
   *
   * @return A structure containing elapsed time and total L2/L3 cache misses.
   **/
  RunStats getRunStats() const {
    RunStats stats;
    stats.elapsed_time = getElapsed();
    stats.l2_misses = getL2CacheMisses();
    stats.l3_misses = getL3CacheMisses();
    return stats;
  }

 private:
  const bool measure_cache_misses_;

#ifdef QUICKSTEP_STORAGE_EXPLORER_POSIX_TIMERS_AVAILABLE
  timespec start_time_;
  timespec end_time_;
#endif

#ifdef QUICKSTEP_STORAGE_EXPLORER_WINDOWS_TIMERS_AVAILABLE
  FILETIME start_time_win_;
  FILETIME end_time_win_;
#endif

#ifdef QUICKSTEP_STORAGE_EXPLORER_USE_INTEL_PCM
  ScopedPtr<SystemCounterState> before_state_;
  ScopedPtr<SystemCounterState> after_state_;
#endif
};

}  // namespace storage_explorer
}  // namespace quickstep

#endif  // QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_TIMER_HPP_
