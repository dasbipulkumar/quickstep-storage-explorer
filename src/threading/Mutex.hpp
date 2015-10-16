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

#ifndef QUICKSTEP_THREADING_MUTEX_HPP_
#define QUICKSTEP_THREADING_MUTEX_HPP_

#include "threading/ThreadingConfig.h"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Threading
 *  @{
 */

/**
 * @brief A Mutex. A normal Mutex may only be locked once, a RecursiveMutex may
 *        be locked an unlimited number of times by one thread, so long as it
 *        is unlocked the same number of times.
 * @note This interface exists to provide a central point of documentation for
 *       platform-specific Mutex and RecursiveMutex implementations, you should
 *       never use it directly. Instead, simply use the Mutex or RecursiveMutex
 *       class, which will be typedefed to the appropriate implementation.
 **/
class MutexInterface {
 public:
  /**
   * @brief Virtual destructor.
   **/
  virtual ~MutexInterface() = 0;

  /**
   * @brief Lock this Mutex. If the mutex is locked by another thread,
   *        execution will block until the Mutex becomes available.
   * @note Normal Mutex version only: It is an error to call lock() while
   *       already holding the Mutex.
   * @note RecursiveMutex version only: May be called an unlimited number of
   *       times by a single thread, so long as unlock() is called the same
   *       number of times.
   **/
  virtual void lock() = 0;

  /**
   * @brief Unlock this Mutex. It is an error to unlock a Mutex which is not
   *        locked by the current thread.
   **/
  virtual void unlock() = 0;
};

/**
 * @brief A scoped lock-holder for a Mutex. Locks a Mutex when it is
 *        constructed, and unlocks the Mutex when it goes out of scope.
 * @note This interface exists to provide a central point of documentation
 *       for platform-specific MutexLock and RecursiveMutexLock
 *       implementations, you should never use it directly. Instead, simply use
 *       the MutexLock or RecursiveMutexLock class, which will be typedefed to
 *       the appropriate implementation.
 **/
class MutexLockInterface {
 public:
  /**
   * @brief Virtual destructor. Unlocks the held Mutex.
   **/
  virtual ~MutexLockInterface() = 0;
};

/** @} */

}  // namespace quickstep

#ifdef QUICKSTEP_HAVE_CPP11_THREADS
#include "threading/cpp11/Mutex.hpp"
#endif

#ifdef QUICKSTEP_HAVE_POSIX_THREADS
#include "threading/posix/Mutex.hpp"
#endif

#ifdef QUICKSTEP_HAVE_WINDOWS_THREADS
#include "threading/windows/Mutex.hpp"
#endif

#endif  // QUICKSTEP_THREADING_MUTEX_HPP_
