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

#ifndef QUICKSTEP_THREADING_WINDOWS_MUTEX_HPP_
#define QUICKSTEP_THREADING_WINDOWS_MUTEX_HPP_

#include "threading/Mutex.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Threading
 *  @{
 */

/**
 * @brief Implementation of RecursiveMutex using MS Windows threads.
 **/
class RecursiveMutexImplWindows : public MutexInterface {
 public:
  RecursiveMutexImplWindows();
  ~RecursiveMutexImplWindows();

  void lock();
  void unlock();

 private:
  // I feel bad about using an untyped pointer, but I'd feel worse about
  // including windows.h in a header.
  void *critical_section_ptr_;

  DISALLOW_COPY_AND_ASSIGN(RecursiveMutexImplWindows);
};
typedef RecursiveMutexImplWindows RecursiveMutex;

// For debug builds, use an implementation which actually prevents recursive
// locking. For release builds, don't bother (it's faster).
#ifdef QUICKSTEP_DEBUG
/**
 * @brief Implementation of Mutex using MS Windows threads.
 **/
class MutexImplWindows : public MutexInterface {
 public:
  MutexImplWindows()
      : held_(false) {
  }

  ~MutexImplWindows() {
  }

  inline void lock() {
    internal_mutex_.lock();
    DEBUG_ASSERT(!held_);
    held_ = true;
  }

  inline void unlock() {
    DEBUG_ASSERT(held_);
    held_ = false;
    internal_mutex_.unlock();
  }

 private:
  RecursiveMutexImplWindows internal_mutex_;
  bool held_;

  DISALLOW_COPY_AND_ASSIGN(MutexImplWindows);
};
typedef MutexImplWindows Mutex;
#else
typedef RecursiveMutexImplWindows Mutex;
#endif

/**
 * @brief Implementation of MutexLock and RecursiveMutexLock using MS Windows
 *        threads.
 **/
template <class MutexType>
class MutexLockImplWindows : public MutexLockInterface {
 public:
  explicit inline MutexLockImplWindows(MutexType &mutex)  // NOLINT - c++11-style interface
      : mutex_ptr_(&mutex) {
    mutex_ptr_->lock();
  }

  inline ~MutexLockImplWindows() {
    mutex_ptr_->unlock();
  }

 private:
  MutexType *mutex_ptr_;

  DISALLOW_COPY_AND_ASSIGN(MutexLockImplWindows);
};
typedef MutexLockImplWindows<Mutex> MutexLock;
typedef MutexLockImplWindows<RecursiveMutex> RecursiveMutexLock;

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_THREADING_WINDOWS_MUTEX_HPP_
