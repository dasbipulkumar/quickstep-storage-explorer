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

#ifndef QUICKSTEP_THREADING_POSIX_MUTEX_HPP_
#define QUICKSTEP_THREADING_POSIX_MUTEX_HPP_

#include <pthread.h>

#include "threading/Mutex.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

namespace threading_posix_internal {

// This wrapper class handles creating and destroying the global
// pthread_mutexattr_t instances at program start and exit, respectively.
class MutexAttrWrapper {
 public:
  static const pthread_mutexattr_t* GetNormal();
  static const pthread_mutexattr_t* GetRecursive();

 private:
  explicit MutexAttrWrapper(bool recursive);
  ~MutexAttrWrapper();

  pthread_mutexattr_t internal_mutex_attr_;
};

static const pthread_mutexattr_t* kMutexattrNormal = MutexAttrWrapper::GetNormal();
static const pthread_mutexattr_t* kMutexattrRecursive = MutexAttrWrapper::GetRecursive();

}  // namespace threading_posix_internal

/** \addtogroup Threading
 *  @{
 */

/**
 * @brief Implementation of Mutex and RecursiveMutex using POSIX threads.
 **/
template <bool RECURSIVE>
class MutexImplPosix : public MutexInterface {
 public:
  inline MutexImplPosix() {
    if (RECURSIVE) {
      DO_AND_DEBUG_ASSERT_ZERO(pthread_mutex_init(&internal_mutex_,
                                                  threading_posix_internal::kMutexattrRecursive));
    } else {
      DO_AND_DEBUG_ASSERT_ZERO(pthread_mutex_init(&internal_mutex_,
                                                  threading_posix_internal::kMutexattrNormal));
    }
  }

  inline ~MutexImplPosix() {
    DO_AND_DEBUG_ASSERT_ZERO(pthread_mutex_destroy(&internal_mutex_));
  }

  inline void lock() {
    DO_AND_DEBUG_ASSERT_ZERO(pthread_mutex_lock(&internal_mutex_));
  }

  inline void unlock() {
    DO_AND_DEBUG_ASSERT_ZERO(pthread_mutex_unlock(&internal_mutex_));
  }

 private:
  pthread_mutex_t internal_mutex_;

  DISALLOW_COPY_AND_ASSIGN(MutexImplPosix);
};
typedef MutexImplPosix<false> Mutex;
typedef MutexImplPosix<true> RecursiveMutex;

/**
 * @brief Implementation of MutexLock and RecursiveMutexLock using POSIX
 *        threads.
 **/
template <class MutexType>
class MutexLockImplPosix : public MutexLockInterface {
 public:
  explicit inline MutexLockImplPosix(MutexType &mutex)  // NOLINT - c++11-style interface
      : mutex_ptr_(&mutex) {
    mutex_ptr_->lock();
  }

  inline ~MutexLockImplPosix() {
    mutex_ptr_->unlock();
  }

 private:
  MutexType *mutex_ptr_;

  DISALLOW_COPY_AND_ASSIGN(MutexLockImplPosix);
};
typedef MutexLockImplPosix<Mutex> MutexLock;
typedef MutexLockImplPosix<RecursiveMutex> RecursiveMutexLock;

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_THREADING_POSIX_MUTEX_HPP_
