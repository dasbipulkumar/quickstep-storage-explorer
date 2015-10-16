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

#ifndef QUICKSTEP_THREADING_CPP11_MUTEX_HPP_
#define QUICKSTEP_THREADING_CPP11_MUTEX_HPP_

#include <mutex>

#include "threading/Mutex.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

template <class InternalMutexType>
class MutexLockImplCPP11;

/** \addtogroup Threading
 *  @{
 */

/**
 * @brief Implementation of Mutex and RecursiveMutex using C++11 threads.
 **/
template <class InternalMutexType>
class MutexImplCPP11 : public MutexInterface {
 public:
  inline MutexImplCPP11() {
  }

  inline ~MutexImplCPP11() {
  }

  inline void lock() {
    internal_mutex_.lock();
  }

  inline void unlock() {
    internal_mutex_.unlock();
  }

 private:
  InternalMutexType internal_mutex_;

  friend class MutexLockImplCPP11<InternalMutexType>;

  DISALLOW_COPY_AND_ASSIGN(MutexImplCPP11);
};
typedef MutexImplCPP11<std::mutex> Mutex;
typedef MutexImplCPP11<std::recursive_mutex> RecursiveMutex;


/**
 * @brief Implementation of MutexLock and RecursiveMutexLock using C++11
 *        threads.
 **/
template <class InternalMutexType>
class MutexLockImplCPP11 : public MutexLockInterface {
 public:
  explicit inline MutexLockImplCPP11(MutexImplCPP11<InternalMutexType> &mutex)  // NOLINT - C++11-style interface
      : internal_lock_guard_(mutex.internal_mutex_) {
  }

  inline ~MutexLockImplCPP11() {
  }

 private:
  std::lock_guard<InternalMutexType> internal_lock_guard_;

  DISALLOW_COPY_AND_ASSIGN(MutexLockImplCPP11);
};
typedef MutexLockImplCPP11<std::mutex> MutexLock;
typedef MutexLockImplCPP11<std::recursive_mutex> RecursiveMutexLock;

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_THREADING_CPP11_MUTEX_HPP_
