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

#include "threading/posix/Mutex.hpp"

namespace quickstep {
namespace threading_posix_internal {

const pthread_mutexattr_t* MutexAttrWrapper::GetNormal() {
  static MutexAttrWrapper wrapper(false);
  return &wrapper.internal_mutex_attr_;
}

const pthread_mutexattr_t *MutexAttrWrapper::GetRecursive() {
  static MutexAttrWrapper wrapper(true);
  return &wrapper.internal_mutex_attr_;
}

MutexAttrWrapper::MutexAttrWrapper(bool recursive) {
  DO_AND_DEBUG_ASSERT_ZERO(pthread_mutexattr_init(&internal_mutex_attr_));
  if (recursive) {
    DO_AND_DEBUG_ASSERT_ZERO(pthread_mutexattr_settype(&internal_mutex_attr_, PTHREAD_MUTEX_RECURSIVE));
  } else {
    DO_AND_DEBUG_ASSERT_ZERO(pthread_mutexattr_settype(&internal_mutex_attr_, PTHREAD_MUTEX_NORMAL));
  }
}

MutexAttrWrapper::~MutexAttrWrapper() {
  pthread_mutexattr_destroy(&internal_mutex_attr_);
}

}  // namespace threading_posix_internal
}  // namespace quickstep
