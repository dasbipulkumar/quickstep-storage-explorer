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

#ifndef QUICKSTEP_THREADING_POSIX_THREAD_HPP_
#define QUICKSTEP_THREADING_POSIX_THREAD_HPP_

#include <pthread.h>

#include "threading/Thread.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Threading
 *  @{
 */

/**
 * @brief Implementation of Thread using POSIX threads.
 **/
class ThreadImplPosix : public ThreadInterface {
 public:
  inline ThreadImplPosix() {
  }

  inline virtual ~ThreadImplPosix() {
  }

  inline void start() {
    DO_AND_DEBUG_ASSERT_ZERO(pthread_create(&internal_thread_,
                                            NULL,
                                            threading_internal::executeRunMethodForThreadReturnNull,
                                            this));
  }

  inline void join() {
    DO_AND_DEBUG_ASSERT_ZERO(pthread_join(internal_thread_, NULL));
  }

 private:
  pthread_t internal_thread_;

  DISALLOW_COPY_AND_ASSIGN(ThreadImplPosix);
};
typedef ThreadImplPosix Thread;

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_THREADING_POSIX_THREAD_HPP_
