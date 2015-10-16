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

#ifndef QUICKSTEP_THREADING_WINDOWS_THREAD_HPP_
#define QUICKSTEP_THREADING_WINDOWS_THREAD_HPP_

#include "threading/Thread.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Threading
 *  @{
 */

/**
 * @brief Implementation of Thread using MS Windows threads.
 **/
class ThreadImplWindows : public ThreadInterface {
 public:
  ThreadImplWindows();
  virtual ~ThreadImplWindows();

  void start();
  void join();

 private:
  void *thread_handle_;

  DISALLOW_COPY_AND_ASSIGN(ThreadImplWindows);
};
typedef ThreadImplWindows Thread;

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_THREADING_WINDOWS_THREAD_HPP_
