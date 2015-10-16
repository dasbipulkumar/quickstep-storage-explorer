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
#ifndef QUICKSTEP_THREADING_THREAD_HPP_
#define QUICKSTEP_THREADING_THREAD_HPP_

#include "threading/ThreadingConfig.h"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Threading
 *  @{
 */

namespace threading_internal {
  void executeRunMethodForThreadReturnNothing(void *thread_ptr);
  void* executeRunMethodForThreadReturnNull(void *thread_ptr);
}  // namespace threading_internal

namespace threading_windows_internal {
  class WindowsExecHelper;
}  // namespace threading_windows_internal

/**
 * @brief An independent thread of execution.
 * @note This interface exists to provide a central point of documentation for
 *       platform-specific Thread implementations, you should never use it
 *       directly. Instead, simply use the Thread class, which will be
 *       typedefed to the appropriate implementation.
 **/
class ThreadInterface {
 public:
  inline ThreadInterface() {
  }

  /**
   * @brief Virtual destructor.
   **/
  virtual ~ThreadInterface() = 0;

  /**
   * @brief Start executing this Thread's run() method in an independent
   *        thread.
   * @warning Call this only once.
   **/
  virtual void start() = 0;

  /**
   * @brief Block until this thread stops running (i.e. until the run()
   *        method returns).
   * @warning You must call start() before calling join().
   **/
  virtual void join() = 0;

 protected:
  /**
   * @brief This method is invoked when start() is called. It should be
   *        overridden with the actual code which you wish to execute in the
   *        independent thread.
   **/
  virtual void run() = 0;

 private:
  friend void threading_internal::executeRunMethodForThreadReturnNothing(void *thread_ptr);
  friend void* threading_internal::executeRunMethodForThreadReturnNull(void *thread_ptr);
  friend class threading_windows_internal::WindowsExecHelper;

  DISALLOW_COPY_AND_ASSIGN(ThreadInterface);
};

/** @} */

}  // namespace quickstep

#ifdef QUICKSTEP_HAVE_CPP11_THREADS
#include "threading/cpp11/Thread.hpp"
#endif

#ifdef QUICKSTEP_HAVE_POSIX_THREADS
#include "threading/posix/Thread.hpp"
#endif

#ifdef QUICKSTEP_HAVE_WINDOWS_THREADS
#include "threading/windows/Thread.hpp"
#endif

#endif  // QUICKSTEP_THREADING_THREAD_HPP_
