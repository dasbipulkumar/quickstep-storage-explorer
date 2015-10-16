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

#include "threading/windows/Thread.hpp"

#include <windows.h>
#ifdef QUICKSTEP_DEBUG
#include <cassert>
#endif

#include "utility/Macros.hpp"

namespace quickstep {

namespace threading_windows_internal {
class WindowsExecHelper {
 public:
  static DWORD WINAPI executeRunMethodForThread(LPVOID thread_ptr) {
    static_cast<ThreadInterface*>(thread_ptr)->run();
    return 0;
  }
};
}  // namespace threading_windows_internal

ThreadImplWindows::ThreadImplWindows() {
  thread_handle_ = CreateThread(NULL,
                                0,
                                threading_windows_internal::WindowsExecHelper::executeRunMethodForThread,
                                this,
                                CREATE_SUSPENDED,
                                NULL);
  DEBUG_ASSERT(thread_handle_ != NULL);
}

ThreadImplWindows::~ThreadImplWindows() {
  CloseHandle(thread_handle_);
}

void ThreadImplWindows::start() {
#ifdef QUICKSTEP_DEBUG
  assert(ResumeThread(thread_handle_) == 1);
#else
  ResumeThread(thread_handle_);
#endif
}

void ThreadImplWindows::join() {
  WaitForSingleObject(thread_handle_, INFINITE);
}

}  // namespace quickstep
