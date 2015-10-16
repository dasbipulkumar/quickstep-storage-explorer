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

#include "threading/windows/Mutex.hpp"

#include <windows.h>

namespace quickstep {

RecursiveMutexImplWindows::RecursiveMutexImplWindows() {
  critical_section_ptr_ = new CRITICAL_SECTION();
  InitializeCriticalSection(static_cast<CRITICAL_SECTION*>(critical_section_ptr_));
}

RecursiveMutexImplWindows::~RecursiveMutexImplWindows() {
  DeleteCriticalSection(static_cast<CRITICAL_SECTION*>(critical_section_ptr_));
  delete static_cast<CRITICAL_SECTION*>(critical_section_ptr_);
}

void RecursiveMutexImplWindows::lock() {
  EnterCriticalSection(static_cast<CRITICAL_SECTION*>(critical_section_ptr_));
}

void RecursiveMutexImplWindows::unlock() {
  LeaveCriticalSection(static_cast<CRITICAL_SECTION*>(critical_section_ptr_));
}

}  // namespace quickstep
