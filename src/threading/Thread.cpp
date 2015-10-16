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

#include "threading/Thread.hpp"

#include <cstddef>

namespace quickstep {

ThreadInterface::~ThreadInterface() {}

namespace threading_internal {
void executeRunMethodForThreadReturnNothing(void *thread_ptr) {
  static_cast<ThreadInterface*>(thread_ptr)->run();
}

void *executeRunMethodForThreadReturnNull(void *thread_ptr) {
  static_cast<ThreadInterface*>(thread_ptr)->run();
  return NULL;
}
}  // namespace threading_internal

}  // namespace quickstep
