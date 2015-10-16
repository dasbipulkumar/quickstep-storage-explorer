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

#ifndef QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_THREAD_AFFINITY_HPP_
#define QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_THREAD_AFFINITY_HPP_

#include "utility/Macros.hpp"

namespace quickstep {
namespace storage_explorer {

/**
 * @brief All-static class with methods for setting the logical CPU affinity of
 *        execution threads.
 **/
class ThreadAffinity {
 public:
  /**
   * @brief Bind (pin) the calling thread so that it only runs on the specified
   *        logical CPU.
   *
   * @param cpu_id The system ID of the CPU which the calling thread should be
   *        bound to.
   **/
  static void BindThisThreadToCPU(const int cpu_id);

 private:
  // Undefined private constructor. Class is all-static and should not be
  // instantiated.
  ThreadAffinity();

  DISALLOW_COPY_AND_ASSIGN(ThreadAffinity);
};

}  // namespace storage_explorer
}  // namespace quickstep

#endif  // QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_THREAD_AFFINITY_HPP_
