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

#ifndef QUICKSTEP_STORAGE_STORAGE_CONSTANTS_HPP_
#define QUICKSTEP_STORAGE_STORAGE_CONSTANTS_HPP_

#include <cstddef>

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

const std::size_t kSlotSizeBytes = 0x100000;  // 1 MB
const std::size_t kAllocationChunkSizeSlots = 256;

// Should always be a power of two. 64 bytes is the cache-line size for most
// modern CPUs.
const std::size_t kCSBTreeNodeSizeBytes = 64;

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_STORAGE_CONSTANTS_HPP_
