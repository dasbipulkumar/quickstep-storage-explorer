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

#include "storage/StorageErrors.hpp"

#include <sstream>
#include <string>

using std::ostringstream;

namespace quickstep {

BlockMemoryTooSmall::BlockMemoryTooSmall(const std::string &block_type,
                                         const std::size_t block_size) {
  ostringstream oss("BlockMemoryTooSmall: ");
  oss << block_size << " bytes is too small to create a block/subblock of type " << block_type;
  message_ = oss.str();
}

TupleTooLargeForBlock::TupleTooLargeForBlock(const std::size_t tuple_size)
    : tuple_size_(tuple_size) {
  ostringstream oss("TupleTooLargeForBlock: ");
  oss << "Tuple of size " << tuple_size_ << " bytes is too large to insert into an empty block";
  message_ = oss.str();
}

}  // namespace quickstep
