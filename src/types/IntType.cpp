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

#include "types/IntType.hpp"

namespace quickstep {

bool IntType::isSafelyCoercibleTo(const Type &other) const {
  if (nullable_ && !other.isNullable()) {
    return false;
  }
  switch (other.getTypeID()) {
    case kInt:
    case kLong:
    case kFloat:
    case kDouble:
      return true;
    default:
      return false;
  }
}

LiteralTypeInstance* IntType::makeCoercedCopy(const TypeInstance &original) const {
  DEBUG_ASSERT(nullable_ || (!original.isNull()));

  if (original.isNull()) {
    return new NullLiteralTypeInstance(*this);
  } else {
    DEBUG_ASSERT(original.supportsNumericInterface());
    return makeLiteralTypeInstance(original.numericGetIntValue());
  }
}

}  // namespace quickstep
