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

#include "types/Comparison.hpp"

#include <cstdlib>

#include "types/BasicComparisons.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

const char *Comparison::kComparisonNames[] = {
  "Equal",
  "NotEqual",
  "Less",
  "LessOrEqual",
  "Greater",
  "GreaterOrEqual"
};

const char *Comparison::kComparisonShortNames[] = {
  "=",
  "!=",
  "<",
  "<=",
  ">",
  ">="
};

const Comparison& Comparison::GetComparison(const ComparisonID id) {
  switch (id) {
    case kEqual:
      return EqualComparison::Instance();
    case kNotEqual:
      return NotEqualComparison::Instance();
    case kLess:
      return LessComparison::Instance();
    case kLessOrEqual:
      return LessOrEqualComparison::Instance();
    case kGreater:
      return GreaterComparison::Instance();
    case kGreaterOrEqual:
      return GreaterOrEqualComparison::Instance();
    default:
      break;  // Prevent compiler from complaining about unhandled case.
  }
  FATAL_ERROR("Unknown ComparisonID");
}

}  // namespace quickstep
