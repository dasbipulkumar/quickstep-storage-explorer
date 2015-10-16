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

#include "types/TypeErrors.hpp"

#include <cstdarg>

#include "types/Operation.hpp"
#include "types/Type.hpp"

using std::va_list;

namespace quickstep {

OperationInapplicableToType::OperationInapplicableToType(const Operation &op, const int num_types, ...)
    : message_("OperationInapplicableToType: Operation ") {
  message_.append(op.getName());
  message_.append(" can not be applied to type");
  if (num_types == 1) {
    message_.append(" ");
  } else {
    message_.append("s ");
  }

  va_list types;
  va_start(types, num_types);
  for (int i = 0; i < num_types; ++i) {
    const Type *type = va_arg(types, const Type*);
    message_.append(type->getName());
    if (i != num_types - 1) {
      message_.append(", ");
    }
  }
  va_end(types);
}

}  // namespace quickstep
