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

#include "types/Type.hpp"

#include "types/CharType.hpp"
#include "types/DoubleType.hpp"
#include "types/FloatType.hpp"
#include "types/IntType.hpp"
#include "types/LongType.hpp"
#include "types/TypeErrors.hpp"
#include "types/TypeInstance.hpp"
#include "types/VarCharType.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

const char *Type::kTypeNames[] = {
  "Int",
  "Long",
  "Float",
  "Double",
  "Char",
  "VarChar"
};

const Type& Type::GetType(const TypeID id, const bool nullable) {
  switch (id) {
    case kInt:
      return IntType::Instance(nullable);
    case kLong:
      return LongType::Instance(nullable);
    case kFloat:
      return FloatType::Instance(nullable);
    case kDouble:
      return DoubleType::Instance(nullable);
    default:
      FATAL_ERROR("Called Type::GetType() for a type which requires a length parameter without specifying one.");
  }
}

const Type& Type::GetType(const TypeID id, const std::size_t length, const bool nullable) {
  switch (id) {
    case kChar:
      return CharType::Instance(length, nullable);
    case kVarChar:
      return VarCharType::Instance(length, nullable);
    default:
      FATAL_ERROR("Provided a length parameter to Type::GetType() for a type which does not take one.");
  }
}

const Type* Type::GetMostSpecificType(const Type &first, const Type &second) {
  if (first.isSafelyCoercibleTo(second)) {
    return &second;
  } else if (second.isSafelyCoercibleTo(first)) {
    return &first;
  } else {
    return NULL;
  }
}

const Type* Type::GetUnifyingType(const Type &first, const Type &second) {
  const Type *unifier = NULL;
  if (first.isNullable() || second.isNullable()) {
    unifier = Type::GetMostSpecificType(first.getNullableVersion(), second.getNullableVersion());
    if (unifier == NULL) {
      if (((first.getTypeID() == kLong) && (second.getTypeID() == kFloat))
            || ((first.getTypeID() == kFloat) && (second.getTypeID() == kLong))) {
        unifier = &(DoubleType::Instance(true));
      }
    }
  } else {
    unifier = Type::GetMostSpecificType(first, second);
    if (unifier == NULL) {
      if (((first.getTypeID() == kLong) && (second.getTypeID() == kFloat))
            || ((first.getTypeID() == kFloat) && (second.getTypeID() == kLong))) {
        unifier = &(DoubleType::Instance(false));
      }
    }
  }

  return unifier;
}

bool AsciiStringSuperType::isCoercibleTo(const Type &other) const {
  if (nullable_ && !other.isNullable()) {
    return false;
  }
  if (other.getSuperTypeID() == kAsciiString) {
    return true;
  } else {
    return false;
  }
}

NullLiteralTypeInstance* Type::makeNullLiteralTypeInstance() const {
  DEBUG_ASSERT(nullable_);
  return new NullLiteralTypeInstance(*this);
}

}  // namespace quickstep
