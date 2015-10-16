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

#include "types/TypeInstance.hpp"

#include <cstdlib>
#include <cstring>

#include "types/Type.hpp"
#include "types/TypeErrors.hpp"

using std::size_t;
using std::free;
using std::memcpy;

namespace quickstep {

std::ostream& operator<< (std::ostream &out, const TypeInstance &ti) {
  ti.putToStream(&out);
  return out;
}

size_t TypeInstance::getInstanceByteLength() const {
  if (getDataPtr() == NULL) {
    return 0;
  } else {
    if (type_.isVariableLength()) {
      return type_.determineByteLength(getDataPtr());
    } else {
      return type_.maximumByteLength();
    }
  }
}

ReferenceTypeInstance* TypeInstance::makeReference() const {
  return type_.makeReferenceTypeInstance(getDataPtr());
}

void TypeInstance::copyInto(void *destination) const {
  DEBUG_ASSERT(destination != NULL);
  DEBUG_ASSERT(getDataPtr() != NULL);
  memcpy(destination, getDataPtr(), getInstanceByteLength());
}

void TypeInstance::putToStream(std::ostream *stream) const {
  if (isNull()) {
    *stream << "NULL";
  } else {
    putToStreamUnsafe(stream);
  }
}

ReferenceTypeInstance::ReferenceTypeInstance(const Type &type, const void *data)
    : TypeInstance(type), data_(data) {
  DEBUG_ASSERT((data != NULL) || (type.isNullable()));
}

LiteralTypeInstance* TypeInstance::makeCoercedCopy(const Type &coerced_type) const {
  if (type_.isCoercibleTo(coerced_type)) {
    return coerced_type.makeCoercedCopy(*this);
  }
  FATAL_ERROR("LiteralTypeInstance::makeCoercedCopy() called with uncoercible type.");
}

NullLiteralTypeInstance::NullLiteralTypeInstance(const Type &type)
    : LiteralTypeInstance(type) {
  DEBUG_ASSERT(type.isNullable());
}

PtrBasedLiteralTypeInstance::~PtrBasedLiteralTypeInstance() {
  if (data_ != NULL) {
    free(data_);
  }
}

}  // namespace quickstep
