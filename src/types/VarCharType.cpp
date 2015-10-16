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

#include "types/VarCharType.hpp"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "types/strnlen.hpp"
#include "types/TypeErrors.hpp"
#include "utility/PtrMap.hpp"

using std::malloc;
using std::memcpy;
using std::ostringstream;
using std::pair;
using std::size_t;
using std::string;
using std::strlen;

namespace quickstep {

template <bool nullable_internal>
const VarCharType& VarCharType::InstanceInternal(const std::size_t length) {
  static PtrMap<size_t, VarCharType> instance_map;
  PtrMap<size_t, VarCharType>::iterator imit = instance_map.find(length);
  if (imit == instance_map.end()) {
    imit = instance_map.insert(length, new VarCharType(length, nullable_internal)).first;
  }
  return *(imit->second);
}

const VarCharType& VarCharType::InstanceNonNullable(const std::size_t length) {
  return InstanceInternal<false>(length);
}

const VarCharType& VarCharType::InstanceNullable(const std::size_t length) {
  return InstanceInternal<true>(length);
}

size_t VarCharType::estimateAverageByteLength() const {
  if (length_ > 160) {
    return 80;
  } else {
    return (length_ / 2) + 1;
  }
}

size_t VarCharType::determineByteLength(const void *data) const {
  DEBUG_ASSERT(nullable_ || (data != NULL));
  if (data == NULL) {
    return 0;
  } else {
    return strlen(static_cast<const char*>(data)) + 1;
  }
}

bool VarCharType::isSafelyCoercibleTo(const Type &other) const {
  if (nullable_ && !other.isNullable()) {
    return false;
  }
  if (other.getTypeID() == kChar) {
    if (length_ <= other.maximumByteLength()) {
      return true;
    } else {
      return false;
    }
  } else if (other.getTypeID() == kVarChar) {
    if (length_ < other.maximumByteLength()) {
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }
}

ReferenceTypeInstance* VarCharType::makeReferenceTypeInstance(const void *data) const {
  DEBUG_ASSERT(nullable_ || (data != NULL));
  return new VarCharReferenceTypeInstance(*this, data);
}

string VarCharType::getName() const {
  ostringstream buf;
  buf << "VarChar(" << length_ << ")";
  return buf.str();
}

LiteralTypeInstance* VarCharType::makeLiteralTypeInstance(const char *value) const {
  return new VarCharLiteralTypeInstance(*this, value, length_);
}

LiteralTypeInstance* VarCharType::makeCoercedCopy(const TypeInstance &original) const {
  DEBUG_ASSERT(nullable_ || (!original.isNull()));
  DEBUG_ASSERT(original.getType().isCoercibleTo(*this));
  if (original.isNull()) {
    return makeNullLiteralTypeInstance();
  } else {
    DEBUG_ASSERT(original.supportsAsciiStringInterface());
    return new VarCharLiteralTypeInstance(*this,
                                          static_cast<const char*>(original.getDataPtr()),
                                          original.asciiStringMaximumLength());
  }
}

LiteralTypeInstance* VarCharReferenceTypeInstance::makeCopy() const {
  if (isNull()) {
    return new NullLiteralTypeInstance(getType());
  } else {
    return new VarCharLiteralTypeInstance(static_cast<const VarCharType&>(getType()),
                                          static_cast<const char*>(getDataPtr()),
                                          asciiStringMaximumLength());
  }
}

VarCharLiteralTypeInstance::VarCharLiteralTypeInstance(const VarCharType &type,
                                                       const char *data,
                                                       const std::size_t copy_limit)
    : PtrBasedLiteralTypeInstance(type, NULL) {
  initCopyHelper(data, copy_limit);
}

LiteralTypeInstance* VarCharLiteralTypeInstance::makeCopy() const {
  if (isNull()) {
    return new NullLiteralTypeInstance(getType());
  } else {
    return new VarCharLiteralTypeInstance(static_cast<const VarCharType&>(getType()),
                                          static_cast<const char*>(getDataPtr()),
                                          asciiStringMaximumLength());
  }
}

void VarCharLiteralTypeInstance::initCopyHelper(const char *data, const std::size_t copy_limit) {
  size_t datalen = strnlen(data, copy_limit);
  size_t maxbytelen = getType().maximumByteLength();
  if (datalen + 1 < maxbytelen) {
    data_ = malloc(datalen + 1);
    memcpy(data_, data, datalen);
    static_cast<char*>(data_)[datalen] = '\0';
  } else {
    data_ = malloc(maxbytelen);
    memcpy(data_, data, maxbytelen - 1);
    static_cast<char*>(data_)[maxbytelen - 1] = '\0';
  }
}

}  // namespace quickstep
