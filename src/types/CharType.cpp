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

#include "types/CharType.hpp"

#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "types/TypeErrors.hpp"
#include "utility/PtrMap.hpp"

using std::free;
using std::malloc;
using std::memcpy;
using std::ostringstream;
using std::pair;
using std::size_t;
using std::string;
using std::strncpy;

namespace quickstep {

namespace {

void putToStreamHelper(const TypeInstance &char_type_instance, std::ostream *stream) {
  DEBUG_ASSERT(stream->width() >= 0);
  size_t len = char_type_instance.asciiStringLength();
  const char *dataptr = static_cast<const char*>(char_type_instance.getDataPtr());

  // The cast below avoids a gcc warning, since std::streamsize is signed.
  if (static_cast<size_t>(stream->width()) > len) {
    for (unsigned int i = 0; i < stream->width() - len; ++i) {
      stream->put(' ');
    }
  }

  for (size_t i = 0; i < len; ++i) {
    stream->put(dataptr[i]);
  }
}

}  // anonymous namespace

template <bool nullable_internal>
const CharType& CharType::InstanceInternal(const std::size_t length) {
  static PtrMap<size_t, CharType> instance_map;
  PtrMap<size_t, CharType>::iterator imit = instance_map.find(length);
  if (imit == instance_map.end()) {
    imit = instance_map.insert(length, new CharType(length, nullable_internal)).first;
  }
  return *(imit->second);
}

const CharType& CharType::InstanceNonNullable(const std::size_t length) {
  return InstanceInternal<false>(length);
}

const CharType& CharType::InstanceNullable(const std::size_t length) {
  return InstanceInternal<true>(length);
}

std::size_t CharType::determineByteLength(const void *data) const {
  DEBUG_ASSERT(nullable_ || (data != NULL));
  if (data == NULL) {
    return 0;
  } else {
    return length_;
  }
}

bool CharType::isSafelyCoercibleTo(const Type &other) const {
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
    if (length_ < other.maximumByteLength() - 1) {
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }
}

ReferenceTypeInstance* CharType::makeReferenceTypeInstance(const void *data) const {
  DEBUG_ASSERT(nullable_ || (data != NULL));
  return new CharReferenceTypeInstance(*this, data);
}

string CharType::getName() const {
  ostringstream buf;
  buf << "Char(" << length_ << ")";
  return buf.str();
}

LiteralTypeInstance* CharType::makeLiteralTypeInstance(const char *value) const {
  return new CharLiteralTypeInstance(*this, value, length_);
}

LiteralTypeInstance* CharType::makeCoercedCopy(const TypeInstance &original) const {
  DEBUG_ASSERT(nullable_ || (!original.isNull()));
  DEBUG_ASSERT(original.getType().isCoercibleTo(*this));
  if (original.isNull()) {
    return new NullLiteralTypeInstance(*this);
  } else {
    DEBUG_ASSERT(original.supportsAsciiStringInterface());
    return new CharLiteralTypeInstance(*this,
                                       static_cast<const char*>(original.getDataPtr()),
                                       original.asciiStringMaximumLength());
  }
}

LiteralTypeInstance* CharReferenceTypeInstance::makeCopy() const {
  if (isNull()) {
    return new NullLiteralTypeInstance(getType());
  } else {
    return new CharLiteralTypeInstance(static_cast<const CharType&>(getType()),
                                       static_cast<const char*>(getDataPtr()),
                                       asciiStringMaximumLength());
  }
}

void CharReferenceTypeInstance::putToStreamUnsafe(std::ostream *stream) const {
  putToStreamHelper(*this, stream);
}

void CharLiteralTypeInstance::putToStreamUnsafe(std::ostream *stream) const {
  putToStreamHelper(*this, stream);
}

CharLiteralTypeInstance::CharLiteralTypeInstance(const CharType &type,
                                                 const char *data,
                                                 const std::size_t copy_limit)
    : PtrBasedLiteralTypeInstance(type, malloc(type.maximumByteLength())) {
  initCopyHelper(data, copy_limit);
}

LiteralTypeInstance* CharLiteralTypeInstance::makeCopy() const {
  return new CharLiteralTypeInstance(static_cast<const CharType&>(getType()),
                                     static_cast<const char*>(getDataPtr()),
                                     asciiStringMaximumLength());
}

void CharLiteralTypeInstance::initCopyHelper(const char *data, const std::size_t copy_limit) {
  if (copy_limit < getType().maximumByteLength()) {
    strncpy(static_cast<char*>(data_), data, copy_limit);
    static_cast<char*>(data_)[copy_limit] = '\0';
  } else {
    strncpy(static_cast<char*>(data_), data, getType().maximumByteLength());
  }
}

}  // namespace quickstep
