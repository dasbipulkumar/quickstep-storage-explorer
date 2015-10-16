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

#ifndef QUICKSTEP_TYPES_CHAR_TYPE_HPP_
#define QUICKSTEP_TYPES_CHAR_TYPE_HPP_

#include <ios>
#include <string>

#include "types/strnlen.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

/**
 * @brief A type representing a fixed-size ASCII string.
 **/
class CharType : public AsciiStringSuperType {
 public:
  /**
   * @brief Get a reference to the non-nullable singleton instance of this Type
   *        for the specified length.
   *
   * @param length The length parameter of the CharType.
   * @return A reference to the non-nullable singleton instance of this Type
   *         for the specified length.
   **/
  static const CharType& InstanceNonNullable(const std::size_t length);

  /**
   * @brief Get a reference to the nullable singleton instance of this Type for
   *        the specified length.
   *
   * @param length The length parameter of the CharType.
   * @return A reference to the nullable singleton instance of this Type for
   *         the specified length.
   **/
  static const CharType& InstanceNullable(const std::size_t length);

  /**
   * @brief Get a reference to the singleton instance of this Type for the
   *        specified length and nullability.
   *
   * @param length The length parameter of the CharType.
   * @param nullable Whether to get the nullable version of this Type.
   * @return A reference to the singleton instance of this Type for the
   *         specified length.
   **/
  static const CharType& Instance(const std::size_t length, bool nullable) {
    if (nullable) {
      return InstanceNullable(length);
    } else {
      return InstanceNonNullable(length);
    }
  }

  const Type& getNullableVersion() const {
    return InstanceNullable(length_);
  }

  const Type& getNonNullableVersion() const {
    return InstanceNonNullable(length_);
  }

  TypeID getTypeID() const {
    return kChar;
  }

  bool isVariableLength() const {
    return false;
  }

  std::size_t minimumByteLength() const {
    return length_;
  }

  std::size_t maximumByteLength() const {
    return length_;
  }

  std::size_t estimateAverageByteLength() const {
    return length_;
  }

  std::size_t determineByteLength(const void *data) const;

  bool isSafelyCoercibleTo(const Type &other) const;

  ReferenceTypeInstance* makeReferenceTypeInstance(const void *data) const;

  std::string getName() const;

  std::streamsize getPrintWidth() const {
    return (isNullable() && (length_ < 4)) ? 4 : length_;
  }

  /**
   * @brief Create a LiteralTypeInstance of this Type.
   *
   * @param value The literal value, which is copied.
   * @return A LiteralTypeInstance with the specified value.
   **/
  LiteralTypeInstance* makeLiteralTypeInstance(const char *value) const;

 protected:
  LiteralTypeInstance* makeCoercedCopy(const TypeInstance &original) const;

 private:
  CharType(const std::size_t length, bool nullable)
      : AsciiStringSuperType(length, nullable) {
  }

  template <bool nullable_internal>
  static const CharType& InstanceInternal(const std::size_t length);

  DISALLOW_COPY_AND_ASSIGN(CharType);
};

/**
 * @brief A reference of CharType.
 **/
class CharReferenceTypeInstance : public ReferenceTypeInstance {
 public:
  LiteralTypeInstance* makeCopy() const;

  bool supportsAsciiStringInterface() const {
    return true;
  }

  bool asciiStringGuaranteedNullTerminated() const {
    return false;
  }

  bool asciiStringNullTerminated() const {
    return asciiStringLength() != asciiStringMaximumLength();
  }

  std::size_t asciiStringMaximumLength() const {
    return getType().maximumByteLength();
  }

  std::size_t asciiStringLength() const {
    return strnlen(static_cast<const char*>(getDataPtr()), asciiStringMaximumLength());
  }

 protected:
  void putToStreamUnsafe(std::ostream *stream) const;

 private:
  CharReferenceTypeInstance(const CharType &type, const void *data)
      : ReferenceTypeInstance(type, data) {
  }

  friend class CharType;

  DISALLOW_COPY_AND_ASSIGN(CharReferenceTypeInstance);
};

/**
 * @brief A literal of CharType.
 **/
class CharLiteralTypeInstance : public PtrBasedLiteralTypeInstance {
 public:
  LiteralTypeInstance* makeCopy() const;

  bool supportsAsciiStringInterface() const {
    return true;
  }

  bool asciiStringGuaranteedNullTerminated() const {
    return false;
  }

  bool asciiStringNullTerminated() const {
    return asciiStringLength() != asciiStringMaximumLength();
  }

  std::size_t asciiStringMaximumLength() const {
    return getType().maximumByteLength();
  };

  std::size_t asciiStringLength() const {
    return strnlen(static_cast<const char*>(getDataPtr()), asciiStringMaximumLength());
  }

 protected:
  void putToStreamUnsafe(std::ostream *stream) const;

 private:
  CharLiteralTypeInstance(const CharType &type, const char *data, const std::size_t copy_limit);

  void initCopyHelper(const char *data, const std::size_t copyLimit);

  friend class CharReferenceTypeInstance;
  friend class CharType;

  DISALLOW_COPY_AND_ASSIGN(CharLiteralTypeInstance);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_CHAR_TYPE_HPP_
