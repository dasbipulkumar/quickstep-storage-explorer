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

#ifndef QUICKSTEP_TYPES_TYPE_HPP_
#define QUICKSTEP_TYPES_TYPE_HPP_

#include <cstddef>
#include <ios>
#include <string>

#include "utility/Macros.hpp"

namespace quickstep {

class TypeInstance;
class LiteralTypeInstance;
class NullLiteralTypeInstance;
class ReferenceTypeInstance;

/** \addtogroup Types
 *  @{
 */

/**
 * @brief A type in Quickstep's type system. Each exact concrete Type is a
 *        singleton.
 **/
class Type {
 public:
  /**
   * @brief Categories of intermediate supertypes.
   **/
  enum SuperTypeID {
    kNumeric = 0,  // Fixed-length numeric types (Int, Long, Float, Double)
    kAsciiString,  // ASCII strings (Char, VarChar)
    kOther         // Others
  };

  /**
   * @brief Concrete Types.
   **/
  enum TypeID {
    kInt = 0,
    kLong,
    kFloat,
    kDouble,
    kChar,
    kVarChar,
    kNumTypeIDs  // Not a real TypeID, exists for counting purposes.
  };

  /**
   * @brief Names of types in the same order as TypeID.
   * @note Defined out-of-line in Type.cpp
   **/
  static const char *kTypeNames[kNumTypeIDs];

  /**
   * @brief Virtual destructor.
   **/
  virtual ~Type() {
  }

  /**
   * @brief Factory method to get a Type by its TypeID.
   * @note This version is for Types without a length parameter (currently
   *       IntType, LongType, FloatType, and DoubleType). It is an error to
   *       call this with a Type which requires a length parameter.
   *
   * @param id The id of the desired Type.
   * @param nullable Whether to get the nullable version of the Type.
   * @return The Type corresponding to id.
   **/
  static const Type& GetType(const TypeID id, const bool nullable = false);

  /**
   * @brief Factory method to get a Type by its TypeID and length.
   * @note This version is for Types with a length parameter (currently
   *       CharType and VarCharType). It is an error to call this with a Type
   *       which does not require a length parameter.
   *
   * @param id The id of the desired Type.
   * @param length The length parameter of the desired Type.
   * @param nullable Whether to get the nullable version of the Type.
   * @return The Type corresponding to id and length.
   **/
  static const Type& GetType(const TypeID id, const std::size_t length, const bool nullable = false);

  /**
   * @brief Determine which of two types is most specific, i.e. which the other
   *        isSafelyCoercibleTo().
   *
   * @param first The first type to check.
   * @param second The second type to check.
   * @return The most precise type, or NULL if neither Type
   *         isSafelyCoercibleTo() the other.
   **/
  static const Type* GetMostSpecificType(const Type &first, const Type &second);

  /**
   * @brief Determine a type, if any exists, which both arguments can be safely
   *        coerced to. It is possible that the resulting type may not be
   *        either argument.
   *
   * @param first The first type to check.
   * @param second The second type to check.
   * @return The unifying type, or NULL if none exists.
   **/
  static const Type* GetUnifyingType(const Type &first, const Type &second);

  /**
   * @brief Determine what supertype this type belongs to.
   *
   * @return The ID of the supertype this type belongs to.
   **/
  virtual SuperTypeID getSuperTypeID() const = 0;

  /**
   * @brief Determine the TypeID of this object.
   *
   * @return The ID of this type.
   **/
  virtual TypeID getTypeID() const = 0;

  /**
   * @brief Determine whether this Type allows NULL values.
   *
   * @return Whether this Type allows NULLs.
   **/
  bool isNullable() const {
    return nullable_;
  }

  /**
   * @brief Get a nullable (but otherwise identical) version of this type.
   *
   * @return This Type's nullable counterpart (or this Type itself if already
   *         nullable).
   **/
  virtual const Type& getNullableVersion() const = 0;

  /**
   * @brief Get a non-nullable (but otherwise identical) version of this type.
   *
   * @return This Type's non-nullable counterpart (or this Type itself if
   *         already non-nullable).
   **/
  virtual const Type& getNonNullableVersion() const = 0;

  /**
   * @brief Determine whether data items of this type have variable
   *        byte-length.
   *
   * @return Whether this is a variable-length type.
   **/
  virtual bool isVariableLength() const = 0;

  /**
   * @brief Determine the minimum number of bytes used by data items of this
   *        type.
   * @note If isVariableLength() is false, this is equivalent to
   *       maximumByteLength().
   *
   * @return The minimum number of bytes used by any data item of this type.
   **/
  virtual std::size_t minimumByteLength() const = 0;

  /**
   * @brief Determine the maximum number of bytes used by data items of this
   *        type.
   * @note If isVariableLength() is false, this is equivalent to
   *       minimumByteLength().
   *
   * @return The maximum number of bytes used by any data item of this type.
   **/
  virtual std::size_t maximumByteLength() const = 0;

  /**
   * @brief Estimate the average number of bytes used by data items of this
   *        type. These estimates may be used in query planning and determining
   *        StorageBlock layout.
   * @note If isVariableLength() is false, this is the same as
   *       minimumByteLength() and maximumByteLength().
   *
   * @return An estimate of the average number of bytes used by data items of
   *         this type.
   **/
  virtual std::size_t estimateAverageByteLength() const = 0;

  /**
   * @brief Determine the actual number of bytes used by a data item of this
   *        type. If isVariableLength() is true, this is always the same as
   *        minimumByteLength() and maximumByteLength().
   *
   * @param data A pointer to a data item of this type.
   * @return The actual number of bytes used by the item pointed to by data (0
   *         for NULL).
   **/
  virtual std::size_t determineByteLength(const void *data) const = 0;

  /**
   * @brief Determine whether this Type is exactly the same as another.
   * @note Because all exact types are singletons, a simple pointer
   *       equality-check is usable here, but this method should be used in
   *       case this behavior might change in the future.
   *
   * @param other The Type to check for equality.
   * @return Whether this Type and other are the same.
   **/
  bool equals(const Type &other) const {
    return (this == &other);
  }

  /**
   * @brief Determine whether data items of this type can be coerced (used as
   *        or converted to) another Type.
   * @note This method only determines whether coercion is possible (truncation
   *       or loss of precision may still occur). To determine if coercion is
   *       possible without loss of precision, use isSafelyCoercibleTo()
   *       instead.
   * @note It is NOT possible to coerce a nullable type to a non-nullable type,
   *       even if coercion would otherwise be possible.
   *
   * @param other The target Type for coercion.
   * @return true if coercion is supported, false otherwise.
   **/
  virtual bool isCoercibleTo(const Type &other) const = 0;

  /**
   * @brief Determine whether data items of this type can be coerced (used as
   *        or converted to) another Type without truncation or loss of
   *        precision.
   * @note It is NOT possible to coerce a nullable type to a non-nullable type,
   *       even if coercion would otherwise be possible.
   * @note Integer types are safely coercible to other integer or
   *       floating-poin types of equal or greater length.
   * @note Floating-point types are safely coercible to other floating-point
   *       types of equal or greater precision.
   * @note ASCII string types are safely coercible to other ASCII string types
   *       of equal or greater maximum length.
   * @warning Integer types are considered safely coercible to floating-point
   *          types of the same length, although for some large integer values
   *          this can lead to rounding off some of the lowest-magnitude binary
   *          digits.
   *
   * @param other The target Type for coercion.
   * @return true if coercion is supported, false otherwise.
   **/
  virtual bool isSafelyCoercibleTo(const Type &other) const = 0;

  /**
   * @brief Create a ReferenceTypeInstance from a pointer to a data item.
   *
   * @param data A pointer to a data item of this Type.
   * @return A ReferenceTypeInstance which refers to data (it is only valid so
   *         long as the underlying pointer is valid).
   **/
  virtual ReferenceTypeInstance* makeReferenceTypeInstance(const void *data) const = 0;

  /**
   * @brief Create a NULL literal of this Type.
   * @warning This should only be called for nullable Types.
   *
   * @return A NULL value of this Type.
   **/
  NullLiteralTypeInstance* makeNullLiteralTypeInstance() const;

  /**
   * @brief Get the name of this Type.
   * @note Default version just returns the name from kTypeNames. Subclasses
   *       may override this to provided additional information, like lengths.
   *
   * @return The human-readable name of this Type.
   **/
  virtual std::string getName() const {
    return kTypeNames[getTypeID()];
  }

  /**
   * @brief Determine the maximum number of characters it takes to print a
   *        value of this Type.
   *
   * @return The maximum number of characters used to print a value of this
   *         Type.
   **/
  virtual std::streamsize getPrintWidth() const = 0;

 protected:
  /**
   * @brief Make a copy of a TypeInstance, coerced to this Type.
   * @warning This is not checked for safety if not using a debug build.
   *          isCoercibleTo() should always be used to check for safety first.
   *
   * @param original An TypeInstance to coerce to this Type.
   * @return A copy of original, coerced to this Type.
   **/
  virtual LiteralTypeInstance* makeCoercedCopy(const TypeInstance &original) const = 0;

  explicit Type(const bool nullable)
      : nullable_(nullable) {
  }

  const bool nullable_;

 private:
  friend class TypeInstance;
  friend class LiteralTypeInstance;

  DISALLOW_COPY_AND_ASSIGN(Type);
};

/**
 * @brief A superclass for ASCII string types.
 **/
class AsciiStringSuperType : public Type {
 public:
  SuperTypeID getSuperTypeID() const {
    return kAsciiString;
  }

  bool isCoercibleTo(const Type &other) const;

  /**
   * @brief Get the character-length of this string type.
   *
   * @return The maximum length of a string of this type.
   **/
  std::size_t getStringLength() const {
    return length_;
  }

 protected:
  AsciiStringSuperType(const std::size_t length, const bool nullable)
      : Type(nullable), length_(length) {
  }

  const std::size_t length_;

 private:
  DISALLOW_COPY_AND_ASSIGN(AsciiStringSuperType);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_TYPE_HPP_
