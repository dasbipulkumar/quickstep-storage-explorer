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

#ifndef QUICKSTEP_TYPES_INT_TYPE_HPP_
#define QUICKSTEP_TYPES_INT_TYPE_HPP_

#include <ios>

#include "types/NumericSuperType.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

// Forward reference for friending purposes.
class IntReferenceTypeInstance;

/**
 * @brief A literal of IntType.
 **/
class IntLiteralTypeInstance : public NumericLiteralTypeInstance<int> {
 private:
  IntLiteralTypeInstance(const Type &type, const int value)
      : NumericLiteralTypeInstance<int>(type, value) {
  }

  // These classes need access to the constructor.
  friend class NumericReferenceTypeInstance<int, IntLiteralTypeInstance>;
  friend class NumericSuperType<int, IntReferenceTypeInstance, IntLiteralTypeInstance>;

  DISALLOW_COPY_AND_ASSIGN(IntLiteralTypeInstance);
};

/**
 * @brief A reference of IntType.
 **/
class IntReferenceTypeInstance : public NumericReferenceTypeInstance<int, IntLiteralTypeInstance> {
 private:
  IntReferenceTypeInstance(const Type &type, const void *data)
      : NumericReferenceTypeInstance<int, IntLiteralTypeInstance>(type, data) {
  }

  // Needs access to the constructor.
  friend class NumericSuperType<int, IntReferenceTypeInstance, IntLiteralTypeInstance>;

  DISALLOW_COPY_AND_ASSIGN(IntReferenceTypeInstance);
};

/**
 * @brief A type representing a 32-bit integer.
 **/
class IntType : public NumericSuperType<int, IntReferenceTypeInstance, IntLiteralTypeInstance> {
 public:
  /**
   * @brief Get a reference to the non-nullable singleton instance of this
   *        Type.
   *
   * @return A reference to the non-nullable singleton instance of this Type.
   **/
  static const IntType& InstanceNonNullable() {
    static IntType instance(false);
    return instance;
  }

  /**
   * @brief Get a reference to the nullable singleton instance of this Type.
   *
   * @return A reference to the nullable singleton instance of this Type.
   **/
  static const IntType& InstanceNullable() {
    static IntType instance(true);
    return instance;
  }

  /**
   * @brief Get a reference to a singleton instance of this Type.
   *
   * @param nullable Whether to get the nullable version of this Type.
   * @return A reference to the desired singleton instance of this Type.
   **/
  static const IntType& Instance(const bool nullable) {
    if (nullable) {
      return InstanceNullable();
    } else {
      return InstanceNonNullable();
    }
  }

  const Type& getNullableVersion() const {
    return InstanceNullable();
  }

  const Type& getNonNullableVersion() const {
    return InstanceNonNullable();
  }

  TypeID getTypeID() const {
    return kInt;
  }

  bool isSafelyCoercibleTo(const Type &other) const;

  std::streamsize getPrintWidth() const {
    return 11;
  }

 protected:
  LiteralTypeInstance* makeCoercedCopy(const TypeInstance &original) const;

 private:
  explicit IntType(const bool nullable)
      : NumericSuperType<int, IntReferenceTypeInstance, IntLiteralTypeInstance>(nullable) {
  }

  DISALLOW_COPY_AND_ASSIGN(IntType);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_INT_TYPE_HPP_
