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

#ifndef QUICKSTEP_TYPES_FLOAT_TYPE_HPP_
#define QUICKSTEP_TYPES_FLOAT_TYPE_HPP_

#include <ios>

#include "types/NumericSuperType.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

// Forward reference for friending purposes.
class FloatReferenceTypeInstance;

/**
 * @brief A literal of FloatType.
 **/
class FloatLiteralTypeInstance : public NumericLiteralTypeInstance<float> {
 protected:
  void putToStreamUnsafe(std::ostream *stream) const;

 private:
  FloatLiteralTypeInstance(const Type &type, const float value)
      : NumericLiteralTypeInstance<float>(type, value) {
  }

  // These classes need access to the constructor.
  friend class NumericReferenceTypeInstance<float, FloatLiteralTypeInstance>;
  friend class NumericSuperType<float, FloatReferenceTypeInstance, FloatLiteralTypeInstance>;

  DISALLOW_COPY_AND_ASSIGN(FloatLiteralTypeInstance);
};

/**
 * @brief A reference of FloatType.
 **/
class FloatReferenceTypeInstance : public NumericReferenceTypeInstance<float, FloatLiteralTypeInstance> {
 protected:
  void putToStreamUnsafe(std::ostream *stream) const;

 private:
  FloatReferenceTypeInstance(const Type &type, const void *data)
      : NumericReferenceTypeInstance<float, FloatLiteralTypeInstance>(type, data) {
  }

  // Needs access to the constructor.
  friend class NumericSuperType<float, FloatReferenceTypeInstance, FloatLiteralTypeInstance>;

  DISALLOW_COPY_AND_ASSIGN(FloatReferenceTypeInstance);
};

/**
 * @brief A type representing a single-precision floating-point number.
 **/
class FloatType : public NumericSuperType<float, FloatReferenceTypeInstance, FloatLiteralTypeInstance> {
 public:
  /**
   * @brief Get a reference to the non-nullable singleton instance of this
   *        Type.
   *
   * @return A reference to the non-nullable singleton instance of this Type
   **/
  static const FloatType& InstanceNonNullable() {
    static FloatType instance(false);
    return instance;
  }

  /**
   * @brief Get a reference to the nullable singleton instance of this Type
   *
   * @return A reference to the nullable singleton instance of this Type
   **/
  static const FloatType& InstanceNullable() {
    static FloatType instance(true);
    return instance;
  }

  /**
   * @brief Get a reference to a singleton instance of this Type
   *
   * @param nullable Whether to get the nullable version of this Type
   * @return A reference to the desired singleton instance of this Type
   **/
  static const FloatType& Instance(const bool nullable) {
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
    return kFloat;
  }

  bool isSafelyCoercibleTo(const Type &other) const;

  std::streamsize getPrintWidth() const {
    return 14;
  }

 protected:
  LiteralTypeInstance* makeCoercedCopy(const TypeInstance &original) const;

 private:
  explicit FloatType(const bool nullable)
      : NumericSuperType<float, FloatReferenceTypeInstance, FloatLiteralTypeInstance>(nullable) {
  }

  DISALLOW_COPY_AND_ASSIGN(FloatType);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_FLOAT_TYPE_HPP_
