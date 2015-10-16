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

#ifndef QUICKSTEP_TYPES_DOUBLE_TYPE_HPP_
#define QUICKSTEP_TYPES_DOUBLE_TYPE_HPP_

#include <ios>

#include "types/NumericSuperType.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

// Forward reference for friending purposes.
class DoubleReferenceTypeInstance;

/**
 * @brief A literal of DoubleType.
 **/
class DoubleLiteralTypeInstance : public NumericLiteralTypeInstance<double> {
 protected:
  void putToStreamUnsafe(std::ostream *stream) const;

 private:
  DoubleLiteralTypeInstance(const Type &type, const double value)
      : NumericLiteralTypeInstance<double>(type, value) {
  }

  // These classes need access to the constructor.
  friend class NumericReferenceTypeInstance<double, DoubleLiteralTypeInstance>;
  friend class NumericSuperType<double, DoubleReferenceTypeInstance, DoubleLiteralTypeInstance>;

  DISALLOW_COPY_AND_ASSIGN(DoubleLiteralTypeInstance);
};

/**
 * @brief A reference of DoubleType.
 **/
class DoubleReferenceTypeInstance : public NumericReferenceTypeInstance<double, DoubleLiteralTypeInstance> {
 protected:
  void putToStreamUnsafe(std::ostream *stream) const;

 private:
  DoubleReferenceTypeInstance(const Type &type, const void *data)
      : NumericReferenceTypeInstance<double, DoubleLiteralTypeInstance>(type, data) {
  }

  // Needs access to the constructor.
  friend class NumericSuperType<double, DoubleReferenceTypeInstance, DoubleLiteralTypeInstance>;

  DISALLOW_COPY_AND_ASSIGN(DoubleReferenceTypeInstance);
};

/**
 * @brief A type representing a double-precision floating-point number.
 **/
class DoubleType : public NumericSuperType<double, DoubleReferenceTypeInstance, DoubleLiteralTypeInstance> {
 public:
  /**
   * @brief Get a reference to the non-nullable singleton instance of this
   *        Type.
   *
   * @return A reference to the non-nullable singleton instance of this Type.
   **/
  static const DoubleType& InstanceNonNullable() {
    static DoubleType instance(false);
    return instance;
  }

  /**
   * @brief Get a reference to the nullable singleton instance of this Type.
   *
   * @return A reference to the nullable singleton instance of this Type.
   **/
  static const DoubleType& InstanceNullable() {
    static DoubleType instance(true);
    return instance;
  }

  /**
   * @brief Get a reference to a singleton instance of this Type.
   *
   * @param nullable Whether to get the nullable version of this Type.
   * @return A reference to the desired singleton instance of this Type.
   **/
  static const DoubleType& Instance(const bool nullable) {
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
    return kDouble;
  }

  bool isSafelyCoercibleTo(const Type &other) const;

  std::streamsize getPrintWidth() const {
    return 23;
  }

 protected:
  LiteralTypeInstance* makeCoercedCopy(const TypeInstance &original) const;

 private:
  explicit DoubleType(const bool nullable)
      : NumericSuperType<double, DoubleReferenceTypeInstance, DoubleLiteralTypeInstance>(nullable) {
  }

  DISALLOW_COPY_AND_ASSIGN(DoubleType);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_DOUBLE_TYPE_HPP_
