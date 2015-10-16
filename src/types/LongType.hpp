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

#ifndef QUICKSTEP_TYPES_LONG_TYPE_HPP_
#define QUICKSTEP_TYPES_LONG_TYPE_HPP_

#include <ios>

#include "types/NumericSuperType.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

// Forward reference for friending purposes.
class LongReferenceTypeInstance;

/**
 * @brief A literal of LongType.
 **/
class LongLiteralTypeInstance : public NumericLiteralTypeInstance<std::int64_t> {
 private:
  LongLiteralTypeInstance(const Type &type, const std::int64_t value)
      : NumericLiteralTypeInstance<std::int64_t>(type, value) {
  }

  // These classes need access to the constructor.
  friend class NumericReferenceTypeInstance<std::int64_t, LongLiteralTypeInstance>;
  friend class NumericSuperType<std::int64_t, LongReferenceTypeInstance, LongLiteralTypeInstance>;

  DISALLOW_COPY_AND_ASSIGN(LongLiteralTypeInstance);
};

/**
 * @brief A reference of LongType.
 **/
class LongReferenceTypeInstance : public NumericReferenceTypeInstance<std::int64_t, LongLiteralTypeInstance> {
 private:
  LongReferenceTypeInstance(const Type &type, const void *data)
      : NumericReferenceTypeInstance<std::int64_t, LongLiteralTypeInstance>(type, data) {
  }

  // Needs access to the constructor.
  friend class NumericSuperType<std::int64_t, LongReferenceTypeInstance, LongLiteralTypeInstance>;

  DISALLOW_COPY_AND_ASSIGN(LongReferenceTypeInstance);
};

/**
 * @brief A type representing a 64-bit integer.
 **/
class LongType : public NumericSuperType<std::int64_t, LongReferenceTypeInstance, LongLiteralTypeInstance> {
 public:
  /**
   * @brief Get a reference to the non-nullable singleton instance of this
   *        Type.
   *
   * @return A reference to the non-nullable singleton instance of this Type.
   **/
  static const LongType& InstanceNonNullable() {
    static LongType instance(false);
    return instance;
  }

  /**
   * @brief Get a reference to the nullable singleton instance of this Type.
   *
   * @return A reference to the nullable singleton instance of this Type.
   **/
  static const LongType& InstanceNullable() {
    static LongType instance(true);
    return instance;
  }

  /**
   * @brief Get a reference to a singleton instance of this Type.
   *
   * @param nullable Whether to get the nullable version of this Type.
   * @return A reference to the desired singleton instance of this Type.
   **/
  static const LongType& Instance(const bool nullable) {
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
    return kLong;
  }

  bool isSafelyCoercibleTo(const Type &other) const;

  std::streamsize getPrintWidth() const {
    return 20;
  }

 protected:
  LiteralTypeInstance* makeCoercedCopy(const TypeInstance &original) const;

 private:
  explicit LongType(const bool nullable)
      : NumericSuperType<std::int64_t, LongReferenceTypeInstance, LongLiteralTypeInstance>(nullable) {
  }

  DISALLOW_COPY_AND_ASSIGN(LongType);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_LONG_TYPE_HPP_
