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

#include "types/BasicComparisons.hpp"

#include <cstring>
#include <functional>

#include "types/AsciiStringComparators.hpp"
#include "types/NumericComparators.hpp"
#include "types/Type.hpp"
#include "types/TypeErrors.hpp"
#include "types/TypeInstance.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"

using std::int64_t;
using std::size_t;
using std::strcmp;
using std::strncmp;

namespace quickstep {

// Hide these helper functions in an anonymous namespace.
namespace {

template <template <typename LeftCppType, bool left_type_nullable,
                    typename RightCppType, bool right_type_nullable> class ComparatorType,
          typename LeftCppType,
          bool left_type_nullable>
UncheckedComparator* makeComparatorInnerHelper(const BasicComparison &comp, const Type &left, const Type &right) {
  switch (right.getTypeID()) {
    case Type::kInt:
      if (right.isNullable()) {
        return new ComparatorType<LeftCppType, left_type_nullable, int, true>();
      } else {
        return new ComparatorType<LeftCppType, left_type_nullable, int, false>();
      }
    case Type::kLong:
      if (right.isNullable()) {
        return new ComparatorType<LeftCppType, left_type_nullable, int64_t, true>();
      } else {
        return new ComparatorType<LeftCppType, left_type_nullable, int64_t, false>();
      }
    case Type::kFloat:
      if (right.isNullable()) {
        return new ComparatorType<LeftCppType, left_type_nullable, float, true>();
      } else {
        return new ComparatorType<LeftCppType, left_type_nullable, float, false>();
      }
    case Type::kDouble:
      if (right.isNullable()) {
        return new ComparatorType<LeftCppType, left_type_nullable, double, true>();
      } else {
        return new ComparatorType<LeftCppType, left_type_nullable, double, false>();
      }
    default:
      throw OperationInapplicableToType(comp, 2, &left, &right);
  }
}

template <template <typename LeftCppType, bool left_type_nullable,
                    typename RightCppType, bool right_type_nullable> class ComparatorType>
UncheckedComparator* makeComparatorOuterHelper(const BasicComparison &comp, const Type &left, const Type &right) {
  switch (left.getTypeID()) {
    case Type::kInt:
      if (left.isNullable()) {
        return makeComparatorInnerHelper<ComparatorType, int, true>(comp, left, right);
      } else {
        return makeComparatorInnerHelper<ComparatorType, int, false>(comp, left, right);
      }
    case Type::kLong:
      if (left.isNullable()) {
        return makeComparatorInnerHelper<ComparatorType, int64_t, true>(comp, left, right);
      } else {
        return makeComparatorInnerHelper<ComparatorType, int64_t, false>(comp, left, right);
      }
    case Type::kFloat:
      if (left.isNullable()) {
        return makeComparatorInnerHelper<ComparatorType, float, true>(comp, left, right);
      } else {
        return makeComparatorInnerHelper<ComparatorType, float, false>(comp, left, right);
      }
    case Type::kDouble:
      if (left.isNullable()) {
        return makeComparatorInnerHelper<ComparatorType, double, true>(comp, left, right);
      } else {
        return makeComparatorInnerHelper<ComparatorType, double, false>(comp, left, right);
      }
    default:
      throw OperationInapplicableToType(comp, 2, &left, &right);
  }
}

template <template <bool left_nullable, bool left_null_terminated, bool left_longer,
                    bool right_nullable, bool right_null_terminated, bool right_longer> class ComparatorType,
         bool left_nullable, bool left_null_terminated,
         bool right_nullable, bool right_null_terminated>
UncheckedComparator* makeStringComparatorInnerHelper(const size_t left_length, const size_t right_length) {
  if (left_length < right_length) {
    return new ComparatorType<left_nullable, left_null_terminated, false,
                              right_nullable, right_null_terminated, true>(left_length, right_length);
  } else if (left_length > right_length) {
    return new ComparatorType<left_nullable, left_null_terminated, true,
                              right_nullable, right_null_terminated, false>(left_length, right_length);
  } else {
    return new ComparatorType<left_nullable, left_null_terminated, false,
                              right_nullable, right_null_terminated, false>(left_length, right_length);
  }
}

template <template <bool left_nullable, bool left_null_terminated, bool left_longer,
                    bool right_nullable, bool right_null_terminated, bool right_longer> class ComparatorType,
         bool left_nullable, bool left_null_terminated>
UncheckedComparator* makeStringComparatorMiddleHelper(const BasicComparison &comp,
                                                      const Type &left,
                                                      const Type &right,
                                                      const size_t left_length) {
  switch (right.getTypeID()) {
    case Type::kChar:
      if (right.isNullable()) {
        return makeStringComparatorInnerHelper<ComparatorType, left_nullable, left_null_terminated, true, false>(
            left_length, right.maximumByteLength());
      } else {
        return makeStringComparatorInnerHelper<ComparatorType, left_nullable, left_null_terminated, false, false>(
            left_length, right.maximumByteLength());
      }
    case Type::kVarChar:
      if (right.isNullable()) {
        return makeStringComparatorInnerHelper<ComparatorType, left_nullable, left_null_terminated, true, true>(
            left_length, right.maximumByteLength() - 1);
      } else {
        return makeStringComparatorInnerHelper<ComparatorType, left_nullable, left_null_terminated, false, true>(
            left_length, right.maximumByteLength() - 1);
      }
    default:
      throw OperationInapplicableToType(comp, 2, &left, &right);
  }
}

template <template <bool left_nullable, bool left_null_terminated, bool left_longer,
                    bool right_nullable, bool right_null_terminated, bool right_longer> class ComparatorType>
UncheckedComparator* makeStringComparatorOuterHelper(const BasicComparison &comp,
                                                     const Type &left,
                                                     const Type &right) {
  switch (left.getTypeID()) {
    case Type::kChar:
      if (left.isNullable()) {
        return makeStringComparatorMiddleHelper<ComparatorType, true, false>(comp,
                                                                             left,
                                                                             right,
                                                                             left.maximumByteLength());
      } else {
        return makeStringComparatorMiddleHelper<ComparatorType, false, false>(comp,
                                                                              left,
                                                                              right,
                                                                              left.maximumByteLength());
      }
    case Type::kVarChar:
      if (left.isNullable()) {
        return makeStringComparatorMiddleHelper<ComparatorType, true, true>(comp,
                                                                            left,
                                                                            right,
                                                                            left.maximumByteLength() - 1);
      } else {
        return makeStringComparatorMiddleHelper<ComparatorType, false, true>(comp,
                                                                             left,
                                                                             right,
                                                                             left.maximumByteLength() - 1);
      }
    default:
      throw OperationInapplicableToType(comp, 2, &left, &right);
  }
}

}  // anonymous namespace

bool BasicComparison::canCompareTypes(const Type &left, const Type &right) const {
  switch (left.getTypeID()) {
    case Type::kInt:
    case Type::kLong:
    case Type::kFloat:
    case Type::kDouble: {
      switch (right.getTypeID()) {
        case Type::kInt:
        case Type::kLong:
        case Type::kFloat:
        case Type::kDouble:
          return true;
        default:
          return false;
      }
    }
    case Type::kChar:
    case Type::kVarChar: {
      switch (right.getTypeID()) {
        case Type::kChar:
        case Type::kVarChar:
          return true;
        default:
          return false;
      }
    }
    default:
      return false;
  }
}

template <template <typename T> class ComparisonFunctor>
bool BasicComparison::compareTypeInstancesCheckedHelper(const TypeInstance &left, const TypeInstance &right) const {
  if (!canCompareTypes(left.getType(), right.getType())) {
    throw OperationInapplicableToType(*this, 2, &left.getType(), &right.getType());
  }

  if (left.isNull() || right.isNull()) {
    return false;
  }

  if (left.getType().getSuperTypeID() == Type::kAsciiString) {
    ComparisonFunctor<int> comparison_functor;
    return comparison_functor(strcmpHelper(left, right), 0);
  } else {
    const Type *unifier = Type::GetUnifyingType(left.getType(), right.getType());
    DEBUG_ASSERT(unifier != NULL);

    switch (unifier->getTypeID()) {
      case Type::kInt: {
        DEBUG_ASSERT(left.supportsNumericInterface());
        DEBUG_ASSERT(right.supportsNumericInterface());
        ComparisonFunctor<int> comparison_functor;
        return comparison_functor(left.numericGetIntValue(), right.numericGetIntValue());
      }
      case Type::kLong: {
        DEBUG_ASSERT(left.supportsNumericInterface());
        DEBUG_ASSERT(right.supportsNumericInterface());
        ComparisonFunctor<int64_t> comparison_functor;
        return comparison_functor(left.numericGetLongValue(), right.numericGetLongValue());
      }
      case Type::kFloat: {
        DEBUG_ASSERT(left.supportsNumericInterface());
        DEBUG_ASSERT(right.supportsNumericInterface());
        ComparisonFunctor<float> comparison_functor;
        return comparison_functor(left.numericGetFloatValue(), right.numericGetFloatValue());
      }
      case Type::kDouble: {
        DEBUG_ASSERT(left.supportsNumericInterface());
        DEBUG_ASSERT(right.supportsNumericInterface());
        ComparisonFunctor<double> comparison_functor;
        return comparison_functor(left.numericGetDoubleValue(), right.numericGetDoubleValue());
      }
      default:
        throw OperationInapplicableToType(*this, 2, &left.getType(), &right.getType());
    }
  }
}

template <template <typename LeftCppType, bool left_type_nullable,
                    typename RightCppType, bool right_type_nullable> class NumericComparator,
          template <bool left_nullable, bool left_null_terminated, bool left_longer,
                    bool right_nullable, bool right_null_terminated, bool right_longer> class StringComparator>
UncheckedComparator* BasicComparison::makeUncheckedComparatorForTypesHelper(const Type &left,
                                                                            const Type &right) const {
  if (!canCompareTypes(left, right)) {
    throw OperationInapplicableToType(*this, 2, &left, &right);
  }

  if (left.getSuperTypeID() == Type::kAsciiString) {
    return makeStringComparatorOuterHelper<StringComparator>(*this, left, right);
  } else {
    return makeComparatorOuterHelper<NumericComparator>(*this, left, right);
  }
}

int BasicComparison::strcmpHelper(const TypeInstance &left, const TypeInstance &right) const {
  DEBUG_ASSERT(left.supportsAsciiStringInterface());
  DEBUG_ASSERT(right.supportsAsciiStringInterface());
  if (left.asciiStringGuaranteedNullTerminated() && right.asciiStringGuaranteedNullTerminated()) {
    return strcmp(static_cast<const char*>(left.getDataPtr()), static_cast<const char*>(right.getDataPtr()));
  } else {
    size_t left_max_length = left.asciiStringMaximumLength();
    size_t right_max_length = right.asciiStringMaximumLength();
    if (left_max_length < right_max_length) {
      int res = strncmp(static_cast<const char*>(left.getDataPtr()),
                        static_cast<const char*>(right.getDataPtr()),
                        left_max_length);
      if (res) {
        return res;
      } else {
        if (right.asciiStringLength() > left_max_length) {
          return -1;
        } else {
          return res;
        }
      }
    } else if (left_max_length > right_max_length) {
      int res = strncmp(static_cast<const char*>(left.getDataPtr()),
                        static_cast<const char*>(right.getDataPtr()),
                        right_max_length);
      if (res) {
        return res;
      } else {
        if (left.asciiStringLength() > right_max_length) {
          return 1;
        } else {
          return res;
        }
      }
    } else {
      return strncmp(static_cast<const char*>(left.getDataPtr()),
                     static_cast<const char*>(right.getDataPtr()),
                     left_max_length);
    }
  }
}

bool EqualComparison::compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const {
  return compareTypeInstancesCheckedHelper<std::equal_to>(left, right);
}

UncheckedComparator* EqualComparison::makeUncheckedComparatorForTypes(const Type &left, const Type &right) const {
  return makeUncheckedComparatorForTypesHelper<EqualUncheckedComparator,
                                               EqualAsciiStringUncheckedComparator>(left, right);
}

bool NotEqualComparison::compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const {
  return compareTypeInstancesCheckedHelper<std::not_equal_to>(left, right);
}

UncheckedComparator* NotEqualComparison::makeUncheckedComparatorForTypes(const Type &left, const Type &right) const {
  return makeUncheckedComparatorForTypesHelper<NotEqualUncheckedComparator,
                                               NotEqualAsciiStringUncheckedComparator>(left, right);
}

bool LessComparison::compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const {
  return compareTypeInstancesCheckedHelper<std::less>(left, right);
}

UncheckedComparator* LessComparison::makeUncheckedComparatorForTypes(const Type &left, const Type &right) const {
  return makeUncheckedComparatorForTypesHelper<LessUncheckedComparator,
                                               LessAsciiStringUncheckedComparator>(left, right);
}

bool LessOrEqualComparison::compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const {
  return compareTypeInstancesCheckedHelper<std::less_equal>(left, right);
}

UncheckedComparator* LessOrEqualComparison::makeUncheckedComparatorForTypes(const Type &left,
                                                                            const Type &right) const {
  return makeUncheckedComparatorForTypesHelper<LessOrEqualUncheckedComparator,
                                               LessOrEqualAsciiStringUncheckedComparator>(left, right);
}

bool GreaterComparison::compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const {
  return compareTypeInstancesCheckedHelper<std::greater>(left, right);
}

UncheckedComparator* GreaterComparison::makeUncheckedComparatorForTypes(const Type &left, const Type &right) const {
  return makeUncheckedComparatorForTypesHelper<GreaterUncheckedComparator,
                                               GreaterAsciiStringUncheckedComparator>(left, right);
}

bool GreaterOrEqualComparison::compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const {
  return compareTypeInstancesCheckedHelper<std::greater_equal>(left, right);
}

UncheckedComparator* GreaterOrEqualComparison::makeUncheckedComparatorForTypes(const Type &left,
                                                                               const Type &right) const {
  return makeUncheckedComparatorForTypesHelper<GreaterOrEqualUncheckedComparator,
                                               GreaterOrEqualAsciiStringUncheckedComparator>(left, right);
}

}  // namespace quickstep
