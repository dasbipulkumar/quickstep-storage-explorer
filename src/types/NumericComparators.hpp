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

#ifndef QUICKSTEP_TYPES_NUMERIC_COMPARATORS_HPP_
#define QUICKSTEP_TYPES_NUMERIC_COMPARATORS_HPP_

#include <functional>

#include "types/TypeInstance.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

// We use these functors instead of the standard-library ones, because the
// standard-library functors in <functional> have to be instantiated for the
// most specific argument type, which would unnecessisarily multiply the number
// of distinct template instantiations of Comparators by 4.
template <typename LeftArgument, typename RightArgument> struct EqualFunctor
    : public std::binary_function<LeftArgument, RightArgument, bool> {
  inline bool operator() (const LeftArgument &left, const RightArgument &right) const {
    return left == right;
  }
};

template <typename LeftArgument, typename RightArgument> struct NotEqualFunctor
    : public std::binary_function<LeftArgument, RightArgument, bool> {
  inline bool operator() (const LeftArgument &left, const RightArgument &right) const {
    return left != right;
  }
};

template <typename LeftArgument, typename RightArgument> struct LessFunctor
    : public std::binary_function<LeftArgument, RightArgument, bool> {
  inline bool operator() (const LeftArgument &left, const RightArgument &right) const {
    return left < right;
  }
};

template <typename LeftArgument, typename RightArgument> struct LessOrEqualFunctor
    : public std::binary_function<LeftArgument, RightArgument, bool> {
  inline bool operator() (const LeftArgument &left, const RightArgument &right) const {
    return left <= right;
  }
};

template <typename LeftArgument, typename RightArgument> struct GreaterFunctor
    : public std::binary_function<LeftArgument, RightArgument, bool> {
  inline bool operator() (const LeftArgument &left, const RightArgument &right) const {
    return left > right;
  }
};

template <typename LeftArgument, typename RightArgument> struct GreaterOrEqualFunctor
    : public std::binary_function<LeftArgument, RightArgument, bool> {
  inline bool operator() (const LeftArgument &left, const RightArgument &right) const {
    return left >= right;
  }
};

template <template <typename LeftArgument, typename RightArgument> class ComparisonFunctor,  // NOLINT - ComparisonFunctor is not a real class
          typename LeftCppType, bool left_nullable,
          typename RightCppType, bool right_nullable>
class NumericUncheckedComparator : public UncheckedComparator {
 public:
  virtual ~NumericUncheckedComparator() {
  }

  inline bool compareTypeInstances(const TypeInstance &left, const TypeInstance &right) const {
    return compareDataPtrs(left.getDataPtr(), right.getDataPtr());
  }

  inline bool compareDataPtrs(const void *left, const void *right) const {
    if ((left_nullable && (left == NULL)) || (right_nullable && (right == NULL))) {
      return false;
    } else {
      return comparison_functor_(*(static_cast<const LeftCppType*>(left)),
                                 *(static_cast<const RightCppType*>(right)));
    }
  }

  inline bool compareTypeInstanceWithDataPtr(const TypeInstance &left, const void *right) const {
    return compareDataPtrs(left.getDataPtr(), right);
  }

  inline bool compareDataPtrWithTypeInstance(const void *left, const TypeInstance &right) const {
    return compareDataPtrs(left, right.getDataPtr());
  }

 protected:
  NumericUncheckedComparator() {
  }

 private:
  ComparisonFunctor<LeftCppType, RightCppType> comparison_functor_;

  DISALLOW_COPY_AND_ASSIGN(NumericUncheckedComparator);
};

/**
 * @brief The equals UncheckedComparator.
 **/
template <typename LeftCppType, bool left_nullable,
          typename RightCppType, bool right_nullable>
class EqualUncheckedComparator
    : public NumericUncheckedComparator<EqualFunctor,
                                        LeftCppType, left_nullable,
                                        RightCppType, right_nullable> {
 public:
  EqualUncheckedComparator() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(EqualUncheckedComparator);
};

/**
 * @brief The not-equal UncheckedComparator.
 **/
template <typename LeftCppType, bool left_nullable,
          typename RightCppType, bool right_nullable>
class NotEqualUncheckedComparator
    : public NumericUncheckedComparator<NotEqualFunctor,
                                        LeftCppType, left_nullable,
                                        RightCppType, right_nullable> {
 public:
  NotEqualUncheckedComparator() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(NotEqualUncheckedComparator);
};

/**
 * @brief The less-than UncheckedComparator.
 **/
template <typename LeftCppType, bool left_nullable,
          typename RightCppType, bool right_nullable>
class LessUncheckedComparator
    : public NumericUncheckedComparator<LessFunctor,
                                        LeftCppType, left_nullable,
                                        RightCppType, right_nullable> {
 public:
  LessUncheckedComparator() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LessUncheckedComparator);
};

/**
 * @brief The less-than-or-equal UncheckedComparator.
 **/
template <typename LeftCppType, bool left_nullable,
          typename RightCppType, bool right_nullable>
class LessOrEqualUncheckedComparator
    : public NumericUncheckedComparator<LessOrEqualFunctor,
                                        LeftCppType, left_nullable,
                                        RightCppType, right_nullable> {
 public:
  LessOrEqualUncheckedComparator() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LessOrEqualUncheckedComparator);
};

/**
 * @brief The greater-than UncheckedComparator.
 **/
template <typename LeftCppType, bool left_nullable,
          typename RightCppType, bool right_nullable>
class GreaterUncheckedComparator
    : public NumericUncheckedComparator<GreaterFunctor,
                                        LeftCppType, left_nullable,
                                        RightCppType, right_nullable> {
 public:
  GreaterUncheckedComparator() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(GreaterUncheckedComparator);
};

/**
 * @brief The greater-than-or-equal UncheckedComparator.
 **/
template <typename LeftCppType, bool left_nullable,
          typename RightCppType, bool right_nullable>
class GreaterOrEqualUncheckedComparator
    : public NumericUncheckedComparator<GreaterOrEqualFunctor,
                                        LeftCppType, left_nullable,
                                        RightCppType, right_nullable> {
 public:
  GreaterOrEqualUncheckedComparator() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(GreaterOrEqualUncheckedComparator);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_NUMERIC_COMPARATORS_HPP_
