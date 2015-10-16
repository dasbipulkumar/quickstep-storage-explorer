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

#ifndef QUICKSTEP_TYPES_BASIC_COMPARISONS_HPP_
#define QUICKSTEP_TYPES_BASIC_COMPARISONS_HPP_

#include <cstddef>

#include "types/Comparison.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

/**
 * @brief Base class for the 6 basic comparisons.
 **/
class BasicComparison : public Comparison {
 public:
  bool canCompareTypes(const Type &left, const Type &right) const;

 protected:
  BasicComparison() : Comparison() {
  }

  template <template <typename T> class ComparisonFunctor>
  bool compareTypeInstancesCheckedHelper(const TypeInstance &left, const TypeInstance &right) const;

  template <template <typename LeftCppType, bool left_type_nullable,
                      typename RightCppType, bool right_type_nullable> class NumericComparator,
            template <bool left_nullable, bool left_null_terminated, bool left_longer,
                      bool right_nullable, bool right_null_terminated, bool right_longer> class StringComparator>
  UncheckedComparator* makeUncheckedComparatorForTypesHelper(const Type &left, const Type &right) const;

 private:
  int strcmpHelper(const TypeInstance &left, const TypeInstance &right) const;

  DISALLOW_COPY_AND_ASSIGN(BasicComparison);
};

/**
 * @brief The equals Comparison.
 **/
class EqualComparison : public BasicComparison {
 public:
  /**
   * @brief Get a reference to the singleton instance of this Operation.
   *
   * @return A reference to the singleton instance of this Operation.
   **/
  static const EqualComparison& Instance() {
    static EqualComparison instance;
    return instance;
  }

  ComparisonID getComparisonID() const {
    return kEqual;
  }

  bool compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const;

  UncheckedComparator* makeUncheckedComparatorForTypes(const Type &left, const Type &right) const;

 private:
  EqualComparison() : BasicComparison() {
  }

  DISALLOW_COPY_AND_ASSIGN(EqualComparison);
};

/**
 * @brief The not-equal Comparison.
 **/
class NotEqualComparison : public BasicComparison {
 public:
  /**
   * @brief Get a reference to the singleton instance of this Operation.
   *
   * @return A reference to the singleton instance of this Operation.
   **/
  static const NotEqualComparison& Instance() {
    static NotEqualComparison instance;
    return instance;
  }

  ComparisonID getComparisonID() const {
    return kNotEqual;
  }

  bool compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const;

  UncheckedComparator* makeUncheckedComparatorForTypes(const Type &left, const Type &right) const;

 private:
  NotEqualComparison() : BasicComparison() {
  }

  DISALLOW_COPY_AND_ASSIGN(NotEqualComparison);
};

/**
 * @brief The less-than Comparison.
 **/
class LessComparison : public BasicComparison {
 public:
  /**
   * @brief Get a reference to the singleton instance of this Operation.
   *
   * @return A reference to the singleton instance of this Operation.
   **/
  static const LessComparison& Instance() {
    static LessComparison instance;
    return instance;
  }

  ComparisonID getComparisonID() const {
    return kLess;
  }

  bool compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const;

  UncheckedComparator* makeUncheckedComparatorForTypes(const Type &left, const Type &right) const;

 private:
  LessComparison() : BasicComparison() {
  }

  DISALLOW_COPY_AND_ASSIGN(LessComparison);
};

/**
 * @brief The less-than-or-equal Comparison.
 **/
class LessOrEqualComparison : public BasicComparison {
 public:
  /**
   * @brief Get a reference to the singleton instance of this Operation.
   *
   * @return A reference to the singleton instance of this Operation.
   **/
  static const LessOrEqualComparison& Instance() {
    static LessOrEqualComparison instance;
    return instance;
  }

  ComparisonID getComparisonID() const {
    return kLessOrEqual;
  }

  bool compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const;

  UncheckedComparator* makeUncheckedComparatorForTypes(const Type &left, const Type &right) const;

 private:
  LessOrEqualComparison() : BasicComparison() {
  }

  DISALLOW_COPY_AND_ASSIGN(LessOrEqualComparison);
};

/**
 * @brief The greater-than Comparison.
 **/
class GreaterComparison : public BasicComparison {
 public:
  /**
   * @brief Get a reference to the singleton instance of this Operation.
   *
   * @return A reference to the singleton instance of this Operation.
   **/
  static const GreaterComparison& Instance() {
    static GreaterComparison instance;
    return instance;
  }

  ComparisonID getComparisonID() const {
    return kGreater;
  }

  bool compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const;

  UncheckedComparator* makeUncheckedComparatorForTypes(const Type &left, const Type &right) const;

 private:
  GreaterComparison() : BasicComparison() {
  }

  DISALLOW_COPY_AND_ASSIGN(GreaterComparison);
};

/**
 * @brief The greater-than-or-equal Comparison.
 **/
class GreaterOrEqualComparison : public BasicComparison {
 public:
  /**
   * @brief Get a reference to the singleton instance of this Operation.
   *
   * @return A reference to the singleton instance of this Operation.
   **/
  static const GreaterOrEqualComparison& Instance() {
    static GreaterOrEqualComparison instance;
    return instance;
  }

  ComparisonID getComparisonID() const {
    return kGreaterOrEqual;
  }

  bool compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const;

  UncheckedComparator* makeUncheckedComparatorForTypes(const Type &left, const Type &right) const;

 private:
  GreaterOrEqualComparison() : BasicComparison() {}

  DISALLOW_COPY_AND_ASSIGN(GreaterOrEqualComparison);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_BASIC_COMPARISONS_HPP_
