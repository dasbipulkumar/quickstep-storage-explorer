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

#ifndef QUICKSTEP_TYPES_COMPARISON_HPP_
#define QUICKSTEP_TYPES_COMPARISON_HPP_

#include "types/Operation.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class Type;
class TypeInstance;

/** \addtogroup Types
 *  @{
 */

/**
 * @brief An comparator which can be used to quickly compare data items WITHOUT
 *        checking their types.
 **/
class UncheckedComparator {
 public:
  /**
   * @brief Virtual destructor.
   **/
  virtual ~UncheckedComparator() {
  }

  /**
   * @brief Compare two TypeInstances without type-checking.
   *
   * @param left The left TypeInstance to compare.
   * @param right The right TypeInstance to compare.
   * @return Whether the comparison is true for the given TypeInstances.
   **/
  virtual bool compareTypeInstances(const TypeInstance &left, const TypeInstance &right) const = 0;

  /**
   * @brief Compare data items without type-checking via pointers.
   *
   * @param left The left data item to compare.
   * @param right The right data item to compare.
   * @return Whether the comparison is true for the given data items.
   **/
  virtual bool compareDataPtrs(const void *left, const void *right) const = 0;

  /**
   * @brief Compare a TypeInstance with a raw data pointer without
   *        type-checking.
   * @note See also compareDataPtrWithTypeInstance().
   *
   * @param left The left TypeInstance to compare.
   * @param right The right data pointer to compare.
   * @return Whether the comparison is true for the given data items.
   **/
  virtual bool compareTypeInstanceWithDataPtr(const TypeInstance &left, const void *right) const = 0;

  /**
   * @brief Compare a raw data pointer with a TypeInstance without
   *        type-checking.
   * @note See also compareTypeInstanceWithDataPtr().
   *
   * @param left The left data pointer to compare.
   * @param right The right TypeInstance to compare.
   * @return Whether the comparison is true for the given data items.
   **/
  virtual bool compareDataPtrWithTypeInstance(const void *left, const TypeInstance &right) const = 0;

 protected:
  UncheckedComparator() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(UncheckedComparator);
};

/**
 * @brief A lightweight wrapper for an UncheckedComparator which implements
 *        a functor interface that is compatible with STL algorithms.
 **/
class STLUncheckedComparatorWrapper {
 public:
  /**
   * @brief Constructor.
   *
   * @param comparator An UncheckedComparator to wrap which is created, owned,
   *        and managed by the caller.
   **/
  explicit STLUncheckedComparatorWrapper(const UncheckedComparator &comparator)
      : internal_comparator_(comparator) {
  }

  /**
   * @brief Compare two data items without type-checking via pointers.
   *        Overloads the call operator for compatibility with STL algorithms.
   *
   * @param left The left data pointer to compare.
   * @param right The right data pointer to compare.
   * @return Whether the comparison is true for the given data items.
   **/
  inline bool operator() (const void *left, const void *right) const {
    return internal_comparator_.compareDataPtrs(left, right);
  }

 private:
  const UncheckedComparator &internal_comparator_;
};

/**
 * @brief An operation which compares two typed values and returns a bool.
 * @note Comparing NULL with any value always results in false
 *       (even NULL = NULL is false).
 **/
class Comparison : public Operation {
 public:
  /**
   * @brief Concrete Comparisons.
   **/
  enum ComparisonID {
    kEqual = 0,
    kNotEqual,
    kLess,
    kLessOrEqual,
    kGreater,
    kGreaterOrEqual,
    kNumComparisonIDs  // Not a real ComparisonID, exists for counting purposes.
  };

  /**
   * @brief Names of comparisons in the same order as ComparisonID.
   * @note Defined out-of-line in Comparison.cpp
   **/
  static const char *kComparisonNames[kNumComparisonIDs];

  /**
   * @brief Short names (i.e. mathematical symbols) of comparisons in the same
   *        order as ComparisonID.
   * @note Defined out-of-line in Comparison.cpp
   **/
  static const char *kComparisonShortNames[kNumComparisonIDs];

  /**
   * @brief Convenience factory method to get a reference to a Comparison from
   *        that Comparison's ID.
   *
   * @param id The ID of the desired Comparison.
   * @return The Comparison corresponding to id.
   **/
  static const Comparison& GetComparison(const ComparisonID id);

  OperationSuperTypeID getOperationSuperTypeID() const {
    return kComparison;
  }

  const char* getName() const {
    return kComparisonNames[getComparisonID()];
  }

  const char* getShortName() const {
    return kComparisonShortNames[getComparisonID()];
  }

  /**
   * @brief Determine the ID of this Comparison.
   *
   * @return The ID of this Comparison.
   **/
  virtual ComparisonID getComparisonID() const = 0;

  /**
   * @brief Determine whether two Types can be compared by this Comparison.
   *
   * @param left The first Type to check.
   * @param right The second Type to check.
   * @return Whether the specified types can be compared by this Comparison.
   **/
  virtual bool canCompareTypes(const Type &left, const Type &right) const = 0;

  /**
   * @brief Compare two TypeInstances (also check that their types are, indeed,
   *        comparable).
   *
   * @param left The left TypeInstance to compare.
   * @param right The right TypeInstance to compare.
   * @return Whether this comparison is true for the given TypeInstances.
   * @exception OperationInapplicableToType This Comparison is not applicable
   *            to either left or right.
   **/
  virtual bool compareTypeInstancesChecked(const TypeInstance &left, const TypeInstance &right) const = 0;

  /**
   * @brief Create an UncheckedComparator which can compare items of the
   *        specified types.
   * @warning The resulting UncheckedComparator performs no type-checking
   *          whatsoever. Nonetheless, it is useful in situations where many
   *          data items of the same, known type are to be compared (for
   *          example, over many tuples in the same table).
   *
   * @param left The left Type to compare.
   * @param right The right Type to compare.
   * @exception OperationInapplicableToType This Comparison is not applicable
   *            to either left or right.
   * @return An UncheckedComparator which applies this Comparison to the
   *         specified Types.
   **/
  virtual UncheckedComparator* makeUncheckedComparatorForTypes(const Type &left, const Type &right) const = 0;

 protected:
  Comparison() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(Comparison);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_COMPARISON_HPP_
