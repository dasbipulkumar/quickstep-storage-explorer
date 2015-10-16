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

#ifndef QUICKSTEP_EXPRESSIONS_COMPARISON_PREDICATE_HPP_
#define QUICKSTEP_EXPRESSIONS_COMPARISON_PREDICATE_HPP_

#include <cstddef>
#include <utility>

#include "expressions/Predicate.hpp"
#include "expressions/Scalar.hpp"
#include "types/Comparison.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief A Predicate which is a comparison of two scalar values.
 **/
class ComparisonPredicate : public Predicate {
 public:
  /**
   * @brief Constructor
   *
   * @param Comparison The comparison operation to be performed.
   * @param left_operand The left argument of the comparison, becomes owned by
   *        this ComparisonPredicate.
   * @param right_operand The right argument of the comparison, becomes owned
   *        by this ComparisonPredicate.
   **/
  ComparisonPredicate(const Comparison &comparison, Scalar *left_operand, Scalar *right_operand);

  ~ComparisonPredicate() {
  }

  Predicate* clone() const;

  PredicateType getPredicateType() const {
    return kComparison;
  }

  bool isAttributeLiteralComparisonPredicate() const;

  bool matchesForSingleTuple(const TupleStorageSubBlock &tupleStore, const tuple_id tuple) const;

  bool hasStaticResult() const {
    return (fast_comparator_.empty());
  }

  bool getStaticResult() const;

  /**
   * @brief Get the comparison operation for this predicate.
   *
   * @return This predicate's comparison.
   **/
  const Comparison& getComparison() const {
    DEBUG_ASSERT(comparison_ != NULL);
    return *comparison_;
  }

  /**
   * @brief Get the left operand of this comparison.
   *
   * @return This comparison's left operand.
   **/
  const Scalar& getLeftOperand() const {
    DEBUG_ASSERT(!left_operand_.empty());
    return *left_operand_;
  }

  /**
   * @brief Get the right operand of this comparison.
   *
   * @return This comparison's right operand.
   **/
  const Scalar& getRightOperand() const {
    DEBUG_ASSERT(!right_operand_.empty());
    return *right_operand_;
  }

 private:
  // Ordinarily we would use a reference, but we use a pointer for deferred
  // setting when deserializing JSON.
  const Comparison *comparison_;
  ScopedPtr<Scalar> left_operand_;
  ScopedPtr<Scalar> right_operand_;
  bool static_result_;
  ScopedPtr<UncheckedComparator> fast_comparator_;

  void initHelper(bool own_children);

  DISALLOW_COPY_AND_ASSIGN(ComparisonPredicate);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_COMPARISON_PREDICATE_HPP_
