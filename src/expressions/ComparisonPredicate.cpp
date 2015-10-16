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

#include "expressions/ComparisonPredicate.hpp"

#include <cstring>
#include <utility>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "expressions/Scalar.hpp"
#include "types/Comparison.hpp"
#include "types/Type.hpp"
#include "types/TypeErrors.hpp"
#include "types/TypeInstance.hpp"

namespace quickstep {

ComparisonPredicate::ComparisonPredicate(const Comparison &comparison,
                                         Scalar *left_operand,
                                         Scalar *right_operand)
    : comparison_(&comparison),
      left_operand_(left_operand),
      right_operand_(right_operand) {
  initHelper(false);
}

Predicate* ComparisonPredicate::clone() const {
  return new ComparisonPredicate(*comparison_, left_operand_->clone(), right_operand_->clone());
}

bool ComparisonPredicate::isAttributeLiteralComparisonPredicate() const {
  return (left_operand_->hasStaticValue() && (right_operand_->getDataSource() == Scalar::kAttribute))
         || (right_operand_->hasStaticValue() && (left_operand_->getDataSource() == Scalar::kAttribute));
}

bool ComparisonPredicate::matchesForSingleTuple(const TupleStorageSubBlock &tuple_store, const tuple_id tuple) const {
  if (fast_comparator_.empty()) {
    return static_result_;
  } else {
    if (left_operand_->supportsDataPtr(tuple_store) && right_operand_->supportsDataPtr(tuple_store)) {
      return fast_comparator_->compareDataPtrs(left_operand_->getDataPtrFor(tuple_store, tuple),
                                               right_operand_->getDataPtrFor(tuple_store, tuple));
    } else {
      ScopedPtr<TypeInstance> left_operand_value(left_operand_->getValueForSingleTuple(tuple_store, tuple));
      ScopedPtr<TypeInstance> right_operand_value(right_operand_->getValueForSingleTuple(tuple_store, tuple));
      return fast_comparator_->compareTypeInstances(*left_operand_value, *right_operand_value);
    }
  }
}

bool ComparisonPredicate::getStaticResult() const {
  if (fast_comparator_.empty()) {
    return static_result_;
  } else {
    FATAL_ERROR("Called getStaticResult() on a predicate which has no static result");
  }
}

void ComparisonPredicate::initHelper(bool own_children) {
  if (comparison_->canCompareTypes(left_operand_->getType(), right_operand_->getType())) {
    if (left_operand_->hasStaticValue() && right_operand_->hasStaticValue()) {
      static_result_ = comparison_->compareTypeInstancesChecked(left_operand_->getStaticValue(),
                                                                right_operand_->getStaticValue());
    } else {
      fast_comparator_.reset(comparison_->makeUncheckedComparatorForTypes(left_operand_->getType(),
                                                                          right_operand_->getType()));
    }
  } else {
    const Type &left_operand_type = left_operand_->getType();
    const Type &right_operand_type = right_operand_->getType();
    if (!own_children) {
      left_operand_.release();
      right_operand_.release();
    }
    throw OperationInapplicableToType(*comparison_, 2, &left_operand_type, &right_operand_type);
  }
}

}  // namespace quickstep
