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

#include "storage/ColumnStoreUtil.hpp"

#include <algorithm>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "expressions/ComparisonPredicate.hpp"
#include "expressions/Predicate.hpp"
#include "expressions/Scalar.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/TupleIdSequence.hpp"
#include "types/Comparison.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

using std::lower_bound;
using std::upper_bound;

namespace quickstep {
namespace column_store_util {

TupleIdSequence* SortColumnPredicateEvaluator::EvaluatePredicateForUncompressedSortColumn(
    const Predicate &predicate,
    const CatalogRelation &relation,
    const attribute_id sort_attribute_id,
    void *sort_attribute_stripe,
    const tuple_id num_tuples) {
  // Determine if the predicate is a comparison of the sort column with a literal.
  if (predicate.isAttributeLiteralComparisonPredicate()) {
    const ComparisonPredicate &comparison_predicate = static_cast<const ComparisonPredicate&>(predicate);

    const CatalogAttribute *comparison_attribute = NULL;
    bool left_literal = false;
    if (comparison_predicate.getLeftOperand().hasStaticValue()) {
      DEBUG_ASSERT(comparison_predicate.getRightOperand().getDataSource() == Scalar::kAttribute);
      comparison_attribute
          = &(static_cast<const ScalarAttribute&>(comparison_predicate.getRightOperand()).getAttribute());
      left_literal = true;
    } else {
      DEBUG_ASSERT(comparison_predicate.getLeftOperand().getDataSource() == Scalar::kAttribute);
      comparison_attribute
          = &(static_cast<const ScalarAttribute&>(comparison_predicate.getLeftOperand()).getAttribute());
      left_literal = false;
    }

    DEBUG_ASSERT(comparison_attribute->getParent().getID() == relation.getID());
    if (comparison_attribute->getID() == sort_attribute_id) {
      const LiteralTypeInstance* comparison_literal;
      if (left_literal) {
        comparison_literal = &(comparison_predicate.getLeftOperand().getStaticValue());
      } else {
        comparison_literal = &(comparison_predicate.getRightOperand().getStaticValue());
      }

      // NOTE(chasseur): A standards-compliant implementation of lower_bound
      // always compares the iterator on the left with the literal on the right,
      // while upper_bound compares the literal on the left with the iterator
      // on the right. These will work even if comparison_attribute and
      // comparison_literal are different types.
      const Comparison &less_comparison = Comparison::GetComparison(Comparison::kLess);
      ScopedPtr<UncheckedComparator> fast_comparator_lower(
          less_comparison.makeUncheckedComparatorForTypes(comparison_attribute->getType(),
                                                          comparison_literal->getType()));
      STLUncheckedComparatorWrapper comp_lower(*fast_comparator_lower);
      ScopedPtr<UncheckedComparator> fast_comparator_upper(
          less_comparison.makeUncheckedComparatorForTypes(comparison_literal->getType(),
                                                          comparison_attribute->getType()));
      STLUncheckedComparatorWrapper comp_upper(*fast_comparator_upper);

      // Find the bounds on the range of matching tuples.
      tuple_id min_match = 0;
      tuple_id max_match_bound = num_tuples;
      ColumnStripeIterator begin_it(sort_attribute_stripe,
                                    relation.getAttributeById(sort_attribute_id).getType().maximumByteLength(),
                                    0);
      ColumnStripeIterator end_it(sort_attribute_stripe,
                                  relation.getAttributeById(sort_attribute_id).getType().maximumByteLength(),
                                  num_tuples);

      switch (comparison_predicate.getComparison().getComparisonID()) {
        case Comparison::kEqual:
        // Note: There is a special branch below for kNotEqual which takes the
        // complement of the matched range.
        case Comparison::kNotEqual:
          min_match = lower_bound(begin_it,
                                  end_it,
                                  comparison_literal->getDataPtr(),
                                  comp_lower).getTuplePosition();
          max_match_bound = upper_bound(begin_it,
                                        end_it,
                                        comparison_literal->getDataPtr(),
                                        comp_upper).getTuplePosition();
          break;
        case Comparison::kLess:
          if (left_literal) {
            min_match = upper_bound(begin_it,
                                    end_it,
                                    comparison_literal->getDataPtr(),
                                    comp_upper).getTuplePosition();
          } else {
            max_match_bound = lower_bound(begin_it,
                                          end_it,
                                          comparison_literal->getDataPtr(),
                                          comp_lower).getTuplePosition();
          }
          break;
        case Comparison::kLessOrEqual:
          if (left_literal) {
            min_match = lower_bound(begin_it,
                                    end_it,
                                    comparison_literal->getDataPtr(),
                                    comp_lower).getTuplePosition();
          } else {
            max_match_bound = upper_bound(begin_it,
                                          end_it,
                                          comparison_literal->getDataPtr(),
                                          comp_upper).getTuplePosition();
          }
          break;
        case Comparison::kGreater:
          if (left_literal) {
            max_match_bound = lower_bound(begin_it,
                                          end_it,
                                          comparison_literal->getDataPtr(),
                                          comp_lower).getTuplePosition();
          } else {
            min_match = upper_bound(begin_it,
                                    end_it,
                                    comparison_literal->getDataPtr(),
                                    comp_upper).getTuplePosition();
          }
          break;
        case Comparison::kGreaterOrEqual:
          if (left_literal) {
            max_match_bound = upper_bound(begin_it,
                                          end_it,
                                          comparison_literal->getDataPtr(),
                                          comp_upper).getTuplePosition();
          } else {
            min_match = lower_bound(begin_it,
                                    end_it,
                                    comparison_literal->getDataPtr(),
                                    comp_lower).getTuplePosition();
          }
          break;
        default:
          FATAL_ERROR("Unknown Comparison in SortColumnPredicateEvaluator::"
                      "EvaluatePredicateForUncompressedSortColumn()");
      }

      // Create and return the sequence of matches.
      TupleIdSequence *matches = new TupleIdSequence();
      if (comparison_predicate.getComparison().getComparisonID() == Comparison::kNotEqual) {
        // Special case: return all tuples NOT in the range for kEqual.
        for (tuple_id tid = 0; tid < min_match; ++tid) {
          matches->append(tid);
        }
        for (tuple_id tid = max_match_bound; tid < num_tuples; ++tid) {
          matches->append(tid);
        }
      } else {
        for (tuple_id tid = min_match; tid < max_match_bound; ++tid) {
          matches->append(tid);
        }
      }

      return matches;
    } else {
      return NULL;
    }
  } else {
    // Can not evaluate a non-comparison predicate, so pass through.
    return NULL;
  }
}

}  // namespace column_store_util
}  // namespace quickstep
