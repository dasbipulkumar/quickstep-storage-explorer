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

#ifndef QUICKSTEP_EXPRESSIONS_PREDICATE_HPP_
#define QUICKSTEP_EXPRESSIONS_PREDICATE_HPP_

#include <utility>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/TupleReference.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogDatabase;
class TupleStorageSubBlock;

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief Base class for all predicates.
 **/
class Predicate {
 public:
  /**
   * @brief The possible types of predicates.
   **/
  enum PredicateType {
    kTrue = 0,
    kFalse,
    kComparison,
    kNumPredicateTypes  // Not a real PredicateType, exists for counting purposes.
  };

  /**
   * @brief Virtual destructor.
   *
   **/
  virtual ~Predicate() {
  }

  /**
   * @brief Make a deep copy of this Predicate.
   *
   * @return A cloned copy of this Predicate.
   **/
  virtual Predicate* clone() const = 0;

  /**
   * @brief Get the type of this particular Predicate instance.
   *
   * @return The type of this Predicate.
   **/
  virtual PredicateType getPredicateType() const = 0;

  /**
   * @brief Check whether this predicate is a comparison of the form
   *        'attribute comp literal' or 'literal comp attribute'.
   **/
  virtual bool isAttributeLiteralComparisonPredicate() const {
    return false;
  }

  /**
   * @brief Determine whether the given tuple in the given TupleStorageSubBlock
   *        matches this predicate.
   * @note This only works for predicates which can be evaluated on a single
   *       table. Use matchesForMultipleTuples() to evaluate join predicates.
   * @note If the symbol QUICKSTEP_DEBUG is defined, then safety checks will be
   *       performed to make sure that the TupleStorageSubBlock is in the
   *       appropriate relation. Otherwise, such checks should be performed
   *       elsewhere (i.e. by the query optimizer).
   *
   * @param tuple_store a TupleStorageSubBlock which contains the tuple to
   *        check this Predicate on.
   * @param tuple The ID of the tuple in tupleStore to check this Predicate on.
   * @return Whether the specified tuple matches this predicate.
   **/
  virtual bool matchesForSingleTuple(const TupleStorageSubBlock &tuple_store, const tuple_id tuple) const = 0;

  /**
   * @brief Determine whether this predicate's result is static (i.e. whether
   *        it can be evaluated completely independent of any tuples).
   *
   * @return Whether this predicate's result is static.
   **/
  virtual bool hasStaticResult() const {
    return false;
  }

  /**
   * @brief Determine whether this predicate's static result is true or false.
   * @note hasStaticResult() should be called first to check whether this
   *       Predicate actually has a static result.
   *
   * @return The static result of this predicate.
   **/
  virtual bool getStaticResult() const {
    FATAL_ERROR("Called getStaticResult() on a predicate which has no static result");
  }

 protected:
  Predicate() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(Predicate);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_PREDICATE_HPP_
