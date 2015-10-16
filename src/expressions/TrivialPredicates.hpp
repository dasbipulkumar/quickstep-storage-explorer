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

#ifndef QUICKSTEP_EXPRESSIONS_TRIVIAL_PREDICATES_HPP_
#define QUICKSTEP_EXPRESSIONS_TRIVIAL_PREDICATES_HPP_

#include <utility>

#include "expressions/Predicate.hpp"

namespace quickstep {

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief Base class for trivial predicates which always evaluate to the same value.
 **/
class TrivialPredicate : public Predicate {
 public:
  bool hasStaticResult() const {
    return true;
  }

 protected:
  TrivialPredicate() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(TrivialPredicate);
};

/**
 * @brief Predicate which always evaluates to true.
 **/
class TruePredicate : public TrivialPredicate {
 public:
  TruePredicate() {
  }

  Predicate* clone() const {
    return new TruePredicate();
  }

  PredicateType getPredicateType() const {
    return kTrue;
  }

  bool matchesForSingleTuple(const TupleStorageSubBlock &tupleStore, const tuple_id tuple) const {
    return true;
  }

  bool getStaticResult() const {
    return true;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(TruePredicate);
};

/**
 * @brief Predicate which always evaluates to false.
 **/
class FalsePredicate : public TrivialPredicate {
 public:
  FalsePredicate() {
  }

  Predicate* clone() const {
    return new FalsePredicate();
  }

  PredicateType getPredicateType() const {
    return kFalse;
  }

  bool matchesForSingleTuple(const TupleStorageSubBlock &tupleStore, const tuple_id tuple) const {
    return false;
  }

  bool getStaticResult() const {
    return false;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(FalsePredicate);
};

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_TRIVIAL_PREDICATES_HPP_
