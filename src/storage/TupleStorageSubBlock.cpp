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

#include "storage/TupleStorageSubBlock.hpp"

#ifdef QUICKSTEP_DEBUG
#include <cassert>
#endif

#include "expressions/Predicate.hpp"
#include "storage/TupleIdSequence.hpp"
#include "utility/Macros.hpp"

#ifdef QUICKSTEP_DEBUG
#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "storage/StorageBlock.hpp"
#include "types/Tuple.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#endif

namespace quickstep {

tuple_id TupleStorageSubBlock::numTuples() const {
  if (isEmpty()) {
    return 0;
  } else if (isPacked()) {
    return getMaxTupleID() + 1;
  } else {
    // WARNING: This branch is O(N). Subclasses should override wherever possible.
    tuple_id count = 0;
    for (tuple_id tid = 0; tid <= getMaxTupleID(); ++tid) {
      if (hasTupleWithID(tid)) {
        ++count;
      }
    }
    // Should have at least one tuple, otherwise isEmpty() would have been true.
    DEBUG_ASSERT(count > 0);
    return count;
  }
}

TupleIdSequence* TupleStorageSubBlock::getMatchesForPredicate(const Predicate *pred) const {
  TupleIdSequence *matches = new TupleIdSequence();

  tuple_id max_tid = getMaxTupleID();

  if (pred == NULL) {
    if (isPacked()) {
      for (tuple_id tid = 0; tid <= max_tid; ++tid) {
        matches->append(tid);
      }
    } else {
      for (tuple_id tid = 0; tid <= max_tid; ++tid) {
        if (hasTupleWithID(tid)) {
          matches->append(tid);
        }
      }
    }
  } else {
    if (isPacked()) {
      for (tuple_id tid = 0; tid <= max_tid; ++tid) {
        if (pred->matchesForSingleTuple(*this, tid)) {
          matches->append(tid);
        }
      }
    } else {
      for (tuple_id tid = 0; tid <= max_tid; ++tid) {
        if (hasTupleWithID(tid) && (pred->matchesForSingleTuple(*this, tid))) {
          matches->append(tid);
        }
      }
    }
  }

  return matches;
}

void TupleStorageSubBlock::paranoidInsertTypeCheck(const Tuple &tuple, const AllowedTypeConversion atc) {
#ifdef QUICKSTEP_DEBUG
  assert(relation_.size() == tuple.size());

  Tuple::const_iterator value_it = tuple.begin();
  CatalogRelation::const_iterator attr_it = relation_.begin();

  while (value_it != tuple.end()) {
    switch (atc) {
      case kNone:
        assert(value_it->getType().equals(attr_it->getType()));
        break;
      case kSafe:
        assert(value_it->getType().isSafelyCoercibleTo(attr_it->getType()));
        break;
      case kUnsafe:
        assert(value_it->getType().isCoercibleTo(attr_it->getType()));
        break;
    }

    ++value_it;
    ++attr_it;
  }
#endif
}

}  // namespace quickstep
