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

#include "types/Tuple.hpp"

#ifdef QUICKSTEP_DEBUG
#include <cassert>
#endif
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "expressions/Scalar.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/PtrList.hpp"
#include "utility/ScopedPtr.hpp"

using std::vector;

namespace quickstep {

Tuple::Tuple(const CatalogRelation &relation) {
  attributes_.reserve(relation.size());
}

Tuple::Tuple(const TupleStorageSubBlock &tuple_store,
             const tuple_id tid,
             const std::vector<attribute_id> &projection_list) {
  attributes_.reserve(projection_list.size());

  for (vector<attribute_id>::const_iterator projection_it = projection_list.begin();
       projection_it != projection_list.end();
       ++projection_it) {
    attributes_.push_back(tuple_store.getAttributeValueTyped(tid, *projection_it));
  }
}

Tuple::Tuple(const TupleStorageSubBlock &tuple_store, const tuple_id tid, const PtrList<Scalar> &selection) {
  attributes_.reserve(selection.size());

  for (PtrList<Scalar>::const_iterator selection_it = selection.begin();
       selection_it != selection.end();
       ++selection_it) {
    attributes_.push_back(selection_it->getValueForSingleTuple(tuple_store, tid));
  }
}

Tuple::Tuple(const TupleStorageSubBlock &tuple_store,
             const tuple_id tid,
             const CompatUnorderedMap<attribute_id, LiteralTypeInstance*>::unordered_map &updated_values) {
  const CatalogRelation &relation = tuple_store.getRelation();

#ifdef QUICKSTEP_DEBUG
  for (CompatUnorderedMap<attribute_id, LiteralTypeInstance *>::unordered_map::const_iterator it
           = updated_values.begin();
       it != updated_values.end();
       ++it) {
    assert(relation.hasAttributeWithId(it->first));
  }
#endif

  attributes_.reserve(relation.size());

  for (CatalogRelation::const_iterator attr_it = relation.begin(); attr_it != relation.end(); ++attr_it) {
    CompatUnorderedMap<attribute_id, LiteralTypeInstance *>::unordered_map::const_iterator update_it
        = updated_values.find(attr_it->getID());
    if (update_it == updated_values.end()) {
      TypeInstance *attrval = tuple_store.getAttributeValueTyped(tid, attr_it->getID());
      if (attrval->isLiteral()) {
        attributes_.push_back(attrval);
      } else {
        attributes_.push_back(attrval->makeCopy());
        delete attrval;
      }
    } else {
      attributes_.push_back(update_it->second);
    }
  }
}

Tuple* Tuple::clone() const {
  ScopedPtr<Tuple> clone(new Tuple());
  clone->attributes_.reserve(attributes_.size());

  for (PtrVector<TypeInstance>::const_iterator attr_it = attributes_.begin();
       attr_it != attributes_.end();
       ++attr_it) {
    clone->append(attr_it->makeCopy());
  }

  return clone.release();
}

Tuple* Tuple::cloneAsInstanceOfRelation(const CatalogRelation &relation) const {
  ScopedPtr<Tuple> clone(new Tuple());
  clone->attributes_.reserve(attributes_.size());

  DEBUG_ASSERT(attributes_.size() == relation.size());

  PtrVector<TypeInstance>::const_iterator value_it = attributes_.begin();
  CatalogRelation::const_iterator attr_it = relation.begin();
  while (value_it != attributes_.end()) {
    if (value_it->getType().equals(attr_it->getType())) {
      clone->append(value_it->makeCopy());
    } else {
      clone->append(value_it->makeCoercedCopy(attr_it->getType()));
    }

    ++value_it;
    ++attr_it;
  }

  return clone.release();
}

std::size_t Tuple::getByteSize() const {
  size_t total_size = 0;
  for (const_iterator it = begin(); it != end(); ++it) {
    total_size += it->getInstanceByteLength();
  }
  return total_size;
}

}  // namespace quickstep
