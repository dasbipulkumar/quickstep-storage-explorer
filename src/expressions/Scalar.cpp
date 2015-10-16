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

#include "expressions/Scalar.hpp"

#include <utility>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogRelation.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/TypeInstance.hpp"
#include "utility/Macros.hpp"

using std::pair;
using std::make_pair;

namespace quickstep {

const LiteralTypeInstance& Scalar::getStaticValue() const {
  FATAL_ERROR("Called getStaticValue() on a Scalar which does not have a static value");
}

const void* Scalar::getDataPtrFor(const TupleStorageSubBlock &tuple_store, tuple_id tuple) const {
  FATAL_ERROR("Called getDataPtrFor() on a Scalar which does not support it");
}

Scalar* ScalarLiteral::clone() const {
  return new ScalarLiteral(internal_literal_->makeCopy());
}

const Type& ScalarLiteral::getType() const {
  return internal_literal_->getType();
}

TypeInstance* ScalarLiteral::getValueForSingleTuple(const TupleStorageSubBlock &tuple_store,
                                                    const tuple_id tuple) const {
  return internal_literal_->makeReference();
}

const void* ScalarLiteral::getDataPtrFor(const TupleStorageSubBlock &tupleStore, tuple_id tuple) const {
  return internal_literal_->getDataPtr();
}

Scalar* ScalarAttribute::clone() const {
  return new ScalarAttribute(*attribute_);
}

const Type& ScalarAttribute::getType() const {
  return attribute_->getType();
}

TypeInstance* ScalarAttribute::getValueForSingleTuple(const TupleStorageSubBlock &tuple_store,
                                                      const tuple_id tuple) const {
  DEBUG_ASSERT(tuple_store.getRelation().getID() == attribute_->getParent().getID());
  DEBUG_ASSERT(tuple_store.getRelation().hasAttributeWithId(attribute_->getID()));
  DEBUG_ASSERT(tuple_store.hasTupleWithID(tuple));

  return tuple_store.getAttributeValueTyped(tuple, attribute_->getID());
}

bool ScalarAttribute::supportsDataPtr(const TupleStorageSubBlock &tuple_store) const {
  return tuple_store.supportsUntypedGetAttributeValue(attribute_->getID());
}

const void* ScalarAttribute::getDataPtrFor(const TupleStorageSubBlock &tuple_store, const tuple_id tuple) const {
  DEBUG_ASSERT(tuple_store.getRelation().getID() == attribute_->getParent().getID());
  DEBUG_ASSERT(tuple_store.getRelation().hasAttributeWithId(attribute_->getID()));
  DEBUG_ASSERT(tuple_store.hasTupleWithID(tuple));

  return tuple_store.getAttributeValue(tuple, attribute_->getID());
}

}  // namespace quickstep
