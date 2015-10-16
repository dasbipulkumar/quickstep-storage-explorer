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

#include "catalog/CatalogDatabase.hpp"

#include <cstddef>
#include <cstring>
#include <string>

#include "catalog/CatalogRelation.hpp"

using std::pair;
using std::size_t;
using std::strcmp;
using std::string;

namespace quickstep {

const CatalogRelation& CatalogDatabase::getRelationByName(const string &rel_name) const {
  CompatUnorderedMap<string, CatalogRelation*>::unordered_map::const_iterator it = rel_map_.find(rel_name);
  if (it == rel_map_.end()) {
    FATAL_ERROR("No relation with name " << rel_name << " in database " << name_);
  } else {
    return *(it->second);
  }
}

CatalogRelation* CatalogDatabase::getRelationByNameMutable(const string &rel_name) {
  CompatUnorderedMap<string, CatalogRelation*>::unordered_map::const_iterator it = rel_map_.find(rel_name);
  if (it == rel_map_.end()) {
    FATAL_ERROR("No relation with name " << rel_name << " in database " << name_);
  } else {
    return it->second;
  }
}

relation_id CatalogDatabase::addRelation(CatalogRelation *new_rel) {
  const string &rel_name = new_rel->getName();
  if (hasRelationWithName(rel_name)) {
    FATAL_ERROR("Database " << name_ << " already contains a relation named " << rel_name);
  } else if (rel_vec_.size() > static_cast<size_t>(kCatalogMaxID)) {
    FATAL_ERROR("ID overflow, too many relations in database " << name_);
  } else {
    rel_map_[rel_name] = new_rel;
    rel_vec_.push_back(new_rel);
    new_rel->setParent(this);
    new_rel->setID(static_cast<relation_id>(rel_vec_.size() - 1));
    return (new_rel->getID());
  }
}

void CatalogDatabase::dropRelationByName(const std::string &rel_name) {
  CompatUnorderedMap<string, CatalogRelation*>::unordered_map::iterator it = rel_map_.find(rel_name);
  if (it == rel_map_.end()) {
    FATAL_ERROR("No relation with name " << rel_name << " in database " << name_);
  } else {
    rel_vec_.deleteElement(it->second->getID());
    rel_map_.erase(it);
  }
}

void CatalogDatabase::dropRelationById(const relation_id id) {
  if (hasRelationWithId(id)) {
    rel_map_.erase(rel_vec_[id].getName());
    rel_vec_.deleteElement(id);
  } else {
    FATAL_ERROR("No relation with ID " << id << " in database " << name_);
  }
}

}  // namespace quickstep
