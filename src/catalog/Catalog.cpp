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

#include "catalog/Catalog.hpp"

#include <cstddef>
#include <cstring>
#include <string>

#include "catalog/CatalogDatabase.hpp"
#include "utility/Macros.hpp"

using std::size_t;
using std::strcmp;
using std::string;

namespace quickstep {

const CatalogDatabase& Catalog::getDatabaseByName(const string &db_name) const {
  CompatUnorderedMap<std::string, CatalogDatabase*>::unordered_map::const_iterator it = db_map_.find(db_name);
  if (it == db_map_.end()) {
    FATAL_ERROR("No database exists with name: " << db_name);
  } else {
    return *(it->second);
  }
}

CatalogDatabase* Catalog::getDatabaseByNameMutable(const string &db_name) {
  CompatUnorderedMap<std::string, CatalogDatabase*>::unordered_map::const_iterator it = db_map_.find(db_name);
  if (it == db_map_.end()) {
    FATAL_ERROR("No database exists with name: " << db_name);
  } else {
    return it->second;
  }
}

database_id Catalog::addDatabase(CatalogDatabase *new_db) {
  const string &db_name = new_db->getName();
  if (hasDatabaseWithName(db_name)) {
    FATAL_ERROR("Attempted to create database with already-existing name: " << db_name);
  } else if (db_vec_.size() > static_cast<size_t>(kCatalogMaxID)) {
    FATAL_ERROR("ID overflow, too many databases in Catalog");
  } else {
    db_map_[db_name] = new_db;
    db_vec_.push_back(new_db);
    new_db->setParent(this);
    new_db->setID(static_cast<database_id>(db_vec_.size() - 1));
    return (new_db->getID());
  }
}

}  // namespace quickstep
