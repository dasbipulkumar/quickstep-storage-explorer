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

#ifndef QUICKSTEP_CATALOG_CATALOG_HPP_
#define QUICKSTEP_CATALOG_CATALOG_HPP_

#include <sstream>
#include <string>

#include "catalog/CatalogDatabase.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"

namespace quickstep {

/** \addtogroup Catalog
 *  @{
 */

/**
 * @brief The entire database catalog.
 **/
class Catalog {
 public:
  typedef CompatUnorderedMap<std::string, CatalogDatabase*>::unordered_map::size_type size_type;
  typedef PtrVector<CatalogDatabase, true>::const_skip_iterator const_iterator;

  /**
   * @brief Construct an empty catalog.
   **/
  Catalog() {
  }

  /**
   * @brief Destructor which recursively destroys children.
   **/
  ~Catalog() {
  }

  /**
   * @brief Check whether a database with the given name exists.
   *
   * @param db_name The name to check for.
   * @return Whether the database exists.
   **/
  bool hasDatabaseWithName(const std::string &db_name) const {
    return (db_map_.find(db_name) != db_map_.end());
  }

  /**
   * @brief Check whether a database with the given id exists.
   *
   * @param id The id to check for.
   * @return Whether the database exists.
   **/
  bool hasDatabaseWithId(const database_id id) const {
    return (idInRange(id) && !db_vec_.elementIsNull(id));
  }

  /**
   * @brief Get a database by name.
   *
   * @param db_name The name to search for.
   * @return The database with the given name.
   **/
  const CatalogDatabase& getDatabaseByName(const std::string &db_name) const;

  /**
   * @brief Get a mutable pointer to a database by name.
   *
   * @param db_name The name to search for.
   * @return The database with the given name.
   **/
  CatalogDatabase* getDatabaseByNameMutable(const std::string &db_name);

  /**
   * @brief Get a database by ID.
   *
   * @param id The id to search for.
   * @return The database with the given ID.
   **/
  const CatalogDatabase& getDatabaseById(const database_id id) const {
    if (hasDatabaseWithId(id)) {
      return db_vec_[id];
    } else {
      FATAL_ERROR("No database exists with id: " << id);
    }
  }

  /**
   * @brief Get a mutable pointer to a database by ID.
   *
   * @param id The id to search for.
   * @return The database with the given ID.
   **/
  CatalogDatabase* getDatabaseByIdMutable(const database_id id) {
    if (hasDatabaseWithId(id)) {
      return &(db_vec_[id]);
    } else {
      FATAL_ERROR("No database exists with id: " << id);
    }
  }

  /**
   * @brief Add a new database to the catalog. If the database already has an
   *        ID and/or parent, it will be overwritten.
   *
   * @param new_db The database to be added.
   * @return The id assigned to the database.
   **/
  database_id addDatabase(CatalogDatabase *new_db);

  /**
   * @brief Get the number of child databases.
   *
   * @return The number of child databases.
   **/
  size_type size() const {
    return db_map_.size();
  }

  /**
   * @brief Get an iterator at the beginning of the child databases.
   *
   * @return An iterator on the first child database.
   **/
  const_iterator begin() const {
    return db_vec_.begin_skip();
  }

  /**
   * @brief Get an iterator at one-past-the-end of the child databases.
   *
   * @return An iterator one-past-the-end of the child databases.
   **/
  const_iterator end() const {
    return db_vec_.end_skip();
  }

 private:
  /**
   * @brief Check whether a database_id is within the range of IDs contained
   *        in this Catalog.
   *
   * @param id The id to check.
   * @return true if id is in range, false otherwise.
   **/
  bool idInRange(const database_id id) const {
    return ((id >= 0)
            && (static_cast<PtrVector<CatalogDatabase>::size_type>(id) < db_vec_.size()));
  }

  PtrVector<CatalogDatabase, true> db_vec_;
  CompatUnorderedMap<std::string, CatalogDatabase*>::unordered_map db_map_;

  DISALLOW_COPY_AND_ASSIGN(Catalog);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_CATALOG_CATALOG_HPP_
