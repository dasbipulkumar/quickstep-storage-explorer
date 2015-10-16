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

#ifndef QUICKSTEP_CATALOG_CATALOG_DATABASE_HPP_
#define QUICKSTEP_CATALOG_CATALOG_DATABASE_HPP_

#include <sstream>
#include <string>

#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"

namespace quickstep {

class Catalog;

/** \addtogroup Catalog
 *  @{
 */

/**
 * @brief A single database in the catalog.
 **/
class CatalogDatabase {
 public:
  typedef CompatUnorderedMap<std::string, CatalogRelation*>::unordered_map::size_type size_type;
  typedef PtrVector<CatalogRelation, true>::const_skip_iterator const_iterator;

  /**
   * @brief Create a new database.
   *
   * @param parent The catalog this database belongs to.
   * @param name This database's name.
   * @param id This database's ID (defaults to -1, which means invalid/unset).
   **/
  CatalogDatabase(Catalog *parent, const std::string &name, const database_id id = -1)
      : parent_(parent), id_(id), name_(name) {
  }

  /**
   * @brief Destructor which recursively destroys children.
   **/
  ~CatalogDatabase() {
  }

  /**
   * @brief Get the parent catalog.
   *
   * @return Parent catalog.
   **/
  const Catalog& getParent() const {
    return *parent_;
  }

  /**
   * @brief Get a mutable pointer to the parent catalog.
   *
   * @return Parent catalog.
   **/
  Catalog* getParentMutable() {
    return parent_;
  }

  /**
   * @brief Get this database's ID.
   *
   * @return This database's ID.
   **/
  database_id getID() const {
    return id_;
  }

  /**
   * @brief Get this database's name.
   *
   * @return This database's name.
   **/
  const std::string& getName() const {
    return name_;
  }

  /**
   * @brief Check whether a relation with the given name exists.
   *
   * @param rel_name The name to check for.
   * @return Whether the relation exists.
   **/
  bool hasRelationWithName(const std::string &rel_name) const {
    return (rel_map_.find(rel_name) != rel_map_.end());
  }

  /**
   * @brief Check whether a relation with the given id exists.
   *
   * @param id The id to check for.
   * @return Whether the relation exists.
   **/
  bool hasRelationWithId(const relation_id id) const {
    return (idInRange(id) && !rel_vec_.elementIsNull(id));
  }

  /**
   * @brief Get a relation by name.
   *
   * @param rel_name The name to search for.
   * @return The relation with the given name.
   **/
  const CatalogRelation& getRelationByName(const std::string &rel_name) const;

  /**
   * @brief Get a mutable pointer to a relation by name.
   *
   * @param rel_name The name to search for.
   * @return The relation with the given name.
   **/
  CatalogRelation* getRelationByNameMutable(const std::string &rel_name);

  /**
   * @brief Get a relation by ID.
   *
   * @param id The id to search for.
   * @return The relation with the given ID.
   **/
  const CatalogRelation& getRelationById(const relation_id id) const {
    if (hasRelationWithId(id)) {
      return rel_vec_[id];
    } else {
      FATAL_ERROR("No relation with id " << id << " in database " << name_);
    }
  }

  /**
   * @brief Get a mutable pointer to a relation by ID.
   *
   * @param id The id to search for.
   * @return The relation with the given ID.
   **/
  CatalogRelation* getRelationByIdMutable(const relation_id id) {
    if (hasRelationWithId(id)) {
      return &(rel_vec_[id]);
    } else {
      FATAL_ERROR("No relation with id " << id << " in database " << name_);
    }
  }

  /**
   * @brief Add a new relation to the database. If the relation already has an
   *        ID and/or parent, it will be overwritten.
   *
   * @param new_rel The relation to be added.
   * @return The id assigned to the relation.
   **/
  relation_id addRelation(CatalogRelation *new_rel);

  /**
   * @brief Drop (delete) a relation by name.
   *
   * @param rel_name The name of the relation to drop.
   **/
  void dropRelationByName(const std::string &rel_name);

  /**
   * @brief Drop (delete) a relation by id.
   *
   * @param id The ID of the relation to drop.
   **/
  void dropRelationById(const relation_id id);

  /**
   * @brief Get the number of child relations.
   *
   * @return The number of child relations.
   **/
  size_type size() const {
    return rel_map_.size();
  }

  /**
   * @brief Get an iterator at the beginning of the child relations.
   *
   * @return An iterator on the first child relation.
   **/
  const_iterator begin() const {
    return rel_vec_.begin_skip();
  }

  /**
   * @brief Get an iterator at one-past-the-end of the child relations.
   *
   * @return An iterator one-past-the-end of the child relations.
   **/
  const_iterator end() const {
    return rel_vec_.end_skip();
  }

 private:
  /**
   * @brief Set the parent Catalog of this database. Used by Catalog (a friend
   *        of this class) when adding a new database.
   *
   * @param parent The new parent for this CatalogDatabase.
   **/
  void setParent(Catalog *parent) {
    parent_ = parent;
  }

  /**
   * @brief Set the ID of this database. Used by Catalog (a friend of this
   *        class) when adding a new database.
   *
   * @param id The new ID for this CatalogDatabase.
   **/
  void setID(const database_id id) {
    id_ = id;
  }

  /**
   * @brief Check whether a relation_id is within the range of IDs contained
   *        in this CatalogDatabase.
   *
   * @param id The id to check.
   * @return true if id is in range, false otherwise.
   **/
  bool idInRange(const relation_id id) const {
    return ((id >= 0)
            && (static_cast<PtrVector<CatalogRelation>::size_type>(id) < rel_vec_.size()));
  }

  Catalog *parent_;
  database_id id_;
  std::string name_;

  PtrVector<CatalogRelation, true> rel_vec_;
  CompatUnorderedMap<std::string, CatalogRelation*>::unordered_map rel_map_;

  friend class Catalog;

  DISALLOW_COPY_AND_ASSIGN(CatalogDatabase);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_CATALOG_CATALOG_DATABASE_HPP_
