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

#ifndef QUICKSTEP_CATALOG_CATALOG_ATTRIBUTE_HPP_
#define QUICKSTEP_CATALOG_CATALOG_ATTRIBUTE_HPP_

#include <cstddef>
#include <string>

#include "catalog/CatalogTypedefs.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogRelation;
class Type;

/** \addtogroup Catalog
 *  @{
 */

/**
 * @brief An attribute in a relation.
 **/
class CatalogAttribute {
 public:
  /**
   * @brief Create a new attribute.
   *
   * @param parent The relation this attribute belongs to.
   * @param name This attribute's name.
   * @param type This attribute's complete data type.
   * @param id This attribute's ID (defaults to -1, which means invalid/unset).
   * @param display_name A different name to display when printing values of
   *        this attribute out. Defaults to name.
   **/
  CatalogAttribute(CatalogRelation *parent,
                   const std::string &name,
                   const Type &type,
                   const attribute_id id = -1,
                   const std::string &display_name = "")
      : parent_(parent), id_(id), name_(name), display_name_(display_name), type_(&type) {
  }

  /**
   * @brief Get the parent relation.
   *
   * @return Parent relation.
   **/
  const CatalogRelation& getParent() const {
    return *parent_;
  }

  /**
   * @brief Get a mutable pointer to the parent relation.
   *
   * @return Parent relation.
   **/
  CatalogRelation* getParentMutable() const {
    return parent_;
  }

  /**
   * @brief Get this attribute's ID.
   *
   * @return This attribute's ID.
   **/
  attribute_id getID() const {
    return id_;
  }

  /**
   * @brief Get this attribute's name.
   *
   * @return This attribute's name.
   **/
  const std::string& getName() const {
    return name_;
  }

  /**
   * @brief Get this attribute's display name (the name which would be printed
   *        to the screen).
   *
   * @return This attribute's display name.
   **/
  const std::string& getDisplayName() const {
    if (display_name_.empty()) {
      return name_;
    } else {
      return display_name_;
    }
  }

  /**
   * @brief Get this attribute's type.
   *
   * @return This attribute's type.
   **/
  const Type& getType() const {
    return *type_;
  }

 private:
    /**
   * @brief Set the parent CatalogRelation for this attribute. Used by
   *        CatalogRelation (a friend of this class) when adding a new
   *        attribute.
   *
   * @param parent The new parent for this CatalogAttribute.
   **/
  void setParent(CatalogRelation *parent) {
    parent_ = parent;
  }

  /**
   * @brief Set the ID of this attribute. Used by CatalogRelation (a friend of
   *        this class) when adding a new attribute.
   *
   * @param id The new ID for this CatalogAttribute.
   **/
  void setID(const attribute_id id) {
    id_ = id;
  }

  CatalogRelation *parent_;
  attribute_id id_;
  std::string name_, display_name_;

  // Oridinarily we would use a reference, but when reconstructing from JSON we
  // need to defer setting this, so we must use a pointer.
  const Type *type_;

  friend class CatalogRelation;

  DISALLOW_COPY_AND_ASSIGN(CatalogAttribute);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_CATALOG_CATALOG_ATTRIBUTE_HPP_
