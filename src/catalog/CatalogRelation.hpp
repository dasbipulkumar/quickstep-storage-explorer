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

#ifndef QUICKSTEP_CATALOG_CATALOG_RELATION_HPP_
#define QUICKSTEP_CATALOG_CATALOG_RELATION_HPP_

#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "types/AllowedTypeConversion.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

class CatalogDatabase;

/** \addtogroup Catalog
 *  @{
 */

/**
 * @brief A relation in a database.
 **/
class CatalogRelation {
 public:
  typedef CompatUnorderedMap<std::string, CatalogAttribute*>::unordered_map::size_type size_type;
  typedef PtrVector<CatalogAttribute, true>::const_skip_iterator const_iterator;

  typedef CompatUnorderedSet<block_id>::unordered_set::size_type size_type_blocks;
  typedef CompatUnorderedSet<block_id>::unordered_set::const_iterator const_iterator_blocks;

  /**
   * @brief Create a new relation.
   *
   * @param parent The database this relation belongs to.
   * @param name This relation's name.
   * @param id This relation's ID (defaults to -1, which means invalid/unset).
   * @param temporary Whether this relation is temporary (stores an
   *        intermediate result during query processing).
   **/
  CatalogRelation(CatalogDatabase* parent,
                  const std::string &name,
                  const relation_id id = -1,
                  bool temporary = false)
      : parent_(parent),
        id_(id),
        name_(name),
        temporary_(temporary),
        variable_length_(false),
        has_nullable_attributes_(false),
        max_byte_length_(0),
        min_byte_length_(0),
        estimated_byte_length_(0),
        fixed_byte_length_(0),
        max_variable_byte_length_(0),
        min_variable_byte_length_(0),
        estimated_variable_byte_length_(0),
        default_layout_(NULL) {
  }

  /**
   * @brief Destructor which recursively destroys children.
   **/
  ~CatalogRelation() {
  }

  /**
   * @brief Get the parent database.
   *
   * @return Parent database.
   **/
  const CatalogDatabase& getParent() const {
    return *parent_;
  }

  /**
   * @brief Get a mutable pointer to the parent database.
   *
   * @return Parent database.
   **/
  CatalogDatabase* getParentMutable() {
    return parent_;
  }

  /**
   * @brief Get this relation's ID.
   *
   * @return This relation's ID.
   **/
  relation_id getID() const {
    return id_;
  }

  /**
   * @brief Get this relation's name.
   *
   * @return This relation's name.
   **/
  const std::string& getName() const {
    return name_;
  }

  /**
   * @brief Check whether this relation is temporary or permanent.
   *
   * @return True if this relation is temporary, false otherwise.
   **/
  bool isTemporary() const {
    return temporary_;
  }

  /**
   * @brief Check whether an attribute with the given name exists.
   *
   * @param attr_name The name to check for.
   * @return Whether the attribute exists.
   **/
  bool hasAttributeWithName(const std::string &attr_name) const {
    return (attr_map_.find(attr_name) != attr_map_.end());
  }

  /**
   * @brief Check whether an attribute with the given id exists.
   *
   * @param id The id to check for.
   * @return Whether the attribute exists.
   **/
  bool hasAttributeWithId(const attribute_id id) const {
    return (idInRange(id) && !attr_vec_.elementIsNull(id));
  }

  /**
   * @brief Get an attribute by name.
   *
   * @param attr_name The name to search for.
   * @return The attribute with the given name.
   **/
  const CatalogAttribute& getAttributeByName(const std::string &attr_name) const;

  /**
   * @brief Get a mutable pointer to an attribute by name.
   *
   * @param attr_name The name to search for.
   * @return The attribute with the given name.
   **/
  CatalogAttribute* getAttributeByNameMutable(const std::string &attr_name);

  /**
   * @brief Get an attribute by ID.
   *
   * @param id The id to search for.
   * @return The attribute with the given ID.
   **/
  const CatalogAttribute& getAttributeById(const attribute_id id) const {
    if (hasAttributeWithId(id)) {
      return attr_vec_[id];
    } else {
      FATAL_ERROR("No attribute with id " << id << " in relation " << name_);
    }
  }

  /**
   * @brief Get a mutable pointer to an attribute by ID.
   *
   * @param id The id to search for.
   * @return The attribute with the given ID.
   **/
  CatalogAttribute* getAttributeByIdMutable(const attribute_id id) {
    if (hasAttributeWithId(id)) {
      return &(attr_vec_[id]);
    } else {
      FATAL_ERROR("No attribute with name " << name_ << " in relation " << name_);
    }
  }

  /**
   * @brief Add a new attribute to the relation. If the attribute already has
   *        an ID and/or parent, it will be overwritten.
   *
   * @param new_attr The attribute to be added.
   * @return The id assigned to the attribute.
   **/
  attribute_id addAttribute(CatalogAttribute *new_attr);

  /**
   * @brief Check whether tuples of the relation are variable-length.
   *
   * @return Whether the relation is variable length (i.e. whether any child
   *         attributes are variable length).
   **/
  bool isVariableLength() const {
    return variable_length_;
  }

  /**
   * @brief Get the maximum length of tuples of this relation, in bytes.
   *
   * @return The maximum length of tuples of this relation, in bytes (equal to
   *         getFixedByteLength() if relation is fixed-length).
   **/
  std::size_t getMaximumByteLength() const {
    return max_byte_length_;
  }

  /**
   * @brief Get the minimum length of tuples of this relation, in bytes.
   *
   * @return The minimum length of tuples of this relation, in bytes (equal
   *         to getFixedByteLength() and getMaximumByteLength() if relation is
   *         fixed-length).
   **/
  std::size_t getMinimumByteLength() const {
    return min_byte_length_;
  }

  /**
   * @brief Get the estimated average length of tuples of this relation, in
   *        bytes.
   *
   * @return The estimated average length of tuples of this relation, in bytes
   *         (equal to getFixedByteLength(), getMinimumByteLength(), and
   *         getMaximumByteLength() if relation is fixed-length).
   **/
  std::size_t getEstimatedByteLength() const {
    return estimated_byte_length_;
  }

  /**
   * @brief Get the total length of the fixed-length attributes in this
   *        relation, in bytes.
   *
   * @return The total length of fixed-length attributes in this relation, in
   *         bytes.
   **/
  std::size_t getFixedByteLength() const {
    return fixed_byte_length_;
  }

  /**
   * @brief Get the total maximum length of the variable-length attributes of
   *        this relation, in bytes.
   *
   * @return The total maximum length of the variable-length attributes of this
   *         relation, in bytes (0 if the relation is fixed-length).
   **/
  std::size_t getMaximumVariableByteLength() const {
    return max_variable_byte_length_;
  }

  /**
   * @brief Get the total minimum length of the variable-length attributes of
   *        this relation, in bytes.
   *
   * @return The total minimum length of the variable-length attributes of this
   *         relation, in bytes (0 if the relation is fixed-length).
   **/
  std::size_t getMinimumVariableByteLength() const {
    return min_variable_byte_length_;
  }

  /**
   * @brief Get the estimated average length of all the variable-length
   *        attributes of this relation, in bytes.
   *
   * @return The total estimated average length of variable-length attributes
   *         of this relation, in bytes (0 if the relation is fixed-length).
   **/
  std::size_t getEstimatedVariableByteLength() const {
    return estimated_variable_byte_length_;
  }

  /**
   * @brief Get the byte offset of a fixed-length attribute in this relation.
   * @warning This method should only be called for attributes which are
   *          fixed-length. For debug builds, this is checked with an assert.
   *          For release builds, it is unchecked.
   *
   * @param id The id of the desired attribute.
   * @return The byte-offset of the specified fixed-length attribute (as it
   *         would be in a conventional row-store) in this relation.
   **/
  std::size_t getFixedLengthAttributeOffset(const attribute_id id) const;

  /**
   * @brief Check whether any attributes of the relation are nullable.
   *
   * @return Whether the relation has any nullable attributes.
   **/
  bool hasNullableAttributes() const {
    return has_nullable_attributes_;
  }

  /**
   * @brief Get the index of a nullable attribute among all the nullable
   *        attributes in this relation.
   * @warning This method should only be called for attributes which are
   *          nullable. For debug builds, this is checked with an assert.
   *          For release builds, it is unchecked.
   *
   * @param id The id of the desired attribute.
   * @return The attribute's index amongst all of the nullable attributes
   *         (intended for indexing into a NULL bitmap).
   **/
  unsigned int getNullableAttributeIndex(const attribute_id id) const;

  /**
   * @brief Register a StorageBlock as belonging to this relation (idempotent).
   *
   * @param block the ID of the block to add.
   **/
  void addBlock(const block_id block) {
    blocks_.insert(block);
  }

  /**
   * @brief Remove a StorageBlock from this relation (idempotent).
   *
   * @param block the ID of the block to remove.
   **/
  void removeBlock(const block_id block) {
    blocks_.erase(block);
  };

  /**
   * @brief Remove all StorageBlocks from this relation.
   **/
  void clearBlocks() {
    blocks_.clear();
  }

  /**
   * @brief Get the number of child attributes.
   *
   * @return The number of child attributes.
   **/
  size_type size() const {
    return attr_map_.size();
  }

  /**
   * @brief Determine whether the sequence of attribute IDs has gaps in it.
   *
   * @return Whether the sequence of attribute IDs has any gaps.
   **/
  bool gapsInAttributeSequence() const {
    return (attr_map_.size() != attr_vec_.size());
  }

  /**
   * @brief Get the highest attribute ID in this relation.
   *
   * @return The highest attribute ID in this relation (-1 if no attributes
   *         exist).
   **/
  attribute_id getMaxAttributeId() const;

  /**
   * @brief Get an iterator at the beginning of the child attributes.
   *
   * @return An iterator on the first child attribute.
   **/
  const_iterator begin() const {
    return attr_vec_.begin_skip();
  }

  /**
   * @brief Get an iterator at one-past-the-end of the child attributes.
   *
   * @return An iterator one-past-the-end of the child attributes.
   **/
  const_iterator end() const {
    return attr_vec_.end_skip();
  }

  /**
   * @brief Get the number of child blocks.
   *
   * @return The number of child blocks.
   **/
  size_type_blocks size_blocks() const {
    return blocks_.size();
  }

  /**
   * @brief Get an iterator at the beginning of the child blocks.
   * @warning Blocks are not guaranteed to be in any particular order.
   *
   * @return An iterator on the first child block.
   **/
  const_iterator_blocks begin_blocks() const {
    return blocks_.begin();
  }

  /**
   * @brief Get an iterator at one-past-the-end of the child blocks.
   * @warning Blocks are not guaranteed to be in any particular order.
   *
   * @return An iterator one-past-the-end of the child blocks.
   **/
  const_iterator_blocks end_blocks() const {
    return blocks_.end();
  }

  /**
   * @brief Set the default StorageBlockLayout for this relation.
   * @note Deletes the previous default layout, if any.
   *
   * @param default_layout The new default StorageBlockLayout for this
   *        relation, which becomes owned by this relation.
   **/
  void setDefaultStorageBlockLayout(StorageBlockLayout *default_layout);

  /**
   * @brief Get this relation's default StorageBlockLayout.
   * @note If no default has been set via setDefaultStorageBlockLayout(), then
   *       one is created with StorageBlockLayout::generateDefaultLayout().
   *
   * @return The default StorageBlockLayout for this relation.
   **/
  const StorageBlockLayout& getDefaultStorageBlockLayout() const;

 private:
  /**
   * @brief Set the parent CatalogDatabase of this relation. Used by
   *        CatalogDatabase (a friend of this class) when adding a new
   *        relation.
   *
   * @param parent The new parent for this CatalogRelation.
   **/
  void setParent(CatalogDatabase *parent) {
    parent_ = parent;
  }

  /**
   * @brief Set the ID of this relation. Used by CatalogDatabase (a friend of
   *        this class) when adding a new relation.
   *
   * @param id The new ID for this CatalogRelation.
   **/
  void setID(const relation_id id) {
    id_ = id;
  }

  /**
   * @brief Check whether an attribute_id is within the range of IDs contained
   *        in this CatalogRelation.
   *
   * @param id The id to check.
   * @return true if id is in range, false otherwise.
   **/
  bool idInRange(const relation_id id) const {
    return ((id >= 0)
            && (static_cast<PtrVector<CatalogAttribute>::size_type>(id) < attr_vec_.size()));
  }

  CatalogDatabase *parent_;
  relation_id id_;
  std::string name_;

  bool temporary_;

  PtrVector<CatalogAttribute, true> attr_vec_;
  CompatUnorderedMap<std::string, CatalogAttribute*>::unordered_map attr_map_;

  // These are cached so we don't have to recalculate them every time
  bool variable_length_;
  bool has_nullable_attributes_;
  std::size_t max_byte_length_,
              min_byte_length_,
              estimated_byte_length_,
              fixed_byte_length_,
              max_variable_byte_length_,
              min_variable_byte_length_,
              estimated_variable_byte_length_;
  std::vector<std::size_t> fixed_length_attribute_offsets_;
  // This actually needs to be kept in order, so we use std::map
  std::map<attribute_id, unsigned int> nullable_attribute_indexes_;

  CompatUnorderedSet<block_id>::unordered_set blocks_;

  mutable ScopedPtr<StorageBlockLayout> default_layout_;

  friend class CatalogDatabase;

  DISALLOW_COPY_AND_ASSIGN(CatalogRelation);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_CATALOG_CATALOG_RELATION_HPP_
