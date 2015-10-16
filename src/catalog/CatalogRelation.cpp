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

#include "catalog/CatalogRelation.hpp"

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogDatabase.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "types/Type.hpp"

using std::size_t;
using std::strcmp;
using std::string;
using std::vector;

namespace quickstep {

const CatalogAttribute& CatalogRelation::getAttributeByName(const string &attr_name) const {
  CompatUnorderedMap<string, CatalogAttribute*>::unordered_map::const_iterator it = attr_map_.find(attr_name);
  if (it == attr_map_.end()) {
    FATAL_ERROR("No attribute with name " << attr_name << " in relation " << name_);
  } else {
    return *(it->second);
  }
}

CatalogAttribute* CatalogRelation::getAttributeByNameMutable(const string &attr_name) {
  CompatUnorderedMap<string, CatalogAttribute*>::unordered_map::const_iterator it = attr_map_.find(attr_name);
  if (it == attr_map_.end()) {
    FATAL_ERROR("No attribute with name " << attr_name << " in relation " << name_);
  } else {
    return it->second;
  }
}

attribute_id CatalogRelation::addAttribute(CatalogAttribute *new_attr) {
  string attr_name = new_attr->getName();
  if (hasAttributeWithName(attr_name)) {
    FATAL_ERROR("Relation " << name_ << " already contains an attribute named " << attr_name);
  } else if (attr_vec_.size() > static_cast<size_t>(kCatalogMaxID)) {
    FATAL_ERROR("ID overflow, too many attributes in relation " << name_);
  } else {
    attr_map_[attr_name] = new_attr;
    attr_vec_.push_back(new_attr);
    new_attr->setParent(this);
    new_attr->setID(static_cast<attribute_id>(attr_vec_.size() - 1));

    const Type &attr_type = new_attr->getType();

    if (attr_type.isVariableLength()) {
      variable_length_ = true;
      max_variable_byte_length_ += attr_type.maximumByteLength();
      min_variable_byte_length_ += attr_type.minimumByteLength();
      estimated_variable_byte_length_ += attr_type.estimateAverageByteLength();
    } else {
      fixed_length_attribute_offsets_.resize(new_attr->getID() + 1, fixed_byte_length_);
      fixed_byte_length_ += attr_type.maximumByteLength();
    }
    max_byte_length_ += attr_type.maximumByteLength();
    min_byte_length_ += attr_type.minimumByteLength();
    estimated_byte_length_ += attr_type.estimateAverageByteLength();

    if (attr_type.isNullable()) {
      if (has_nullable_attributes_) {
        nullable_attribute_indexes_[new_attr->getID()] = nullable_attribute_indexes_.rbegin()->second + 1;
      } else {
        has_nullable_attributes_ = true;
        nullable_attribute_indexes_[new_attr->getID()] = 0;
      }
    }

    return new_attr->getID();
  }
}

std::size_t CatalogRelation::getFixedLengthAttributeOffset(const attribute_id id) const {
  DEBUG_ASSERT(hasAttributeWithId(id));
  DEBUG_ASSERT(!getAttributeById(id).getType().isVariableLength());
  return fixed_length_attribute_offsets_.at(id);
}

unsigned int CatalogRelation::getNullableAttributeIndex(const attribute_id id) const {
  DEBUG_ASSERT(hasAttributeWithId(id));
  DEBUG_ASSERT(getAttributeById(id).getType().isNullable());
  return nullable_attribute_indexes_.find(id)->second;
}

attribute_id CatalogRelation::getMaxAttributeId() const {
  if (size() > 0) {
    return attr_vec_.back().getID();
  } else {
    return -1;
  }
}

void CatalogRelation::setDefaultStorageBlockLayout(StorageBlockLayout *default_layout) {
  DEBUG_ASSERT(&(default_layout->getRelation()) == this);
  default_layout_.reset(default_layout);
}

const StorageBlockLayout& CatalogRelation::getDefaultStorageBlockLayout() const {
  if (default_layout_.empty()) {
    default_layout_.reset(StorageBlockLayout::GenerateDefaultLayout(*this));
  }

  return *default_layout_;
}

}  // namespace quickstep
