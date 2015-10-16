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

#include "types/CompressionDictionaryBuilder.hpp"

#include <limits>
#include <map>
#include <utility>

#include "types/Comparison.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"

using std::numeric_limits;
using std::map;
using std::pair;
using std::uint32_t;

namespace quickstep {

CompressionDictionaryBuilder::CompressionDictionaryBuilder(const Type &type)
    : type_(type),
      last_value_by_reference_(NULL),
      code_length_bits_(0) {
  const Comparison &less_comparison = Comparison::GetComparison(Comparison::kLess);
  if (!less_comparison.canCompareTypes(type_, type_)) {
    FATAL_ERROR("Attempted to create a CompressionDictionaryBuilder for a Type "
                "which can not be compared by LessComparison.");
  }
  less_comparator_.reset(less_comparison.makeUncheckedComparatorForTypes(type_, type_));

  value_map_.reset(new map<const void*,
                           const TypeInstance*,
                           STLUncheckedComparatorWrapper>(
                               STLUncheckedComparatorWrapper(*less_comparator_)));
}

bool CompressionDictionaryBuilder::insertEntry(const TypeInstance &value) {
  DEBUG_ASSERT(value.getType().equals(type_));
  if (value.isNull()) {
    FATAL_ERROR("Attempted to insert a NULL value into a CompressionDictionaryBuilder.");
  }
  if (value_map_->size() == numeric_limits<uint32_t>::max()) {
    FATAL_ERROR("Attempted to insert a value into a CompressionDictionaryBuilder which "
                "would cause it to overflow the limit of " << numeric_limits<uint32_t>::max() << " entries.");
  }

  pair<map<const void*, const TypeInstance*, STLUncheckedComparatorWrapper>::iterator, bool>
      insertion_result = value_map_->insert(pair<const void*, const TypeInstance*>(value.getDataPtr(), &value));
  if (!insertion_result.second) {
    // This value has already been inserted.
    return false;
  }

  // Make a copy of 'value'.
  value_copies_.push_back(value.makeCopy());

  // Modify the newly-inserted entry in 'value_map_' to point to the copy
  // instead of the original 'value', which might be deleted by the caller.
  //
  // This const_cast hack is safe, because although we are modifying the key
  // of an entry in the map, we are changing the pointer to refer to a copy of
  // the same value, so the ordering/position of the changed key will be
  // exactly the same as the original.
  const_cast<const void *&>(insertion_result.first->first) = value_copies_.back().getDataPtr();
  insertion_result.first->second = &(value_copies_.back());

  if ((code_length_bits_ == 0) || (value_map_->size() == (1u << code_length_bits_) + 1)) {
    ++code_length_bits_;
  }
  last_value_by_reference_ = NULL;

  return true;
}

bool CompressionDictionaryBuilder::insertEntryByReference(const TypeInstance &value) {
  DEBUG_ASSERT(value.getType().equals(type_));
  if (value.isNull()) {
    FATAL_ERROR("Attempted to insert a NULL value into a CompressionDictionaryBuilder.");
  }
  if (value_map_->size() == numeric_limits<uint32_t>::max()) {
    FATAL_ERROR("Attempted to insert a value into a CompressionDictionaryBuilder which "
                "would cause it to overflow the limit of " << numeric_limits<uint32_t>::max() << " entries.");
  }

  pair<map<const void*, const TypeInstance*, STLUncheckedComparatorWrapper>::iterator, bool>
      insertion_result = value_map_->insert(pair<const void*, const TypeInstance*>(value.getDataPtr(), &value));
  if (insertion_result.second) {
    if ((code_length_bits_ == 0) || (value_map_->size() == (1u << code_length_bits_) + 1)) {
      ++code_length_bits_;
    }
    last_value_by_reference_ = &value;

    return true;
  } else {
    // This value has already been inserted.
    return false;
  }
}

void CompressionDictionaryBuilder::undoLastInsert() {
  if (last_value_by_reference_ == NULL) {
    if (value_copies_.empty()) {
      FATAL_ERROR("Called undoLastInsert() on an empty CompressionDictionaryBuilder.");
    }

    value_map_->erase(value_copies_.back().getDataPtr());
    value_copies_.removeBack();
  } else {
    value_map_->erase(last_value_by_reference_->getDataPtr());
    last_value_by_reference_ = NULL;
  }

  if (value_map_->empty()) {
    code_length_bits_ = 0;
  } else if ((code_length_bits_ > 1) && (value_map_->size() == (1u << (code_length_bits_ - 1)))) {
    --code_length_bits_;
  }
}

FixedLengthTypeCompressionDictionaryBuilder::FixedLengthTypeCompressionDictionaryBuilder(const Type &type)
    : CompressionDictionaryBuilder(type) {
  if (type_.isVariableLength()) {
    FATAL_ERROR("Attempted to create a FixedLengthTypeCompressionDictionaryBuilder "
                "for a variable-length Type.");
  }
}

void FixedLengthTypeCompressionDictionaryBuilder::buildDictionary(void *location) const {
  *static_cast<uint32_t*>(location) = value_map_->size();

  char *copy_location = static_cast<char*>(location) + sizeof(uint32_t);
  for (map<const void*, const TypeInstance*, STLUncheckedComparatorWrapper>::const_iterator
           it = value_map_->begin();
       it != value_map_->end();
       ++it) {
    it->second->copyInto(copy_location);
    copy_location += type_.maximumByteLength();
  }
}

VariableLengthTypeCompressionDictionaryBuilder::VariableLengthTypeCompressionDictionaryBuilder(const Type &type)
    : CompressionDictionaryBuilder(type),
      total_value_size_(0) {
  if (!type_.isVariableLength()) {
    FATAL_ERROR("Attempted to create a VariableLengthTypeCompressionDictionaryBuilder "
                "for a variable-length Type.");
  }
}

void VariableLengthTypeCompressionDictionaryBuilder::buildDictionary(void *location) const {
  *static_cast<uint32_t*>(location) = value_map_->size();

  uint32_t *offset_array_ptr = static_cast<uint32_t*>(location) + 1;
  char *values_location = static_cast<char*>(location)
                          + (value_map_->size() + 1) * sizeof(uint32_t);
  uint32_t value_offset = 0;
  for (map<const void*, const TypeInstance*, STLUncheckedComparatorWrapper>::const_iterator
           it = value_map_->begin();
       it != value_map_->end();
       ++it) {
    *offset_array_ptr = value_offset;
    it->second->copyInto(values_location + value_offset);

    ++offset_array_ptr;
    value_offset += it->second->getInstanceByteLength();
  }
}

bool VariableLengthTypeCompressionDictionaryBuilder::insertEntry(const TypeInstance &value) {
  if (total_value_size_ + value.getInstanceByteLength() > numeric_limits<uint32_t>::max()) {
    FATAL_ERROR("Attempted to insert a value into a "
                "VariableLengthTypeCompressionDictionaryBuilder which would "
                "overflow the limit of " << numeric_limits<uint32_t>::max() << " total bytes.");
  }

  if (CompressionDictionaryBuilder::insertEntry(value)) {
    total_value_size_ += value_copies_.back().getInstanceByteLength();
    return true;
  } else {
    return false;
  }
}

bool VariableLengthTypeCompressionDictionaryBuilder::insertEntryByReference(const TypeInstance &value) {
  if (total_value_size_ + value.getInstanceByteLength() > numeric_limits<uint32_t>::max()) {
    FATAL_ERROR("Attempted to insert a value into a "
                "VariableLengthTypeCompressionDictionaryBuilder which would "
                "overflow the limit of " << numeric_limits<uint32_t>::max() << " total bytes.");
  }

  if (CompressionDictionaryBuilder::insertEntryByReference(value)) {
    total_value_size_ += value.getInstanceByteLength();
    return true;
  } else {
    return false;
  }
}

void VariableLengthTypeCompressionDictionaryBuilder::undoLastInsert() {
  if (last_value_by_reference_ == NULL) {
    total_value_size_ -= value_copies_.back().getInstanceByteLength();
  } else {
    total_value_size_ -= last_value_by_reference_->getInstanceByteLength();
  }

  CompressionDictionaryBuilder::undoLastInsert();
}

}  // namespace quickstep
