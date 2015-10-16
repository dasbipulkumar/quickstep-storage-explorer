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

#include "types/CompressionDictionary.hpp"

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <utility>

#include "types/Comparison.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

using std::lower_bound;
using std::pair;
using std::size_t;
using std::uint32_t;
using std::upper_bound;

namespace quickstep {

namespace {

// This is templated for CompressionDictionaryT so that non-virtual inline
// versions of getUntypedValueForCode() can be used.
template <typename CompressionDictionaryT>
class CompressionDictionaryIterator : public std::iterator<std::random_access_iterator_tag, const void*> {
 public:
  typedef std::iterator<std::random_access_iterator_tag, const void*>::difference_type difference_type;

  CompressionDictionaryIterator(const CompressionDictionaryT &dictionary, const uint32_t code)
      : dictionary_(&dictionary),
        code_(code) {
  }

  CompressionDictionaryIterator()
      : dictionary_(NULL),
        code_(0) {
  }

  CompressionDictionaryIterator(const CompressionDictionaryIterator &other)
      : dictionary_(other.dictionary_),
        code_(other.code_) {
  }

  CompressionDictionaryIterator& operator=(const CompressionDictionaryIterator& other) {
    if (this != &other) {
      dictionary_ = other.dictionary_;
      code_ = other.code_;
    }
    return *this;
  }

  // Comparisons.
  inline bool operator==(const CompressionDictionaryIterator& other) const {
    DEBUG_ASSERT(dictionary_ == other.dictionary_);
    return code_ == other.code_;
  }

  inline bool operator!=(const CompressionDictionaryIterator& other) const {
    DEBUG_ASSERT(dictionary_ == other.dictionary_);
    return code_ != other.code_;
  }

  inline bool operator<(const CompressionDictionaryIterator& other) const {
    DEBUG_ASSERT(dictionary_ == other.dictionary_);
    return code_ < other.code_;
  }

  inline bool operator<=(const CompressionDictionaryIterator& other) const {
    DEBUG_ASSERT(dictionary_ == other.dictionary_);
    return code_ <= other.code_;
  }

  inline bool operator>(const CompressionDictionaryIterator& other) const {
    DEBUG_ASSERT(dictionary_ == other.dictionary_);
    return code_ > other.code_;
  }

  inline bool operator>=(const CompressionDictionaryIterator& other) const {
    DEBUG_ASSERT(dictionary_ == other.dictionary_);
    return code_ >= other.code_;
  }

  // Increment/decrement.
  inline CompressionDictionaryIterator& operator++() {
    ++code_;
    return *this;
  }

  CompressionDictionaryIterator operator++(int) {  // NOLINT - increment operator doesn't need named param
    CompressionDictionaryIterator result(*this);
    ++(*this);
    return result;
  }

  inline CompressionDictionaryIterator& operator--() {
    --code_;
    return *this;
  }

  CompressionDictionaryIterator operator--(int) {  // NOLINT - decrement operator doesn't need named param
    CompressionDictionaryIterator result(*this);
    --(*this);
    return result;
  }

  // Compound assignment.
  inline CompressionDictionaryIterator& operator+=(difference_type n) {
    code_ += n;
    return *this;
  }

  inline CompressionDictionaryIterator& operator-=(difference_type n) {
    code_ -= n;
    return *this;
  }

  // Note: + operator with difference_type on the left is not defined.
  CompressionDictionaryIterator operator+(difference_type n) const {
    return CompressionDictionaryIterator(dictionary_, code_ + n);
  }

  CompressionDictionaryIterator operator-(difference_type n) const {
    return CompressionDictionaryIterator(dictionary_, code_ - n);
  }

  difference_type operator-(const CompressionDictionaryIterator &other) const {
    DEBUG_ASSERT(dictionary_ == other.dictionary_);
    return code_ - other.code_;
  }

  // Dereference.
  inline const void* operator*() const {
    DEBUG_ASSERT(dictionary_ != NULL);
    return dictionary_->getUntypedValueForCode(code_);
  }

  inline const void** operator->() const {
    FATAL_ERROR("-> dereference operator unimplemented for CompressionDictionaryIterator.");
  }

  const void* operator[](difference_type n) const {
    DEBUG_ASSERT(dictionary_ != NULL);
    return dictionary_->getUntypedValueForCode(code_ + n);
  }

  uint32_t getCode() const {
    return code_;
  }

 private:
  const CompressionDictionaryT *dictionary_;
  uint32_t code_;
};

}  // anonymous namespace


CompressionDictionary::CompressionDictionary(
    const Type &type,
    const void *dictionary_memory,
    const std::size_t dictionary_memory_size)
    : type_(type),
      dictionary_memory_(dictionary_memory),
      dictionary_memory_size_(dictionary_memory_size) {
  uint32_t num_codes = numberOfCodes();
  for (code_length_bits_ = 32; code_length_bits_ > 0; --code_length_bits_) {
    if (num_codes >> (code_length_bits_ - 1)) {
      break;
    }
  }

  const Comparison &less_comparison = Comparison::GetComparison(Comparison::kLess);
  if (!less_comparison.canCompareTypes(type_, type_)) {
    FATAL_ERROR("Attempted to create a CompressionDictionary for a Type which can't be ordered by LessComparison.");
  }
  less_comparator_.reset(less_comparison.makeUncheckedComparatorForTypes(type_, type_));
}

uint32_t CompressionDictionary::getCodeForUntypedValue(const void *value) const {
  uint32_t candidate_code = getLowerBoundCodeForUntypedValue(value);
  if (candidate_code == numberOfCodes()) {
    return candidate_code;
  }

  if (less_comparator_->compareDataPtrs(value, getUntypedValueForCode(candidate_code))) {
    return numberOfCodes();
  } else {
    return candidate_code;
  }
}

std::pair<uint32_t, uint32_t> CompressionDictionary::getLimitCodesForComparisonUntyped(
    const Comparison::ComparisonID comp,
    const void *value) const {
  pair<uint32_t, uint32_t> limit_codes;
  switch (comp) {
    case Comparison::kEqual:
      limit_codes.first = getCodeForUntypedValue(value);
      limit_codes.second = (limit_codes.first == numberOfCodes())
                           ? limit_codes.first
                           : limit_codes.first + 1;
      break;
    case Comparison::kNotEqual:
      FATAL_ERROR("Called CompressionDictionary::getLimitCodesForComparisonUntyped() "
                  "with comparison kNotEqual, which is not allowed.");
    case Comparison::kLess:
      limit_codes.first = 0;
      limit_codes.second = getLowerBoundCodeForUntypedValue(value);
      break;
    case Comparison::kLessOrEqual:
      limit_codes.first = 0;
      limit_codes.second = getUpperBoundCodeForUntypedValue(value);
      break;
    case Comparison::kGreater:
      limit_codes.first = getUpperBoundCodeForUntypedValue(value);
      limit_codes.second = numberOfCodes();
      break;
    case Comparison::kGreaterOrEqual:
      limit_codes.first = getLowerBoundCodeForUntypedValue(value);
      limit_codes.second = numberOfCodes();
      break;
    default:
      FATAL_ERROR("Unknown comparison in CompressionDictionary::getLimitCodesForComparisonUntyped().");
  }

  return limit_codes;
}

std::uint32_t CompressionDictionary::getCodeForDifferentTypedValue(const TypeInstance &value) const {
  uint32_t candidate_code = getLowerBoundCodeForDifferentTypedValue(value);
  if (candidate_code == numberOfCodes()) {
    return candidate_code;
  }

  ScopedPtr<UncheckedComparator> check_comp(
      Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(
          value.getType(),
          type_));
  if (check_comp->compareTypeInstanceWithDataPtr(value, getUntypedValueForCode(candidate_code))) {
    return numberOfCodes();
  } else {
    return candidate_code;
  }
}

std::pair<uint32_t, uint32_t> CompressionDictionary::getLimitCodesForComparisonDifferentTyped(
    const Comparison::ComparisonID comp,
    const TypeInstance &value) const {
  pair<uint32_t, uint32_t> limit_codes;
  switch (comp) {
    case Comparison::kEqual:
      limit_codes.first = getCodeForDifferentTypedValue(value);
      limit_codes.second = (limit_codes.first == numberOfCodes())
                           ? limit_codes.first
                           : limit_codes.first + 1;
      break;
    case Comparison::kNotEqual:
      FATAL_ERROR("Called CompressionDictionary::getLimitCodesForComparisonTyped() "
                  "with comparison kNotEqual, which is not allowed.");
    case Comparison::kLess:
      limit_codes.first = 0;
      limit_codes.second = getLowerBoundCodeForDifferentTypedValue(value);
      break;
    case Comparison::kLessOrEqual:
      limit_codes.first = 0;
      limit_codes.second = getUpperBoundCodeForDifferentTypedValue(value);
      break;
    case Comparison::kGreater:
      limit_codes.first = getUpperBoundCodeForDifferentTypedValue(value);
      limit_codes.second = numberOfCodes();
      break;
    case Comparison::kGreaterOrEqual:
      limit_codes.first = getLowerBoundCodeForDifferentTypedValue(value);
      limit_codes.second = numberOfCodes();
      break;
    default:
      FATAL_ERROR("Unknown comparison in CompressionDictionary::getLimitCodesForComparisonTyped().");
  }

  return limit_codes;
}

FixedLengthTypeCompressionDictionary::FixedLengthTypeCompressionDictionary(
    const Type &type,
    const void *dictionary_memory,
    const std::size_t dictionary_memory_size)
    : CompressionDictionary(type, dictionary_memory, dictionary_memory_size),
      type_byte_length_(type.maximumByteLength()) {
  if (type_.isVariableLength()) {
    FATAL_ERROR("Attempted to create a FixedLengthTypeCompressionDictionary for a variable-length Type.");
  }

  if (numberOfCodes() * type_byte_length_ + sizeof(uint32_t) < dictionary_memory_size_) {
    FATAL_ERROR("Attempted to create a FixedLengthTypeCompressionDictionary with "
                << dictionary_memory_size_ << " bytes of memory, which is insufficient for "
                << numberOfCodes() << " entries of type " << type_.getName() << ".");
  }
  // NOTE(chasseur): If dictionary_memory_size_ is larger than the required
  // amount of memory, it's not strictly an error, but there will be wasted
  // space.
}

uint32_t FixedLengthTypeCompressionDictionary::getLowerBoundCodeForUntypedValue(const void *value) const {
  return lower_bound(CompressionDictionaryIterator<FixedLengthTypeCompressionDictionary>(*this, 0),
                     CompressionDictionaryIterator<FixedLengthTypeCompressionDictionary>(*this, numberOfCodes()),
                     value,
                     STLUncheckedComparatorWrapper(*less_comparator_)).getCode();
}

uint32_t FixedLengthTypeCompressionDictionary::getUpperBoundCodeForUntypedValue(const void *value) const {
  return upper_bound(CompressionDictionaryIterator<FixedLengthTypeCompressionDictionary>(*this, 0),
                     CompressionDictionaryIterator<FixedLengthTypeCompressionDictionary>(*this, numberOfCodes()),
                     value,
                     STLUncheckedComparatorWrapper(*less_comparator_)).getCode();
}

uint32_t FixedLengthTypeCompressionDictionary::getLowerBoundCodeForDifferentTypedValue(
    const TypeInstance &value) const {
  // NOTE(chasseur): A standards-compliant implementation of lower_bound always
  // compares the iterator on the left with the literal on the right.
  ScopedPtr<UncheckedComparator> comp(
      Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(
          type_,
          value.getType()));
  return lower_bound(CompressionDictionaryIterator<FixedLengthTypeCompressionDictionary>(*this, 0),
                     CompressionDictionaryIterator<FixedLengthTypeCompressionDictionary>(*this, numberOfCodes()),
                     value.getDataPtr(),
                     STLUncheckedComparatorWrapper(*comp)).getCode();
}

uint32_t FixedLengthTypeCompressionDictionary::getUpperBoundCodeForDifferentTypedValue(
    const TypeInstance &value) const {
  // NOTE(chasseur): A standards-compliant implementation of upper_bound always
  // compares the literal on the left with the iterator on the right.
  ScopedPtr<UncheckedComparator> comp(
      Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(
          value.getType(),
          type_));
  return upper_bound(CompressionDictionaryIterator<FixedLengthTypeCompressionDictionary>(*this, 0),
                     CompressionDictionaryIterator<FixedLengthTypeCompressionDictionary>(*this, numberOfCodes()),
                     value.getDataPtr(),
                     STLUncheckedComparatorWrapper(*comp)).getCode();
}

VariableLengthTypeCompressionDictionary::VariableLengthTypeCompressionDictionary(
    const Type &type,
    const void *dictionary_memory,
    const std::size_t dictionary_memory_size)
    : CompressionDictionary(type, dictionary_memory, dictionary_memory_size) {
  uint32_t num_codes = numberOfCodes();
  if (dictionary_memory_size_ <
      sizeof(uint32_t) + num_codes * (sizeof(uint32_t) + type_.minimumByteLength())) {
    FATAL_ERROR("Attempted to create a VariableLengthTypeCompressionDictionary with "
                << dictionary_memory_size_ << " bytes of memory, which is insufficient for "
                << num_codes << " entries of type " << type_.getName() << ".");
  }

  variable_length_data_region_ = static_cast<const char*>(dictionary_memory_)
                                 + (num_codes + 1) * sizeof(uint32_t);

  DEBUG_ASSERT(paranoidOffsetsCheck());
}

uint32_t VariableLengthTypeCompressionDictionary::getLowerBoundCodeForUntypedValue(const void *value) const {
  return lower_bound(CompressionDictionaryIterator<VariableLengthTypeCompressionDictionary>(*this, 0),
                     CompressionDictionaryIterator<VariableLengthTypeCompressionDictionary>(*this, numberOfCodes()),
                     value,
                     STLUncheckedComparatorWrapper(*less_comparator_)).getCode();
}

uint32_t VariableLengthTypeCompressionDictionary::getUpperBoundCodeForUntypedValue(const void *value) const {
  return upper_bound(CompressionDictionaryIterator<VariableLengthTypeCompressionDictionary>(*this, 0),
                     CompressionDictionaryIterator<VariableLengthTypeCompressionDictionary>(*this, numberOfCodes()),
                     value,
                     STLUncheckedComparatorWrapper(*less_comparator_)).getCode();
}

uint32_t VariableLengthTypeCompressionDictionary::getLowerBoundCodeForDifferentTypedValue(
    const TypeInstance &value) const {
  // NOTE(chasseur): A standards-compliant implementation of lower_bound always
  // compares the iterator on the left with the literal on the right.
  ScopedPtr<UncheckedComparator> comp(
      Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(
          type_,
          value.getType()));
  return lower_bound(CompressionDictionaryIterator<VariableLengthTypeCompressionDictionary>(*this, 0),
                     CompressionDictionaryIterator<VariableLengthTypeCompressionDictionary>(*this, numberOfCodes()),
                     value.getDataPtr(),
                     STLUncheckedComparatorWrapper(*comp)).getCode();
}

uint32_t VariableLengthTypeCompressionDictionary::getUpperBoundCodeForDifferentTypedValue(
    const TypeInstance &value) const {
  // NOTE(chasseur): A standards-compliant implementation of upper_bound always
  // compares the literal on the left with the iterator on the right.
  ScopedPtr<UncheckedComparator> comp(
      Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(
          value.getType(),
          type_));
  return upper_bound(CompressionDictionaryIterator<VariableLengthTypeCompressionDictionary>(*this, 0),
                     CompressionDictionaryIterator<VariableLengthTypeCompressionDictionary>(*this, numberOfCodes()),
                     value.getDataPtr(),
                     STLUncheckedComparatorWrapper(*comp)).getCode();
}

bool VariableLengthTypeCompressionDictionary::paranoidOffsetsCheck() const {
  uint32_t num_codes = numberOfCodes();
  size_t variable_length_offset = (num_codes + 1) * sizeof(uint32_t);
  const uint32_t *offsets_array = static_cast<const uint32_t*>(dictionary_memory_) + 1;

  size_t expected_value_offset = variable_length_offset;
  for (uint32_t code = 0; code < num_codes; ++code) {
    size_t value_offset = variable_length_offset + offsets_array[code];
    if ((value_offset >= dictionary_memory_size_)
        || (value_offset != expected_value_offset)) {
      return false;
    }

    ScopedPtr<ReferenceTypeInstance> value(getTypedValueForCode(code));
    expected_value_offset = value_offset + value->getInstanceByteLength();
    if (expected_value_offset > dictionary_memory_size_) {
      return false;
    }
  }

  return true;
}

}  // namespace quickstep
