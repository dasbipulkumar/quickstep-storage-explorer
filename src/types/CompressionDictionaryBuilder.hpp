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

#ifndef QUICKSTEP_TYPES_COMPRESSION_DICTIONARY_BUILDER_HPP_
#define QUICKSTEP_TYPES_COMPRESSION_DICTIONARY_BUILDER_HPP_

#include <cstddef>
#include <map>

#include "types/Comparison.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

/**
 * @brief An object which accumulates typed values and builds a physical
 *        dictionary for a CompressionDictionary object. This class defines a
 *        common interface which has an implementation for fixed-length Types,
 *        FixedLengthTypeCompressionDictionaryBuilder (for use with
 *        FixedLengthTypeCompressionDictionary), and an implementation for
 *        variable-length Types, VariableLengthTypeCompressionDictionaryBuilder
 *        (for use with VariableLengthTypeCompressionDictionary).
 **/
class CompressionDictionaryBuilder {
 public:
  /**
   * @brief Constructor.
   *
   * @param type The Type to build a CompressionDictionary for.
   **/
  explicit CompressionDictionaryBuilder(const Type &type);

  /**
   * @brief Destructor.
   **/
  virtual ~CompressionDictionaryBuilder() {
  }

  /**
   * @brief Get the number of entries (unique values/codes) in the dictionary
   *        being built.
   *
   * @return The number of entries in the dictionary.
   **/
  std::uint32_t numberOfEntries() const {
    return value_map_->size();
  }

  /**
   * @brief Get the number of bits needed to represent a code in the dictionary
   *        being built.
   *
   * @return The length, in bits, of codes for the dictionary.
   **/
  std::uint8_t codeLengthBits() const {
    return code_length_bits_;
  }

  /**
   * @brief Get the number of bytes used to represent a code in the dictionary
   *        being build when all codes are padded up to the next power-of-two
   *        number of bytes.
   *
   * @return The length, in bytes, of codes padded up to a power-of-two bytes.
   **/
  std::uint8_t codeLengthPaddedBytes() const {
    if (code_length_bits_ < 9) {
      return 1;
    } else if (code_length_bits_ < 17) {
      return 2;
    } else {
      return 4;
    }
  }

  /**
   * @brief Get the number of bytes needed to store the physical dictionary
   *        being built.
   *
   * @return The size, in bytes, of the dictionary.
   **/
  virtual std::size_t dictionarySizeBytes() const = 0;

  /**
   *
   **/
  bool containsValue(const TypeInstance &value) const {
    DEBUG_ASSERT(value.getType().equals(type_));
    if (value.isNull()) {
      return false;
    }
    return value_map_->find(value.getDataPtr()) != value_map_->end();
  }

  /**
   * @brief Construct a physical dictionary in the specified memory location.
   *
   * @param location The memory location where the physical dictionary should
   *        be built. Must have dictionarySizeBytes() available to write at
   *        location.
   **/
  virtual void buildDictionary(void *location) const = 0;

  /**
   * @brief Add a value to the dictionary being built.
   * @note This method makes a copy of the value passed in. If the caller can
   *       guarantee that value remains in existence for the life of this
   *       CompressionDictionaryBuilder, it is more memory-efficient to use
   *       insertEntryByReference() instead.
   *
   * @param value A typed value to add to the dictionary.
   * @return True if value has been added, false if it was already present and
   *         the dictionary was not modified.
   **/
  virtual bool insertEntry(const TypeInstance &value);

  /**
   * @brief Add a value to the dictionary being built without copying it.
   * @warning The caller must ensure that value is not deleted until after
   *          done using this CompressionDictionaryBuilder.
   *
   * @param value A typed value to add to the dictionary.
   * @return True if value has been added, false if it was already present and
   *         the dictionary was not modified.
   **/
  virtual bool insertEntryByReference(const TypeInstance &value);

  /**
   * @brief Remove the last entry successfully added to the dictionary via
   *        insertEntry(), reducing the dictionary size and potentially
   *        reducing the code length in bits.
   **/
  virtual void undoLastInsert();

 protected:
  const Type &type_;

  ScopedPtr<UncheckedComparator> less_comparator_;
  ScopedPtr<std::map<const void*, const TypeInstance*, STLUncheckedComparatorWrapper> > value_map_;
  PtrVector<LiteralTypeInstance> value_copies_;
  const TypeInstance *last_value_by_reference_;

  std::uint8_t code_length_bits_;

 private:
  DISALLOW_COPY_AND_ASSIGN(CompressionDictionaryBuilder);
};

/**
 * @brief An implementation of CompressionDictionaryBuilder for fixed-length
 *        Types, for use with FixedLengthTypeCompressionDictionary.
 **/
class FixedLengthTypeCompressionDictionaryBuilder : public CompressionDictionaryBuilder {
 public:
  explicit FixedLengthTypeCompressionDictionaryBuilder(const Type &type);

  std::size_t dictionarySizeBytes() const {
    return sizeof(std::uint32_t) + value_map_->size() * type_.maximumByteLength();
  }

  void buildDictionary(void *location) const;

 private:
  DISALLOW_COPY_AND_ASSIGN(FixedLengthTypeCompressionDictionaryBuilder);
};

/**
 * @brief An implementation of CompressionDictionaryBuilder for variable-length
 *        Types, for use with VariableLengthTypeCompressionDictionary.
 **/
class VariableLengthTypeCompressionDictionaryBuilder : public CompressionDictionaryBuilder {
 public:
  explicit VariableLengthTypeCompressionDictionaryBuilder(const Type &type);

  virtual ~VariableLengthTypeCompressionDictionaryBuilder() {
  }

  std::size_t dictionarySizeBytes() const {
    return (value_map_->size() + 1) * sizeof(std::uint32_t) + total_value_size_;
  }

  void buildDictionary(void *location) const;

  bool insertEntry(const TypeInstance &value);
  bool insertEntryByReference(const TypeInstance &value);

  void undoLastInsert();

 private:
  std::size_t total_value_size_;

  DISALLOW_COPY_AND_ASSIGN(VariableLengthTypeCompressionDictionaryBuilder);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_COMPRESSION_DICTIONARY_BUILDER_HPP_
