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

#ifndef QUICKSTEP_TYPES_COMPRESSION_DICTIONARY_HPP_
#define QUICKSTEP_TYPES_COMPRESSION_DICTIONARY_HPP_

#include <cstddef>
#include <utility>

#include "types/Comparison.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

/**
 * @brief A dictionary which maps short integer codes to typed values. This
 *        class defines a common interface which has implementations for
 *        fixed-length and variable-length types.
 * @note Codes in a CompressionDictionary compare in the same order as the
 *       underlying values.
 **/
class CompressionDictionary {
 public:
  /**
   * @brief Constructor.
   *
   * @param type The type of values being compressed. LessComparison must be
   *        applicable to this Type.
   * @param dictionary_memory The memory location of the physical dictionary.
   * @param dictionary_memory_size The size (in bytes) of the physical
   *        dictionary at dictionary_memory.
   **/
  CompressionDictionary(const Type &type,
                        const void *dictionary_memory,
                        const std::size_t dictionary_memory_size);

  virtual ~CompressionDictionary() {
  }

  /**
   * @brief Get the number of code/value mappings in this dictionary.
   *
   * @return The number of codes/values in this dictionary.
   **/
  inline std::uint32_t numberOfCodes() const {
    return *static_cast<const std::uint32_t*>(dictionary_memory_);
  }

  /**
   * @brief Get the minimum number of bits needed to represent a code for this
   *        dictionary.
   *
   * @return The length of codes for this dictionary in bits.
   **/
  inline std::uint8_t codeLengthBits() const {
    return code_length_bits_;
  }

  /**
   * @brief Get an untyped pointer to the value represented by the specified
   *        code.
   * @note This version is for codes of 8 bits or less. Also see
   *       getUntypedValueForShortCode() and getUntypedValueForCode().
   * @warning It is an error to use this method with a code which does not
   *          exist in this dictionary, i.e. code must be less than
   *          numberOfCodes().
   *
   * @param code The compressed code to get the value for.
   * @return An untyped pointer to the value that corresponds to code.
   **/
  virtual const void* getUntypedValueForByteCode(const std::uint8_t code) const = 0;

  /**
   * @brief Get an untyped pointer to the value represented by the specified
   *        code.
   * @note This version is for codes of 16 bits or less. Also see
   *       getUntypedValueForByteCode() and getUntypedValueForCode().
   * @warning It is an error to use this method with a code which does not
   *          exist in this dictionary, i.e. code must be less than
   *          numberOfCodes().
   *
   * @param code The compressed code to get the value for.
   * @return An untyped pointer to the value that corresponds to code.
   **/
  virtual const void* getUntypedValueForShortCode(const std::uint16_t code) const = 0;

  /**
   * @brief Get an untyped pointer to the value represented by the specified
   *        code.
   * @note This version is for any code up to the maximum length of 32 bits.
   *       Also see getUntypedValueForByteCode() and
   *       getUntypedValueForShortCode().
   * @warning It is an error to use this method with a code which does not
   *          exist in this dictionary, i.e. code must be less than
   *          numberOfCodes().
   *
   * @param code The compressed code to get the value for.
   * @return An untyped pointer to the value that corresponds to code.
   **/
  virtual const void* getUntypedValueForCode(const std::uint32_t code) const = 0;

  /**
   * @brief Get the value represented by the specified code as a TypeInstance.
   * @note This version is for codes of 8 bits or less. Also see
   *       getTypedValueForShortCode() and getTypedValueForCode().
   * @warning It is an error to use this method with a code which does not
   *          exist in this dictionary, i.e. code must be less than
   *          numberOfCodes().
   *
   * @param code The compressed code to get the value for.
   * @return The typed value that corresponds to code.
   **/
  inline ReferenceTypeInstance* getTypedValueForByteCode(const std::uint8_t code) const {
    return type_.makeReferenceTypeInstance(getUntypedValueForByteCode(code));
  }

  /**
   * @brief Get the value represented by the specified code as a TypeInstance.
   * @note This version is for codes of 16 bits or less. Also see
   *       getTypedValueForByteCode() and getTypedValueForCode().
   * @warning It is an error to use this method with a code which does not
   *          exist in this dictionary, i.e. code must be less than
   *          numberOfCodes().
   *
   * @param code The compressed code to get the value for.
   * @return The typed value that corresponds to code.
   **/
  inline ReferenceTypeInstance* getTypedValueForShortCode(const std::uint16_t code) const {
    return type_.makeReferenceTypeInstance(getUntypedValueForShortCode(code));
  }

  /**
   * @brief Get the value represented by the specified code as a TypeInstance.
   * @note This version is for any code up to the maximum length of 32 bits.
   *       Also see getTypedValueForByteCode() and getTypedValueForShortCode().
   * @warning It is an error to use this method with a code which does not
   *          exist in this dictionary, i.e. code must be less than
   *          numberOfCodes().
   *
   * @param code The compressed code to get the value for.
   * @return The typed value that corresponds to code.
   **/
  inline ReferenceTypeInstance* getTypedValueForCode(const std::uint32_t code) const {
    return type_.makeReferenceTypeInstance(getUntypedValueForCode(code));
  }

  /**
   * @brief Get the compressed code that represents the specified untyped
   *        value.
   * @note This uses a binary search to find the appropriate code. It runs in
   *       O(log(n)) time.
   *
   * @param value An untyped pointer to a value, which must be of the exact
   *        same Type as the Type used to construct this dictionary.
   * @return The code for value in this dictionary, or the value of
   *         numberOfCodes() (the maximum code plus one) if value is not
   *         contained in this dictionary.
   **/
  std::uint32_t getCodeForUntypedValue(const void *value) const;

  /**
   * @brief Get the compressed code that represents the specified typed value.
   * @note This uses a binary search to find the appropriate code. It runs in
   *       O(log(n)) time.
   *
   * @param value A typed value, which can be either the exact same Type as
   *        the values in this dictionary, or another Type which is comparable
   *        according to LessComparison.
   * @return The code for value in this dictionary, or the value of
   *         numberOfCodes() (the maximum code plus one) if value is not
   *         contained in this dictionary.
   **/
  std::uint32_t getCodeForTypedValue(const TypeInstance &value) const {
    DEBUG_ASSERT(!value.isNull());
    if (value.getType().equals(type_)) {
      return getCodeForUntypedValue(value.getDataPtr());
    } else {
      return getCodeForDifferentTypedValue(value);
    }
  }

  /**
   * @brief Find the first code which is not less than the specified untyped
   *        value, similar to std::lower_bound().
   *
   * @param value An untyped pointer to a value, which must be of the exact
   *        same Type as the Type used to construct this dictionary.
   * @return The first code whose corresponding uncompressed value is not less
   *         than value. May return numberOfCodes() if every value in the
   *         dictionary is less than value.
   **/
  virtual std::uint32_t getLowerBoundCodeForUntypedValue(const void *value) const = 0;

  /**
   * @brief Find the first code which is not less than the specified typed
   *        value, similar to std::lower_bound().
   *
   * @param value A typed value, which can be either the exact same Type as
   *        the values in this dictionary, or another Type which is comparable
   *        according to LessComparison.
   * @return The first code whose corresponding uncompressed value is not less
   *         than value. May return numberOfCodes() if every value in the
   *         dictionary is less than value.
   **/
  std::uint32_t getLowerBoundCodeForTypedValue(const TypeInstance &value) const {
    DEBUG_ASSERT(!value.isNull());
    if (value.getType().equals(type_)) {
      return getLowerBoundCodeForUntypedValue(value.getDataPtr());
    } else {
      return getLowerBoundCodeForDifferentTypedValue(value);
    }
  }

  /**
   * @brief Find the first code which is greater than the specified untyped
   *        value, similar to std::upper_bound().
   *
   * @param value An untyped pointer to a value, which must be of the exact
   *        same Type as the Type used to construct this dictionary.
   * @return The first code whose corresponding uncompressed value is greater
   *         than value. May return numberOfCodes() if every value in the
   *         dictionary is less than or equal to value.
   **/
  virtual std::uint32_t getUpperBoundCodeForUntypedValue(const void *value) const = 0;

  /**
   * @brief Find the first code which is greater than the specified typed
   *        value, similar to std::upper_bound().
   *
   * @param value A typed value, which can be either the exact same Type as
   *        the values in this dictionary, or another Type which is comparable
   *        according to LessComparison.
   * @return The first code whose corresponding uncompressed value is greater
   *         than value. May return numberOfCodes() if every value in the
   *         dictionary is less than or equal to value.
   **/
  std::uint32_t getUpperBoundCodeForTypedValue(const TypeInstance &value) const {
    DEBUG_ASSERT(!value.isNull());
    if (value.getType().equals(type_)) {
      return getUpperBoundCodeForUntypedValue(value.getDataPtr());
    } else {
      return getUpperBoundCodeForDifferentTypedValue(value);
    }
  }

  /**
   * @brief Determine the range of codes that match a specified comparison with
   *        a specified untyped value.
   *
   * @param comp The comparison to evaluate.
   * @param value An untyped pointer to a value, which must be of the exact
   *        same Type as the Type used to construct this dictionary.
   * @return The limits of the range of codes which match the predicate
   *         "coded-value comp value". The range is [first, second) (i.e. it
   *         is inclusive of first but not second).
   **/
  std::pair<std::uint32_t, std::uint32_t> getLimitCodesForComparisonUntyped(
      const Comparison::ComparisonID comp,
      const void *value) const;

  /**
   * @brief Determine the range of codes that match a specified comparison with
   *        a specified typed value.
   *
   * @param comp The comparison to evaluate.
   * @param value A typed value, which can be either the exact same Type as
   *        the values in this dictionary, or another Type which is comparable
   *        according to LessComparison.
   * @return The limits of the range of codes which match the predicate
   *         "coded-value comp value". The range is [first, second) (i.e. it
   *         is inclusive of first but not second).
   **/
  std::pair<std::uint32_t, std::uint32_t> getLimitCodesForComparisonTyped(
      const Comparison::ComparisonID comp,
      const TypeInstance &value) const {
    DEBUG_ASSERT(!value.isNull());
    if (value.getType().equals(type_)) {
      return getLimitCodesForComparisonUntyped(comp, value.getDataPtr());
    } else {
      return getLimitCodesForComparisonDifferentTyped(comp, value);
    }
  }

 protected:
  virtual std::uint32_t getLowerBoundCodeForDifferentTypedValue(const TypeInstance &value) const = 0;
  virtual std::uint32_t getUpperBoundCodeForDifferentTypedValue(const TypeInstance &value) const = 0;

  const Type &type_;
  const void *dictionary_memory_;
  const std::size_t dictionary_memory_size_;
  std::uint8_t code_length_bits_;

  ScopedPtr<UncheckedComparator> less_comparator_;

 private:
  std::uint32_t getCodeForDifferentTypedValue(const TypeInstance &value) const;

  std::pair<std::uint32_t, std::uint32_t> getLimitCodesForComparisonDifferentTyped(
      const Comparison::ComparisonID comp,
      const TypeInstance &value) const;

  DISALLOW_COPY_AND_ASSIGN(CompressionDictionary);
};

/**
 * @brief An implementation of CompressionDictionary for a fixed-length Type.
 **/
class FixedLengthTypeCompressionDictionary : public CompressionDictionary {
 public:
  FixedLengthTypeCompressionDictionary(const Type &type,
                                       const void *dictionary_memory,
                                       const std::size_t dictionary_memory_size);

  virtual ~FixedLengthTypeCompressionDictionary() {
  }

  inline const void* getUntypedValueForByteCode(const std::uint8_t code) const {
    return getUntypedValueHelper<std::uint8_t>(code);
  }

  inline const void* getUntypedValueForShortCode(const std::uint16_t code) const {
    return getUntypedValueHelper<std::uint16_t>(code);
  }

  inline const void* getUntypedValueForCode(const std::uint32_t code) const {
    return getUntypedValueHelper<std::uint32_t>(code);
  }

  std::uint32_t getLowerBoundCodeForUntypedValue(const void *value) const;
  std::uint32_t getUpperBoundCodeForUntypedValue(const void *value) const;

 protected:
  std::uint32_t getLowerBoundCodeForDifferentTypedValue(const TypeInstance &value) const;
  std::uint32_t getUpperBoundCodeForDifferentTypedValue(const TypeInstance &value) const;

 private:
  template <typename CodeType>
  inline const void* getUntypedValueHelper(const CodeType code) const {
    DEBUG_ASSERT(code < numberOfCodes());
    return static_cast<const char*>(dictionary_memory_)
           + sizeof(std::uint32_t)  // Number of codes stored at header of dictionary_memory_.
           + code * type_byte_length_;  // Index into value array.
  }

  const std::size_t type_byte_length_;

  DISALLOW_COPY_AND_ASSIGN(FixedLengthTypeCompressionDictionary);
};

/**
 * @brief An implementation of CompressionDictionary for a variable-length
 *        Type.
 **/
class VariableLengthTypeCompressionDictionary : public CompressionDictionary {
 public:
  VariableLengthTypeCompressionDictionary(const Type &type,
                                          const void *dictionary_memory,
                                          const std::size_t dictionary_memory_size);

  virtual ~VariableLengthTypeCompressionDictionary() {
  }

  inline const void* getUntypedValueForByteCode(const std::uint8_t code) const {
    return getUntypedValueHelper<std::uint8_t>(code);
  }

  inline const void* getUntypedValueForShortCode(const std::uint16_t code) const {
    return getUntypedValueHelper<std::uint16_t>(code);
  }

  inline const void* getUntypedValueForCode(const std::uint32_t code) const {
    return getUntypedValueHelper<std::uint32_t>(code);
  }

  std::uint32_t getLowerBoundCodeForUntypedValue(const void *value) const;
  std::uint32_t getUpperBoundCodeForUntypedValue(const void *value) const;

 protected:
  std::uint32_t getLowerBoundCodeForDifferentTypedValue(const TypeInstance &value) const;
  std::uint32_t getUpperBoundCodeForDifferentTypedValue(const TypeInstance &value) const;

 private:
  bool paranoidOffsetsCheck() const;

  template <typename CodeType>
  inline const void* getUntypedValueHelper(const CodeType code) const {
    DEBUG_ASSERT(code < numberOfCodes());
    const void *retval = variable_length_data_region_
                         + static_cast<const std::uint32_t*>(dictionary_memory_)[code + 1];
    DEBUG_ASSERT(retval < static_cast<const char*>(dictionary_memory_) + dictionary_memory_size_);
    return retval;
  }

  const char *variable_length_data_region_;

  DISALLOW_COPY_AND_ASSIGN(VariableLengthTypeCompressionDictionary);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_COMPRESSION_DICTIONARY_HPP_
