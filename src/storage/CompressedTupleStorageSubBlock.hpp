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

#ifndef QUICKSTEP_STORAGE_COMPRESSED_TUPLE_STORAGE_SUB_BLOCK_HPP_
#define QUICKSTEP_STORAGE_COMPRESSED_TUPLE_STORAGE_SUB_BLOCK_HPP_

#include <cstddef>
#include <limits>
#include <utility>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/CompressedBlockBuilder.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageErrors.hpp"
#include "storage/TupleIdSequence.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/Comparison.hpp"
#include "types/CompressionDictionary.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrMap.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

class CatalogRelation;
class TupleStorageSubBlockDescription;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief Abstract base class which implements common functionality for
 *        CompressedPackedRowStoreTupleStorageSubBlock and
 *        CompressedColumnStoreTupleStorageSubBlock.
 **/
class CompressedTupleStorageSubBlock : public TupleStorageSubBlock {
 public:
  CompressedTupleStorageSubBlock(
      const CatalogRelation &relation,
      const TupleStorageSubBlockDescription &description,
      const bool new_block,
      void *sub_block_memory,
      const std::size_t sub_block_memory_size);

  virtual ~CompressedTupleStorageSubBlock() {
  }

  /**
   * @brief Get a long value for a comparison of a truncated attribute with a
   *        literal TypeInstance. If the literal is a Float or Double with a
   *        fractional part, the value will be adjusted so that the comparison
   *        will still evaluate correctly.
   *
   * @param comp The ID of the comparison being evaluated (the order is
   *        'attribute comp literal'). Must be kLess, kLessOrEqual, kGreater,
   *        or kGreaterOrEqual (kEqual and kNotEqual are not supported).
   * @param right_literal A literal of a numeric type (Int, Long, Float, or
   *        Double) to get the effective truncated value for.
   * @return The value of right_literal as a long, adjusted as necessary so
   *         that the specified comparison still evaluates correctly.
   **/
  static std::int64_t GetEffectiveLiteralValueForComparisonWithTruncatedAttribute(
      const Comparison::ComparisonID comp,
      const TypeInstance &right_literal);

  bool supportsUntypedGetAttributeValue(const attribute_id attr) const {
    return !truncated_attributes_[attr];
  }

  bool supportsAdHocInsert() const {
    return false;
  }

  bool adHocInsertIsEfficient() const {
    return false;
  }

  bool isEmpty() const {
    if (builder_.empty()) {
      return *static_cast<const tuple_id*>(sub_block_memory_) == 0;
    } else {
      return builder_->numTuples() == 0;
    }
  }

  bool isPacked() const {
    return true;
  }

  tuple_id getMaxTupleID() const {
    return *static_cast<const tuple_id*>(sub_block_memory_) - 1;
  }

  bool hasTupleWithID(const tuple_id tuple) const {
    return tuple < *static_cast<const tuple_id*>(sub_block_memory_);
  }

  InsertResult insertTuple(const Tuple &tuple, const AllowedTypeConversion atc) {
    return InsertResult(-1, false);
  }

  bool insertTupleInBatch(const Tuple &tuple, const AllowedTypeConversion atc);

  const void* getAttributeValue(const tuple_id tuple, const attribute_id attr) const;
  TypeInstance* getAttributeValueTyped(const tuple_id tuple, const attribute_id attr) const;

  // This override can more efficiently evaluate comparisons between a
  // compressed attribute and a literal value.
  virtual TupleIdSequence* getMatchesForPredicate(const Predicate *predicate) const;

  bool isCompressed() const {
    return true;
  }

  // NOTE(chasseur): The methods below, with the prefix "compressed", are
  // intended for use by IndexSubBlock implementations which are capable of
  // using compressed codes directly. They can be accessed by checking that
  // isCompressed() is true, then casting to CompressedTupleStorageSubBlock.

  /**
   * @brief Check if the physical block has been built and compression has been
   *        finalized.
   *
   * @return true if the physical block has been built by a call to rebuild(),
   *         false if building is still in progress.
   **/
  bool compressedBlockIsBuilt() const {
    return builder_.empty();
  }

  /**
   * @brief Check if an attribute may be compressed in a block which is not yet
   *        built.
   * @warning This method can only be called if compressedBlockIsBuilt()
   *          returns false.
   * @note Even if this method returns true, the attribute specified might
   *       still be uncompressed when the block is built if compression fails.
   *
   * @param attr_id The ID of the attribute to check for possible compression.
   * @return true if the attribute specified by attr_id might be compressed
   *         when rebuild() is called, false if no compression will be
   *         attempted.
   **/
  bool compressedUnbuiltBlockAttributeMayBeCompressed(const attribute_id attr_id) const {
    DEBUG_ASSERT(!builder_.empty());
    return builder_->attributeMayBeCompressed(attr_id);
  }

  /**
   * @brief Check if an attribute is dictionary-compressed.
   * @warning This method can only be called if compressedBlockIsBuilt()
   *          returns true.
   *
   * @param attr_id The ID of the attribute to check for dictionary
   *        compression.
   * @return true if the attribute specified by attr_id is dictionary-
   *         compressed, false otherwise.
   **/
  inline bool compressedAttributeIsDictionaryCompressed(const attribute_id attr_id) const {
    DEBUG_ASSERT(builder_.empty());
    return dictionary_coded_attributes_[attr_id];
  }

  /**
   * @brief Check if an attribute is trunctation-compressed.
   * @warning This method can only be called if compressedBlockIsBuilt()
   *          returns true.
   *
   * @param attr_id The ID of the attribute to check for truncation.
   * @return true if the attribute specified by attr_id is an Int or Long which
   *         is truncated, false otherwise.
   **/
  inline bool compressedAttributeIsTruncationCompressed(const attribute_id attr_id) const {
    DEBUG_ASSERT(builder_.empty());
    return truncated_attributes_[attr_id];
  }

  /**
   * @brief Determine the compressed byte-length of an attribute.
   * @warning This method can only be called if compressedBlockIsBuilt()
   *          returns true.
   *
   * @param attr_id The ID of the attribute to check the size of.
   * @return The compressed length of the attribute specified by attr_id in
   *         bytes (or the original byte-length if the attribute is
   *         uncompressed).
   **/
  std::size_t compressedGetCompressedAttributeSize(const attribute_id attr_id) const {
    DEBUG_ASSERT(builder_.empty());
    return compression_info_.attribute_size(attr_id);
  }

  /**
   * @brief Get the CompressionDictionary used for compressing an attribute.
   * @warning compressedAttributeIsDictionaryCompressed() must return true for
   *          the specified attribute.
   *
   * @param attr_id The ID of the attribute to get the CompressionDictionary
   *        for.
   * @return The CompressionDictionary used to compress the attribute specified
   *         by attr_id.
   **/
  const CompressionDictionary& compressedGetDictionary(const attribute_id attr_id) const {
    DEBUG_ASSERT(builder_.empty());
    PtrMap<attribute_id, CompressionDictionary>::const_iterator dict_it = dictionaries_.find(attr_id);
    if (dict_it == dictionaries_.end()) {
      FATAL_ERROR("Called CompressedTupleStorageSubBlock::getCompressionDictionary() "
                  "for an attribute which is not dictionary-compressed.");
    } else {
      return *(dict_it->second);
    }
  }

  /**
   * @brief Determine if a comparison must always be true for any possible
   *        value of a truncated attribute.
   * @warning compressedAttributeIsTruncationCompressed() must return true for
   *          the specified attribute.
   *
   * @param comp The comparison to evaluate.
   * @param left_attr_id The ID of the truncated attribute on the left side
   *        of the comparison.
   * @param right_literal The literal value on the right side of the
   *        comparison.
   * @return true if 'left_attr_id comp right_literal' must always be true for
   *         all possible values of the attribute specified by left_attr_id.
   **/
  bool compressedComparisonIsAlwaysTrueForTruncatedAttribute(
      const Comparison::ComparisonID comp,
      const attribute_id left_attr_id,
      const TypeInstance &right_literal) const;

  /**
   * @brief Determine if a comparison must always be false for any possible
   *        value of a truncated attribute.
   * @warning compressedAttributeIsTruncationCompressed() must return true for
   *          the specified attribute.
   *
   * @param comp The comparison to evaluate.
   * @param left_attr_id The ID of the truncated attribute on the left side
   *        of the comparison.
   * @param right_literal The literal value on the right side of the
   *        comparison.
   * @return true if 'left_attr_id comp right_literal' must always be false for
   *         all possible values of the attribute specified by left_attr_id.
   **/
  bool compressedComparisonIsAlwaysFalseForTruncatedAttribute(
      const Comparison::ComparisonID comp,
      const attribute_id left_attr_id,
      const TypeInstance &right_literal) const;

  /**
   * @brief Get the compressed code for the specified tuple and compressed
   *        attribute.
   * @warning The specified attribute must be compressed, i.e. either
   *          compressedAttributeIsDictionaryCompressed() or
   *          compressedAttributeIsTruncationCompressed() must be true for it.
   *
   * @param tid The ID of the desired tuple.
   * @param attr_id The ID of the compressed attribute to get the code for.
   **/
  virtual std::uint32_t compressedGetCode(const tuple_id tid,
                                          const attribute_id attr_id) const = 0;

 protected:
  inline static std::int64_t GetMaxTruncatedValue(const std::size_t byte_length) {
    switch (byte_length) {
      case 1:
        return std::numeric_limits<std::uint8_t>::max();
      case 2:
        return std::numeric_limits<std::uint16_t>::max();
      case 4:
        return std::numeric_limits<std::uint32_t>::max() - 1;
      default:
        FATAL_ERROR("Unexpected byte_length for truncated value in "
                    "CompressedTupleStorageSubBlock::GetMaxTruncatedValue()");
    }
  }

  void* initializeCommon();

  virtual const void* getAttributePtr(const tuple_id tid,
                                      const attribute_id attr_id) const = 0;

  virtual TupleIdSequence* getEqualCodes(const attribute_id attr_id,
                                         const std::uint32_t code) const = 0;

  virtual TupleIdSequence* getNotEqualCodes(const attribute_id attr_id,
                                            const std::uint32_t code) const = 0;

  virtual TupleIdSequence* getLessCodes(const attribute_id attr_id,
                                        const std::uint32_t code) const = 0;

  virtual TupleIdSequence* getGreaterOrEqualCodes(const attribute_id attr_id,
                                                  const std::uint32_t code) const = 0;

  virtual TupleIdSequence* getCodesInRange(const attribute_id attr_id,
                                           const std::pair<std::uint32_t, std::uint32_t> range) const = 0;

  ScopedPtr<CompressedBlockBuilder> builder_;

  CompressedBlockInfo compression_info_;

  std::vector<bool> dictionary_coded_attributes_;
  std::vector<bool> truncated_attributes_;

 private:
  TupleIdSequence* evaluateEqualPredicateOnCompressedAttribute(
      const attribute_id left_attr_id,
      const TypeInstance &right_literal) const;

  TupleIdSequence* evaluateNotEqualPredicateOnCompressedAttribute(
      const attribute_id left_attr_id,
      const TypeInstance &right_literal) const;

  TupleIdSequence* evaluateOtherComparisonPredicateOnCompressedAttribute(
      const Comparison::ComparisonID comp,
      const attribute_id left_attr_id,
      const TypeInstance &right_literal) const;

  PtrMap<attribute_id, CompressionDictionary> dictionaries_;

  DISALLOW_COPY_AND_ASSIGN(CompressedTupleStorageSubBlock);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_COMPRESSED_TUPLE_STORAGE_SUB_BLOCK_HPP_
