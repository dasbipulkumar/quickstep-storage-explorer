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
#include "storage/CompressedBlockBuilder.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <utility>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "types/Comparison.hpp"
#include "types/CompressionDictionary.hpp"
#include "types/CompressionDictionaryBuilder.hpp"
#include "types/Tuple.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/BitManipulation.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

using std::numeric_limits;
using std::pair;
using std::sort;
using std::uint8_t;
using std::uint16_t;
using std::uint32_t;
using std::uint64_t;
using std::vector;

namespace quickstep {

namespace {

class TupleComparator {
 public:
  TupleComparator(const attribute_id sort_attribute_id,
                  const UncheckedComparator &comparator)
      : sort_attribute_id_(sort_attribute_id),
        internal_comparator_(comparator) {
  }

  inline bool operator() (const Tuple *left, const Tuple *right) const {
    return internal_comparator_.compareTypeInstances(left->getAttributeValue(sort_attribute_id_),
                                                     right->getAttributeValue(sort_attribute_id_));
  }

 private:
  const attribute_id sort_attribute_id_;
  const UncheckedComparator &internal_comparator_;
};

}  // anonymous namespace

CompressedBlockBuilder::CompressedBlockBuilder(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description,
    const std::size_t block_size)
    : relation_(relation),
      block_size_(block_size),
      sort_attribute_id_(0) {
  CompatUnorderedSet<attribute_id>::unordered_set compressed_attribute_ids;

  if (description.sub_block_type() == TupleStorageSubBlockDescription::COMPRESSED_PACKED_ROW_STORE) {
    for (int compressed_attr_num = 0;
         compressed_attr_num < description.ExtensionSize(
             CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id);
         ++compressed_attr_num) {
      compressed_attribute_ids.insert(
          description.GetExtension(CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id,
                                   compressed_attr_num));
    }
  } else if (description.sub_block_type() == TupleStorageSubBlockDescription::COMPRESSED_COLUMN_STORE) {
    if (!description.HasExtension(CompressedColumnStoreTupleStorageSubBlockDescription::sort_attribute_id)) {
      FATAL_ERROR("Attempted to create a CompressedBlockBuilder with a "
                  "TupleStorageSubBlockDescription that specified a "
                  "sub_block_type of COMPRESSED_COLUMN_STORE, but did not "
                  "specify a sort_attribute_id.");
    }
    sort_attribute_id_ = description.GetExtension(
        CompressedColumnStoreTupleStorageSubBlockDescription::sort_attribute_id);

    for (int compressed_attr_num = 0;
         compressed_attr_num < description.ExtensionSize(
             CompressedColumnStoreTupleStorageSubBlockDescription::compressed_attribute_id);
         ++compressed_attr_num) {
      compressed_attribute_ids.insert(
          description.GetExtension(CompressedColumnStoreTupleStorageSubBlockDescription::compressed_attribute_id,
                                   compressed_attr_num));
    }
  } else {
    FATAL_ERROR("Attempted to create a CompressedBlockBuilder with a "
                "TupleStorageSubBlockDescription that did not specify a "
                "compressed sub_block_type.");
  }

  for (attribute_id attr_num = 0;
       attr_num <= relation.getMaxAttributeId();
       ++attr_num) {
    compression_info_.add_attribute_size(0);
    compression_info_.add_dictionary_size(0);

    if (relation_.hasAttributeWithId(attr_num)
        && (compressed_attribute_ids.find(attr_num) != compressed_attribute_ids.end())) {
      const Type &attr_type = relation_.getAttributeById(attr_num).getType();
      if (attr_type.isVariableLength()) {
        dictionary_builders_.insert(attr_num,
                                    new VariableLengthTypeCompressionDictionaryBuilder(attr_type));
      } else {
        dictionary_builders_.insert(attr_num,
                                    new FixedLengthTypeCompressionDictionaryBuilder(attr_type));
      }
      if ((attr_type.getTypeID() == Type::kInt)
          || (attr_type.getTypeID() == Type::kLong)) {
        // Some older versions of GCC will raise an error here if we don't
        // manually cast NULL to the appropriate type.
        maximum_integers_.insert(pair<attribute_id, const TypeInstance*>(attr_num,
                                                                         static_cast<const TypeInstance*>(NULL)));
      }
    }
  }
}

bool CompressedBlockBuilder::addTuple(const Tuple &tuple,
                                      const bool coerce_types) {
  ScopedPtr<Tuple> candidate_tuple;
  if (coerce_types) {
    candidate_tuple.reset(tuple.cloneAsInstanceOfRelation(relation_));
  } else {
    candidate_tuple.reset(tuple.clone());
  }

  DEBUG_ASSERT(candidate_tuple->size() == relation_.size());

  // Modify dictionaries and maximum integers to reflect the new tuple's
  // values. Keep track of what has changed in case a rollback is needed.
  vector<CompressionDictionaryBuilder*> modified_dictionaries;
  CompatUnorderedMap<attribute_id, const TypeInstance*>::unordered_map previous_maximum_integers;

  CatalogRelation::const_iterator attr_it = relation_.begin();
  Tuple::const_iterator value_it = candidate_tuple->begin();
  while (attr_it != relation_.end()) {
    attribute_id attr_id = attr_it->getID();

    PtrMap<attribute_id, CompressionDictionaryBuilder>::iterator
        dictionary_it = dictionary_builders_.find(attr_id);
    if (dictionary_it != dictionary_builders_.end()) {
      if (dictionary_it->second->insertEntryByReference(*value_it)) {
        modified_dictionaries.push_back(dictionary_it->second);
      }
    }

    CompatUnorderedMap<attribute_id, const TypeInstance*>::unordered_map::iterator
        max_int_it = maximum_integers_.find(attr_id);
    if (max_int_it != maximum_integers_.end()) {
      // If the new value less than zero, we can't truncate it.
      bool value_is_negative;
      switch (attr_it->getType().getTypeID()) {
        case Type::kInt:
          value_is_negative = value_it->numericGetIntValue() < 0;
          break;
        case Type::kLong:
          value_is_negative = value_it->numericGetLongValue() < 0;
          break;
        default:
          FATAL_ERROR("Non-integer type encountered in the maximum_integers_ map "
                      "of a CompressedBlockBuilder.");
      }

      if (value_is_negative) {
        previous_maximum_integers.insert(pair<attribute_id, const TypeInstance*>(attr_id, max_int_it->second));
        // Stop counting maximum, since we can no longer compress by truncating.
        maximum_integers_.erase(max_int_it);
      } else if (max_int_it->second == NULL) {
        // If there was no maximum before, then this is the first value and
        // automatically becomes the maximum.
        //
        // Some older versions of GCC will raise an error here if we don't
        // manually cast NULL to the appropriate type.
        previous_maximum_integers.insert(pair<attribute_id, const TypeInstance*>(
            attr_id,
            static_cast<const TypeInstance*>(NULL)));
        max_int_it->second = &(*value_it);
      } else {
        // Compare with the previous maximum and update as necessary.
        bool new_maximum;
        switch (attr_it->getType().getTypeID()) {
          case Type::kInt:
            new_maximum = max_int_it->second->numericGetIntValue() < value_it->numericGetIntValue();
            break;
          case Type::kLong:
            new_maximum = max_int_it->second->numericGetLongValue() < value_it->numericGetLongValue();
            break;
          default:
            FATAL_ERROR("Non-integer type encountered in the maximum_integers_ map "
                        "of a CompressedBlockBuilder.");
        }

        if (new_maximum) {
          previous_maximum_integers.insert(pair<attribute_id, const TypeInstance*>(attr_id, max_int_it->second));
          max_int_it->second = &(*value_it);
        }
      }
    }

    ++attr_it;
    ++value_it;
  }

  if (computeRequiredStorage(tuples_.size() + 1) > block_size_) {
    rollbackLastInsert(modified_dictionaries, previous_maximum_integers);
    return false;
  } else {
    tuples_.push_back(candidate_tuple.release());
    return true;
  }
}

void CompressedBlockBuilder::buildCompressedPackedRowStoreTupleStorageSubBlock(void *sub_block_memory) {
  DEBUG_ASSERT(computeRequiredStorage(tuples_.size()) <= block_size_);

  char *data_ptr = static_cast<char*>(sub_block_memory)
                   + buildTupleStorageSubBlockHeader(sub_block_memory);

  PtrMap<attribute_id, CompressionDictionary> dictionaries;
  buildDictionaryMap(sub_block_memory, &dictionaries);

  for (PtrVector<Tuple>::const_iterator tuple_it = tuples_.begin();
       tuple_it != tuples_.end();
       ++tuple_it) {
    for (CatalogRelation::const_iterator attr_it = relation_.begin();
         attr_it != relation_.end();
         ++attr_it) {
      if (compression_info_.dictionary_size(attr_it->getID()) > 0) {
        // Attribute is dictionary-compressed.
        PtrMap<attribute_id, CompressionDictionary>::const_iterator
            dictionary_it = dictionaries.find(attr_it->getID());
        DEBUG_ASSERT(dictionary_it != dictionaries.end());
        switch (compression_info_.attribute_size(attr_it->getID())) {
          case 1:
            *reinterpret_cast<uint8_t*>(data_ptr)
                = dictionary_it->second->getCodeForTypedValue(tuple_it->getAttributeValue(attr_it->getID()));
            break;
          case 2:
            *reinterpret_cast<uint16_t*>(data_ptr)
                = dictionary_it->second->getCodeForTypedValue(tuple_it->getAttributeValue(attr_it->getID()));
            break;
          case 4:
            *reinterpret_cast<uint32_t*>(data_ptr)
                = dictionary_it->second->getCodeForTypedValue(tuple_it->getAttributeValue(attr_it->getID()));
            break;
          default:
            FATAL_ERROR("Dictionary-compressed type had non power-of-two length in "
                        "CompressedBlockBuilder::buildCompressedPackedRowStoreTupleStorageSubBlock()");
        }
      } else if (compression_info_.attribute_size(attr_it->getID())
                 != attr_it->getType().maximumByteLength()) {
        // Attribute is compressed by truncation.
        switch (compression_info_.attribute_size(attr_it->getID())) {
          case 1:
            *reinterpret_cast<uint8_t*>(data_ptr)
                = tuple_it->getAttributeValue(attr_it->getID()).numericGetIntValue();
            break;
          case 2:
            *reinterpret_cast<uint16_t*>(data_ptr)
                = tuple_it->getAttributeValue(attr_it->getID()).numericGetIntValue();
            break;
          case 4:
            *reinterpret_cast<uint32_t*>(data_ptr)
                = tuple_it->getAttributeValue(attr_it->getID()).numericGetLongValue();
            break;
          default:
            FATAL_ERROR("Truncation-compressed type had non power-of-two length in "
                        "CompressedBlockBuilder::buildCompressedPackedRowStoreupleStorageSubBlock()");
        }
      } else {
        // Attribute is uncompressed.
        tuple_it->getAttributeValue(attr_it->getID()).copyInto(data_ptr);
      }
      data_ptr += compression_info_.attribute_size(attr_it->getID());
    }
  }
}

void CompressedBlockBuilder::buildCompressedColumnStoreTupleStorageSubBlock(void *sub_block_memory) {
  DEBUG_ASSERT(computeRequiredStorage(tuples_.size()) <= block_size_);

  // Sort tuples according to values of the sort attribute.
  const Type &sort_attribute_type = relation_.getAttributeById(sort_attribute_id_).getType();
  ScopedPtr<UncheckedComparator> sort_attribute_comp(
      Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(sort_attribute_type,
                                                                                   sort_attribute_type));
  sort(tuples_.getInternalVectorMutable()->begin(),
       tuples_.getInternalVectorMutable()->end(),
       TupleComparator(sort_attribute_id_, *sort_attribute_comp));

  char *current_stripe = static_cast<char*>(sub_block_memory)
                         + buildTupleStorageSubBlockHeader(sub_block_memory);

  PtrMap<attribute_id, CompressionDictionary> dictionaries;
  buildDictionaryMap(sub_block_memory, &dictionaries);

  size_t tuple_size = 0;
  size_t total_dictionary_size = 0;
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    tuple_size += compression_info_.attribute_size(attr_it->getID());
    total_dictionary_size += compression_info_.dictionary_size(attr_it->getID());
  }
  DEBUG_ASSERT(tuple_size > 0);
  size_t max_tuples = (block_size_
                      - (sizeof(tuple_id) + sizeof(int) + compression_info_.ByteSize() + total_dictionary_size))
                      / tuple_size;

  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    if (compression_info_.dictionary_size(attr_it->getID()) > 0) {
      // Attribute is dictionary-compressed.
      PtrMap<attribute_id, CompressionDictionary>::const_iterator
          dictionary_it = dictionaries.find(attr_it->getID());
      DEBUG_ASSERT(dictionary_it != dictionaries.end());
      buildDictionaryCompressedColumnStripe(attr_it->getID(),
                                            *(dictionary_it->second),
                                            current_stripe);
    } else if (compression_info_.attribute_size(attr_it->getID())
               != attr_it->getType().maximumByteLength()) {
      // Attribute is truncation-compressed.
      buildTruncationCompressedColumnStripe(attr_it->getID(),
                                            current_stripe);
    } else {
      // Attribute is uncompressed.
      buildUncompressedColumnStripe(attr_it->getID(),
                                    current_stripe);
    }

    current_stripe += max_tuples * compression_info_.attribute_size(attr_it->getID());
  }
}

std::size_t CompressedBlockBuilder::computeRequiredStorage(const std::size_t num_tuples) const {
  // Start with the size of the header.
  size_t required_storage = compression_info_.ByteSize() + sizeof(int) + sizeof(tuple_id);

  // Add required storage attribute-by-attribute.
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    PtrMap<attribute_id, CompressionDictionaryBuilder>::const_iterator
        dictionary_it = dictionary_builders_.find(attr_it->getID());
    if (dictionary_it == dictionary_builders_.end()) {
      // This attribute is not compressed.
      required_storage += num_tuples * attr_it->getType().maximumByteLength();
    } else if (attr_it->getType().isVariableLength()) {
      // Variable-length types MUST use dictionary compression.
      required_storage += dictionary_it->second->dictionarySizeBytes()
                          + num_tuples * dictionary_it->second->codeLengthPaddedBytes();
    } else {
      // Calculate the number of bytes needed to store all values when
      // truncating (if possible) or just storing values uncompressed.
      size_t truncated_bytes = num_tuples * computeTruncatedByteLengthForAttribute(attr_it->getID());
      // Calculate the total number of bytes (including storage for the
      // dictionary itself) needed to store all values with dictionary
      // compression.
      size_t dictionary_bytes = dictionary_it->second->dictionarySizeBytes()
                                + num_tuples * dictionary_it->second->codeLengthPaddedBytes();
      // Choose the method that uses space most efficiently.
      if (truncated_bytes < dictionary_bytes) {
        required_storage += truncated_bytes;
      } else {
        required_storage += dictionary_bytes;
      }
    }
  }

  return required_storage;
}

std::size_t CompressedBlockBuilder::computeTruncatedByteLengthForAttribute(
    const attribute_id attr_id) const {
  DEBUG_ASSERT(relation_.hasAttributeWithId(attr_id));

  size_t truncated_bytes = relation_.getAttributeById(attr_id).getType().maximumByteLength();
  CompatUnorderedMap<attribute_id, const TypeInstance*>::unordered_map::const_iterator
      max_int_it = maximum_integers_.find(attr_id);
  if ((max_int_it != maximum_integers_.end() && (max_int_it->second != NULL))) {
    unsigned int leading_zero_bits;
    switch (max_int_it->second->getType().getTypeID()) {
      case Type::kInt:
        DEBUG_ASSERT(max_int_it->second->numericGetIntValue() >= 0);
        if (max_int_it->second->numericGetIntValue()) {
          leading_zero_bits = leading_zero_count_32(
              static_cast<uint32_t>(max_int_it->second->numericGetIntValue()));
        } else {
          leading_zero_bits = 32;
        }
        break;
      case Type::kLong:
        DEBUG_ASSERT(max_int_it->second->numericGetLongValue() >= 0);
        if (max_int_it->second->numericGetLongValue()) {
          // Due to a quirk in predicate evaluation on truncated values,
          // we shouldn't store UINT32_MAX truncated.
          if (max_int_it->second->numericGetLongValue() == numeric_limits<uint32_t>::max()) {
            return truncated_bytes;
          }
          leading_zero_bits = leading_zero_count_64(
              static_cast<uint64_t>(max_int_it->second->numericGetLongValue()));
        } else {
          leading_zero_bits = 64;
        }
        break;
      default:
        FATAL_ERROR("Non-integer type encountered in the maximum_integers_ map "
                    "of a CompressedBlockBuilder.");
    }

    unsigned int needed_bits = (truncated_bytes << 3) - leading_zero_bits;
    if (needed_bits < 9) {
      truncated_bytes = 1;
    } else if (needed_bits < 17) {
      truncated_bytes = 2;
    } else if (needed_bits < 33) {
      truncated_bytes = 4;
    }
  }

  return truncated_bytes;
}

void CompressedBlockBuilder::rollbackLastInsert(
    const std::vector<CompressionDictionaryBuilder*> &modified_dictionaries,
    const CompatUnorderedMap<attribute_id, const TypeInstance*>::unordered_map &previous_maximum_integers) {
  for (vector<CompressionDictionaryBuilder*>::const_iterator dictionary_it = modified_dictionaries.begin();
       dictionary_it != modified_dictionaries.end();
       ++dictionary_it) {
    (*dictionary_it)->undoLastInsert();
  }

  for (CompatUnorderedMap<attribute_id, const TypeInstance*>::unordered_map::const_iterator
           previous_max_it = previous_maximum_integers.begin();
       previous_max_it != previous_maximum_integers.end();
       ++previous_max_it) {
    maximum_integers_[previous_max_it->first] = previous_max_it->second;
  }
}

std::size_t CompressedBlockBuilder::buildTupleStorageSubBlockHeader(void *sub_block_memory) {
  // Build up the CompressedBlockInfo.
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    PtrMap<attribute_id, CompressionDictionaryBuilder>::const_iterator
        dictionary_it = dictionary_builders_.find(attr_it->getID());
    if (dictionary_it == dictionary_builders_.end()) {
      // This attribute is not compressed.
      compression_info_.set_attribute_size(attr_it->getID(),
                                           attr_it->getType().maximumByteLength());
      compression_info_.set_dictionary_size(attr_it->getID(), 0);
    } else if (attr_it->getType().isVariableLength()) {
      // Variable-length types MUST use dictionary compression.
      compression_info_.set_attribute_size(attr_it->getID(),
                                           dictionary_it->second->codeLengthPaddedBytes());
      compression_info_.set_dictionary_size(attr_it->getID(),
                                            dictionary_it->second->dictionarySizeBytes());
    } else {
      // Calculate the number of bytes needed to store all values when
      // truncating (if possible) or just storing values uncompressed.
      size_t truncated_bytes = tuples_.size() * computeTruncatedByteLengthForAttribute(attr_it->getID());
      // Calculate the total number of bytes (including storage for the
      // dictionary itself) needed to store all values with dictionary
      // compression.
      size_t dictionary_bytes = dictionary_it->second->dictionarySizeBytes()
                                + tuples_.size() * dictionary_it->second->codeLengthPaddedBytes();
      // Choose the method that uses space most efficiently.
      if (truncated_bytes < dictionary_bytes) {
        compression_info_.set_attribute_size(attr_it->getID(),
                                             computeTruncatedByteLengthForAttribute(attr_it->getID()));
        compression_info_.set_dictionary_size(attr_it->getID(), 0);
      } else {
        compression_info_.set_attribute_size(attr_it->getID(),
                                             dictionary_it->second->codeLengthPaddedBytes());
        compression_info_.set_dictionary_size(attr_it->getID(),
                                              dictionary_it->second->dictionarySizeBytes());
      }
    }
  }

  // Record the number of tuples.
  *static_cast<tuple_id*>(sub_block_memory) = tuples_.size();

  // Serialize the compression info.
  *reinterpret_cast<int*>(static_cast<char*>(sub_block_memory) + sizeof(tuple_id))
      = compression_info_.ByteSize();
  if (!compression_info_.SerializeToArray(static_cast<char*>(sub_block_memory) + sizeof(tuple_id) + sizeof(int),
                                          compression_info_.ByteSize())) {
    FATAL_ERROR("Failed to do binary serialization of CompressedBlockInfo in "
                "CompressedBlockBuilder::buildTupleStorageSubBlockHeader");
  }

  // Build the physical dictionaries.
  size_t memory_offset = sizeof(tuple_id) + sizeof(int) + compression_info_.ByteSize();
  for (attribute_id attr_id = 0;
       attr_id <= relation_.getMaxAttributeId();
       ++attr_id) {
    if (compression_info_.dictionary_size(attr_id) > 0) {
      dictionary_builders_.find(attr_id)->second->buildDictionary(
          static_cast<char*>(sub_block_memory) + memory_offset);
      memory_offset += compression_info_.dictionary_size(attr_id);
    }
  }

  return memory_offset;
}

void CompressedBlockBuilder::buildDictionaryMap(
    const void *sub_block_memory,
    PtrMap<attribute_id, CompressionDictionary> *dictionary_map) const {
  const char *dictionary_memory = static_cast<const char*>(sub_block_memory)
                                  + sizeof(int) + sizeof(tuple_id) + compression_info_.ByteSize();
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    if (compression_info_.dictionary_size(attr_it->getID()) > 0) {
      if (attr_it->getType().isVariableLength()) {
        dictionary_map->insert(
            attr_it->getID(),
            new VariableLengthTypeCompressionDictionary(attr_it->getType(),
                                                        dictionary_memory,
                                                        compression_info_.dictionary_size(attr_it->getID())));
      } else {
        dictionary_map->insert(
            attr_it->getID(),
            new FixedLengthTypeCompressionDictionary(attr_it->getType(),
                                                     dictionary_memory,
                                                     compression_info_.dictionary_size(attr_it->getID())));
      }

      dictionary_memory += compression_info_.dictionary_size(attr_it->getID());
    }
  }
}

void CompressedBlockBuilder::buildDictionaryCompressedColumnStripe(
    const attribute_id attr_id,
    const CompressionDictionary &dictionary,
    void *stripe_location) const {
  switch (compression_info_.attribute_size(attr_id)) {
    case 1:
      for (size_t tuple_num = 0;
           tuple_num < tuples_.size();
           ++tuple_num) {
        reinterpret_cast<uint8_t*>(stripe_location)[tuple_num]
            = dictionary.getCodeForTypedValue(tuples_[tuple_num].getAttributeValue(attr_id));
      }
      break;
    case 2:
      for (size_t tuple_num = 0;
           tuple_num < tuples_.size();
           ++tuple_num) {
        reinterpret_cast<uint16_t*>(stripe_location)[tuple_num]
            = dictionary.getCodeForTypedValue(tuples_[tuple_num].getAttributeValue(attr_id));
      }
      break;
    case 4:
      for (size_t tuple_num = 0;
           tuple_num < tuples_.size();
           ++tuple_num) {
        reinterpret_cast<uint32_t*>(stripe_location)[tuple_num]
            = dictionary.getCodeForTypedValue(tuples_[tuple_num].getAttributeValue(attr_id));
      }
      break;
    default:
      FATAL_ERROR("Dictionary-compressed type had non power-of-two length in "
                   "CompressedBlockBuilder::buildDictionaryCompressedColumnStripe()");
  }
}

void CompressedBlockBuilder::buildTruncationCompressedColumnStripe(
    const attribute_id attr_id,
    void *stripe_location) const {
  switch (compression_info_.attribute_size(attr_id)) {
    case 1:
      for (size_t tuple_num = 0;
           tuple_num < tuples_.size();
           ++tuple_num) {
        reinterpret_cast<uint8_t*>(stripe_location)[tuple_num]
            = tuples_[tuple_num].getAttributeValue(attr_id).numericGetIntValue();
      }
      break;
    case 2:
      for (size_t tuple_num = 0;
           tuple_num < tuples_.size();
           ++tuple_num) {
        reinterpret_cast<uint16_t*>(stripe_location)[tuple_num]
            = tuples_[tuple_num].getAttributeValue(attr_id).numericGetIntValue();
      }
      break;
    case 4:
      for (size_t tuple_num = 0;
           tuple_num < tuples_.size();
           ++tuple_num) {
        reinterpret_cast<uint32_t*>(stripe_location)[tuple_num]
            = tuples_[tuple_num].getAttributeValue(attr_id).numericGetLongValue();
      }
      break;
    default:
      FATAL_ERROR("Truncation-compressed type had non power-of-two length in "
                   "CompressedBlockBuilder::buildTruncationCompressedColumnStripe()");
  }
}

void CompressedBlockBuilder::buildUncompressedColumnStripe(
    const attribute_id attr_id,
    void *stripe_location) const {
  char *value_location = static_cast<char*>(stripe_location);
  size_t value_length = compression_info_.attribute_size(attr_id);
  for (PtrVector<Tuple>::const_iterator tuple_it = tuples_.begin();
       tuple_it != tuples_.end();
       ++tuple_it) {
    tuple_it->getAttributeValue(attr_id).copyInto(value_location);
    value_location += value_length;
  }
}

}  // namespace quickstep
