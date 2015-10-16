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

#include "storage/CSBTreeIndexSubBlock.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <utility>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "expressions/ComparisonPredicate.hpp"
#include "expressions/Predicate.hpp"
#include "expressions/Scalar.hpp"
#include "storage/CompressedTupleStorageSubBlock.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageConstants.hpp"
#include "storage/StorageErrors.hpp"
#include "storage/TupleIdSequence.hpp"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/Comparison.hpp"
#include "types/CompressionDictionary.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/BitVector.hpp"
#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"
#include "utility/ScopedBuffer.hpp"
#include "utility/ScopedPtr.hpp"

using std::memcpy;
using std::memmove;
using std::pair;
using std::size_t;
using std::sort;
using std::uint8_t;
using std::uint16_t;
using std::uint32_t;
using std::vector;

namespace quickstep {

namespace csbtree_internal {

// Hack which provides the UncheckedComparator interface, but compares
// composite keys.
class CompositeKeyLessComparator : public UncheckedComparator {
 public:
  CompositeKeyLessComparator(const CSBTreeIndexSubBlock &owner, const CatalogRelation &relation)
      : owner_(owner) {
    attribute_comparators_.reserve(owner_.indexed_attribute_ids_.size());
    for (vector<attribute_id>::const_iterator it = owner_.indexed_attribute_ids_.begin();
         it != owner_.indexed_attribute_ids_.end();
         ++it) {
      const Type &attribute_type = relation.getAttributeById(*it).getType();
      DEBUG_ASSERT(!attribute_type.isVariableLength());
      attribute_comparators_.push_back(
          Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(attribute_type,
                                                                                       attribute_type));
    }
  }

  ~CompositeKeyLessComparator() {
  }

  bool compareTypeInstances(const TypeInstance &left, const TypeInstance &right) const {
    FATAL_ERROR("Can not use CompositeKeyLessComparator to compare TypeInstance.");
  }

  bool compareDataPtrs(const void *left, const void *right) const {
    DEBUG_ASSERT(attribute_comparators_.size() == owner_.indexed_attribute_offsets_.size());
    vector<size_t>::const_iterator offset_it = owner_.indexed_attribute_offsets_.begin();
    for (PtrVector<UncheckedComparator>::const_iterator comparator_it = attribute_comparators_.begin();
         comparator_it != attribute_comparators_.end();
         ++comparator_it, ++offset_it) {
      if (comparator_it->compareDataPtrs(static_cast<const char*>(left) + *offset_it,
                                         static_cast<const char*>(right) + *offset_it)) {
        return true;
      } else if (comparator_it->compareDataPtrs(static_cast<const char*>(right) + *offset_it,
                                                static_cast<const char*>(left) + *offset_it)) {
        return false;
      }
      // Attributes are equal, so loop procedes to compare next attribute in
      // composite key.
    }

    return false;  // Keys are exactly equal.
  }

  bool compareTypeInstanceWithDataPtr(const TypeInstance &left, const void *right) const {
    FATAL_ERROR("Can not use CompositeKeyLessComparator to compare TypeInstance.");
  }

  bool compareDataPtrWithTypeInstance(const void *left, const TypeInstance &right) const {
    FATAL_ERROR("Can not use CompositeKeyLessComparator to compare TypeInstance.");
  }

 private:
  const CSBTreeIndexSubBlock &owner_;
  PtrVector<UncheckedComparator> attribute_comparators_;
};

// Hack which provides the UncheckedComparator interface, but compares
// compressed codes.
template <typename CodeType>
class CompressedCodeLessComparator : public UncheckedComparator {
 public:
  bool compareTypeInstances(const TypeInstance &left, const TypeInstance &right) const {
    FATAL_ERROR("Can not use CompressedCodeLessComparator to compare TypeInstance.");
  }

  bool compareDataPtrs(const void *left, const void *right) const {
    return (*static_cast<const CodeType*>(left) < *static_cast<const CodeType*>(right));
  }

  bool compareTypeInstanceWithDataPtr(const TypeInstance &left, const void *right) const {
    FATAL_ERROR("Can not use CompressedCodeLessComparator to compare TypeInstance.");
  }

  bool compareDataPtrWithTypeInstance(const void *left, const TypeInstance &right) const {
    FATAL_ERROR("Can not use CompressedCodeLessComparator to compare TypeInstance.");
  }
};

class EntryReference {
 public:
  EntryReference(const void *key, const tuple_id tuple)
      : key_(key), tuple_(tuple) {
  }

  inline const void* getKeyPtr() const {
    return key_;
  }

  inline tuple_id getTupleID() const {
    return tuple_;
  }

 private:
  const void *key_;
  tuple_id tuple_;
};

class CompressedEntryReference {
 public:
  CompressedEntryReference(const uint32_t key_code, const tuple_id tuple)
      : key_code_(key_code), tuple_(tuple) {
  }

  inline const void* getKeyPtr() const {
    return &key_code_;
  }

  inline uint32_t getKeyCode() const {
    return key_code_;
  }

  inline tuple_id getTupleID() const {
    return tuple_;
  }

 private:
  uint32_t key_code_;
  tuple_id tuple_;
};

template <class EntryReferenceT>
class EntryReferenceComparator {
 public:
  explicit EntryReferenceComparator(const UncheckedComparator &comparator)
      : internal_comparator_(comparator) {
  }

  inline bool operator() (const EntryReferenceT &left, const EntryReferenceT &right) const {
    return internal_comparator_.compareDataPtrs(left.getKeyPtr(), right.getKeyPtr());
  }

 private:
  const UncheckedComparator &internal_comparator_;
};

// Faster specialization for the compressed case:
template <>
class EntryReferenceComparator<CompressedEntryReference> {
 public:
  explicit EntryReferenceComparator(const UncheckedComparator &comparator) {
  }

  inline bool operator() (const CompressedEntryReference &left, const CompressedEntryReference &right) const {
    return left.getKeyCode() < right.getKeyCode();
  }
};

}  // namespace csbtree_internal

const int CSBTreeIndexSubBlock::kNodeGroupNone = -1;
const int CSBTreeIndexSubBlock::kNodeGroupNextLeaf = -2;
const int CSBTreeIndexSubBlock::kNodeGroupFull = -3;

CSBTreeIndexSubBlock::CSBTreeIndexSubBlock(const TupleStorageSubBlock &tuple_store,
                                           const IndexSubBlockDescription &description,
                                           const bool new_block,
                                           void *sub_block_memory,
                                           const std::size_t sub_block_memory_size)
    : IndexSubBlock(tuple_store,
                    description,
                    new_block,
                    sub_block_memory,
                    sub_block_memory_size),
      initialized_(false),
      tuple_store_supports_untyped_ptr_(false),
      key_may_be_compressed_(false),
      key_is_compressed_(false),
      key_is_nullable_(false),
      next_free_node_group_(kNodeGroupNone),
      num_free_node_groups_(0) {
  if (!DescriptionIsValid(relation_, description_)) {
    FATAL_ERROR("Attempted to construct a CSBTreeIndexSubBlock from an invalid description.");
  }

  const int num_indexed_attributes
      = description_.ExtensionSize(CSBTreeIndexSubBlockDescription::indexed_attribute_id);
  if (num_indexed_attributes > 1) {
    key_is_composite_ = true;
  } else {
    key_is_composite_ = false;
  }

  indexed_attribute_ids_.reserve(num_indexed_attributes);
  indexed_attribute_offsets_.reserve(num_indexed_attributes);

  for (int indexed_attribute_num = 0;
       indexed_attribute_num < num_indexed_attributes;
       ++indexed_attribute_num) {
    attribute_id indexed_attribute_id
        = description_.GetExtension(CSBTreeIndexSubBlockDescription::indexed_attribute_id,
                                    indexed_attribute_num);
    indexed_attribute_ids_.push_back(indexed_attribute_id);

    // TODO(chasseur): Support a composite key with compressed parts.
    if ((!key_is_composite_) && tuple_store_.isCompressed()) {
      const CompressedTupleStorageSubBlock &compressed_tuple_store
          = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
      if (compressed_tuple_store.compressedBlockIsBuilt()) {
        if (compressed_tuple_store.compressedAttributeIsDictionaryCompressed(indexed_attribute_id)
            || compressed_tuple_store.compressedAttributeIsTruncationCompressed(indexed_attribute_id)) {
          key_may_be_compressed_ = true;
        }
      } else {
        if (compressed_tuple_store.compressedUnbuiltBlockAttributeMayBeCompressed(indexed_attribute_id)) {
          key_may_be_compressed_ = true;
        }
      }
    }
  }

  bool initialize_now = true;
  if (key_may_be_compressed_) {
    if (!static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_).compressedBlockIsBuilt()) {
      initialize_now = false;
    }
  }

  if (initialize_now) {
    if (new_block) {
      initialize(new_block);
    } else {
      if (!initialize(new_block)) {
        throw MalformedBlock();
      }
    }
  }
}

bool CSBTreeIndexSubBlock::DescriptionIsValid(const CatalogRelation &relation,
                                              const IndexSubBlockDescription &description) {
  // Make sure description is initialized and specifies CSBTree.
  if (!description.IsInitialized()) {
    return false;
  }
  if (description.sub_block_type() != IndexSubBlockDescription::CSB_TREE) {
    return false;
  }

  // Make sure at least one key attribute is specified.
  if (description.ExtensionSize(CSBTreeIndexSubBlockDescription::indexed_attribute_id) == 0) {
    return false;
  }

  // Check that all key attributes exist and are fixed-length.
  for (int indexed_attribute_num = 0;
       indexed_attribute_num < description.ExtensionSize(CSBTreeIndexSubBlockDescription::indexed_attribute_id);
       ++indexed_attribute_num) {
    attribute_id indexed_attribute_id
        = description.GetExtension(CSBTreeIndexSubBlockDescription::indexed_attribute_id,
                                   indexed_attribute_num);
    if (!relation.hasAttributeWithId(indexed_attribute_id)) {
      return false;
    }
    const Type &attr_type = relation.getAttributeById(indexed_attribute_id).getType();
    if (attr_type.isVariableLength()) {
      return false;
    }
  }

  return true;
}

// TODO(chasseur): Make this heuristic more accurate, particularly if keys may
// be compressed.
std::size_t CSBTreeIndexSubBlock::EstimateBytesPerTuple(
    const CatalogRelation &relation,
    const IndexSubBlockDescription &description) {
  DEBUG_ASSERT(DescriptionIsValid(relation, description));

  size_t key_length = 0;
  for (int indexed_attribute_num = 0;
       indexed_attribute_num < description.ExtensionSize(CSBTreeIndexSubBlockDescription::indexed_attribute_id);
       ++indexed_attribute_num) {
    key_length += relation.getAttributeById(
        description.GetExtension(CSBTreeIndexSubBlockDescription::indexed_attribute_id, indexed_attribute_num))
            .getType().maximumByteLength();
  }

  return (5 * key_length) >> 1;
}

bool CSBTreeIndexSubBlock::addEntry(const tuple_id tuple) {
  DEBUG_ASSERT(initialized_);
  DEBUG_ASSERT(tuple_store_.hasTupleWithID(tuple));

  void *root_node = getRootNode();
  NodeHeader super_root;
  super_root.num_keys = 0;
  super_root.is_leaf = false;
  super_root.node_group_reference = getRootNodeGroupNumber();

  InsertReturnValue retval;

  if (key_is_composite_) {
    ScopedBuffer composite_key_buffer(makeKeyCopy(tuple));
    if (key_is_nullable_) {
      if (composite_key_buffer.empty()) {
        // Don't insert a NULL key.
        return true;
      }
    } else {
      DEBUG_ASSERT(!composite_key_buffer.empty());
    }

    if (static_cast<NodeHeader*>(root_node)->is_leaf) {
      retval = leafInsertHelper(0,
                                tuple,
                                composite_key_buffer.get(),
                                &super_root,
                                root_node);
    } else {
      retval = internalInsertHelper(0,
                                    tuple,
                                    composite_key_buffer.get(),
                                    &super_root,
                                    root_node);
    }
  } else if (key_is_compressed_) {
    // Don't insert a NULL key.
    if (key_is_nullable_) {
      if (tuple_store_supports_untyped_ptr_) {
        if (tuple_store_.getAttributeValue(tuple, indexed_attribute_ids_.front()) == NULL) {
          return true;
        }
      } else {
        ScopedPtr<TypeInstance> typed_key(tuple_store_.getAttributeValueTyped(tuple,
                                                                              indexed_attribute_ids_.front()));
        if (typed_key->isNull()) {
          return true;
        }
      }
    }

    const CompressedTupleStorageSubBlock &compressed_tuple_store
        = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
    uint32_t code = compressed_tuple_store.compressedGetCode(tuple,
                                                             indexed_attribute_ids_.front());
    switch (compressed_tuple_store.compressedGetCompressedAttributeSize(indexed_attribute_ids_.front())) {
      case 1:
        retval = compressedKeyAddEntryHelper<uint8_t>(tuple, code, super_root, root_node);
        break;
      case 2:
        retval = compressedKeyAddEntryHelper<uint16_t>(tuple, code, super_root, root_node);
        break;
      case 4:
        retval = compressedKeyAddEntryHelper<uint32_t>(tuple, code, super_root, root_node);
        break;
      default:
        FATAL_ERROR("Unexpected compressed key byte-length (not 1, 2, or 4) encountered "
                    "in CSBTreeIndexSubBlock::addEntry()");
    }
  } else if (tuple_store_supports_untyped_ptr_) {
    const void *key_ptr = tuple_store_.getAttributeValue(tuple, indexed_attribute_ids_.front());
    if (key_is_nullable_) {
      if (key_ptr == NULL) {
        // Don't insert a NULL key.
        return true;
      }
    } else {
      DEBUG_ASSERT(key_ptr != NULL);
    }

    if (static_cast<NodeHeader*>(root_node)->is_leaf) {
      retval = leafInsertHelper(0,
                                tuple,
                                key_ptr,
                                &super_root,
                                root_node);
    } else {
      retval = internalInsertHelper(0,
                                    tuple,
                                    key_ptr,
                                    &super_root,
                                    root_node);
    }
  } else {
    ScopedPtr<TypeInstance> typed_key(tuple_store_.getAttributeValueTyped(tuple, indexed_attribute_ids_.front()));
    if (key_is_nullable_) {
      if (typed_key->isNull()) {
        // Don't insert a NULL key.
        return true;
      }
    } else {
      DEBUG_ASSERT(!typed_key->isNull());
    }

    if (static_cast<NodeHeader*>(root_node)->is_leaf) {
      retval = leafInsertHelper(0,
                                tuple,
                                typed_key->getDataPtr(),
                                &super_root,
                                root_node);
    } else {
      retval = internalInsertHelper(0,
                                    tuple,
                                    typed_key->getDataPtr(),
                                    &super_root,
                                    root_node);
    }
  }

  if (retval.new_node_group_id == kNodeGroupFull) {
    // Needed to split a node group, but not enough space.
    return false;
  }

  DEBUG_ASSERT(retval.new_node_group_id == kNodeGroupNone);
  if (retval.split_node_least_key != NULL) {
    // The root was split, must create a new root.
    // Allocate the new root.
    int new_root_group_id = allocateNodeGroup();
    DEBUG_ASSERT(new_root_group_id >= 0);
    void *new_root = getNode(new_root_group_id, 0);

    // Set up the new root's header.
    static_cast<NodeHeader*>(new_root)->num_keys = 1;
    static_cast<NodeHeader*>(new_root)->is_leaf = false;
    static_cast<NodeHeader*>(new_root)->node_group_reference = getRootNodeGroupNumber();

    // Insert the split key into the new root.
    memcpy(static_cast<char*>(new_root) + sizeof(NodeHeader),
           retval.split_node_least_key,
           key_length_bytes_);

    // Update the root node group number.
    setRootNodeGroupNumber(new_root_group_id);
  }

  return true;
}

void CSBTreeIndexSubBlock::removeEntry(const tuple_id tuple) {
  DEBUG_ASSERT(initialized_);
  if (key_is_composite_) {
    ScopedBuffer composite_key_buffer(makeKeyCopy(tuple));
    if (key_is_nullable_) {
      if (composite_key_buffer.empty()) {
        // Don't remove a NULL key (it would not have been inserted in the
        // first place).
        return;
      }
    } else {
      DEBUG_ASSERT(!composite_key_buffer.empty());
    }

    removeEntryFromLeaf(tuple,
                        composite_key_buffer.get(),
                        findLeaf(getRootNode(), composite_key_buffer.get()));
  } else if (key_is_compressed_) {
    // Don't remove a NULL key (it would not have been inserted in the first
    // place).
    if (key_is_nullable_) {
      if (tuple_store_supports_untyped_ptr_) {
        if (tuple_store_.getAttributeValue(tuple, indexed_attribute_ids_.front()) == NULL) {
          return;
        }
      } else {
        ScopedPtr<TypeInstance> typed_key(tuple_store_.getAttributeValueTyped(tuple,
                                                                              indexed_attribute_ids_.front()));
        if (typed_key->isNull()) {
          return;
        }
      }
    }

    const CompressedTupleStorageSubBlock &compressed_tuple_store
        = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
    uint32_t code = compressed_tuple_store.compressedGetCode(tuple,
                                                             indexed_attribute_ids_.front());
    switch (compressed_tuple_store.compressedGetCompressedAttributeSize(indexed_attribute_ids_.front())) {
      case 1:
        compressedKeyRemoveEntryHelper<uint8_t>(tuple, code);
        break;
      case 2:
        compressedKeyRemoveEntryHelper<uint16_t>(tuple, code);
        break;
      case 4:
        compressedKeyRemoveEntryHelper<uint32_t>(tuple, code);
        break;
      default:
        FATAL_ERROR("Unexpected compressed key byte-length (not 1, 2, or 4) encountered "
                    "in CSBTreeIndexSubBlock::removeEntry()");
    }
  } else if (tuple_store_supports_untyped_ptr_) {
    const void *key_ptr = tuple_store_.getAttributeValue(tuple, indexed_attribute_ids_.front());
    if (key_is_nullable_) {
      if (key_ptr == NULL) {
        // Don't remove a NULL key (it would not have been inserted in the
        // first place).
        return;
      }
    } else {
      DEBUG_ASSERT(key_ptr != NULL);
    }

    removeEntryFromLeaf(tuple,
                        key_ptr,
                        findLeaf(getRootNode(), key_ptr));
  } else {
    ScopedPtr<TypeInstance> typed_key(
        tuple_store_.getAttributeValueTyped(tuple, indexed_attribute_ids_.front()));
    if (key_is_nullable_) {
      if (typed_key->isNull()) {
        // Don't remove a NULL key (it would not have been inserted in the
        // first place).
        return;
      }
    } else {
      DEBUG_ASSERT(!typed_key->isNull());
    }

    removeEntryFromLeaf(tuple,
                        typed_key->getDataPtr(),
                        findLeaf(getRootNode(), typed_key->getDataPtr()));
  }
}

IndexSearchResult CSBTreeIndexSubBlock::getMatchesForPredicate(const Predicate &predicate) const {
  DEBUG_ASSERT(initialized_);
  if (key_is_composite_) {
    // TODO(chasseur): Evaluate predicates on composite keys.
    FATAL_ERROR("CSBTreeIndexSubBlock::getMatchesForPredicate() is unimplemented for composite keys.");
  }

  if (!predicate.isAttributeLiteralComparisonPredicate()) {
    FATAL_ERROR("CSBTreeIndexSubBlock::getMatchesForPredicate() can not "
                "evaluate predicates other than simple comparisons.");
  }

  const ComparisonPredicate &comparison_predicate = static_cast<const ComparisonPredicate&>(predicate);

  const CatalogAttribute *comparison_attribute = NULL;
  bool left_literal = false;
  if (comparison_predicate.getLeftOperand().hasStaticValue()) {
    DEBUG_ASSERT(comparison_predicate.getRightOperand().getDataSource() == Scalar::kAttribute);
    comparison_attribute
        = &(static_cast<const ScalarAttribute&>(comparison_predicate.getRightOperand()).getAttribute());
    left_literal = true;
  } else {
    DEBUG_ASSERT(comparison_predicate.getLeftOperand().getDataSource() == Scalar::kAttribute);
    comparison_attribute
        = &(static_cast<const ScalarAttribute&>(comparison_predicate.getLeftOperand()).getAttribute());
    left_literal = false;
  }

  if (comparison_attribute->getID() != indexed_attribute_ids_.front()) {
    FATAL_ERROR("CSBTreeIndexSubBlock::getMatchesForPredicate() can not "
                "evaluate predicates on non-indexed attributes.");
  }

  const LiteralTypeInstance* comparison_literal;
  if (left_literal) {
    comparison_literal = &(comparison_predicate.getLeftOperand().getStaticValue());
  } else {
    comparison_literal = &(comparison_predicate.getRightOperand().getStaticValue());
  }

  IndexSearchResult result;
  result.is_superset = false;

  if (comparison_literal->isNull()) {
    result.sequence = new TupleIdSequence();
    return result;
  }

  // If the literal is on the left, flip the comparison around.
  Comparison::ComparisonID comp = comparison_predicate.getComparison().getComparisonID();
  if (left_literal) {
    switch (comp) {
      case Comparison::kLess:
        comp = Comparison::kGreater;
        break;
      case Comparison::kLessOrEqual:
        comp = Comparison::kGreaterOrEqual;
        break;
      case Comparison::kGreater:
        comp = Comparison::kLess;
        break;
      case Comparison::kGreaterOrEqual:
        comp = Comparison::kLessOrEqual;
        break;
      default:
        break;
    }
  }

  if (key_is_compressed_) {
    result.sequence = evaluateComparisonPredicateOnCompressedKey(comp, *comparison_literal);
  } else {
    result.sequence = evaluateComparisonPredicateOnUncompressedKey(comp, *comparison_literal);
  }
  return result;
}

bool CSBTreeIndexSubBlock::rebuild() {
  if (!initialized_) {
    if (!initialize(false)) {
      return false;
    }
  }

  clearIndex();
  if (tuple_store_.isEmpty()) {
    return true;
  }
  if (!rebuildSpaceCheck()) {
    return false;
  }

  vector<int> node_groups_this_level;
  // Rebuild leaves.
  uint16_t nodes_in_last_group = rebuildLeaves(&node_groups_this_level);
  // Keep building intermediate levels from the bottom up until there is a
  // single root node.
  while (!((node_groups_this_level.size() == 1) && (nodes_in_last_group == 1))) {
    vector<int> node_groups_next_level;
    nodes_in_last_group = rebuildInternalLevel(node_groups_this_level,
                                               nodes_in_last_group,
                                               &node_groups_next_level);
    node_groups_this_level.swap(node_groups_next_level);
  }

  // Set the root number.
  setRootNodeGroupNumber(node_groups_this_level.front());
  return true;
}

bool CSBTreeIndexSubBlock::initialize(const bool new_block) {
  if (key_may_be_compressed_) {
    const CompressedTupleStorageSubBlock &compressed_tuple_store
        = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
    if (!compressed_tuple_store.compressedBlockIsBuilt()) {
      FATAL_ERROR("CSBTreeIndexSubBlock::initialize() called with a key which "
                  "may be compressed before the associated TupleStorageSubBlock "
                  "was built.");
    }

    if (compressed_tuple_store.compressedAttributeIsDictionaryCompressed(indexed_attribute_ids_.front())
        || compressed_tuple_store.compressedAttributeIsTruncationCompressed(indexed_attribute_ids_.front())) {
      key_is_compressed_ = true;
    }
  }

  // Compute the number of bytes needed to store a key, and fill in the vector
  // of indexed attribute offsets.
  key_length_bytes_ = 0;
  for (vector<attribute_id>::const_iterator indexed_attr_it = indexed_attribute_ids_.begin();
       indexed_attr_it != indexed_attribute_ids_.end();
       ++indexed_attr_it) {
    indexed_attribute_offsets_.push_back(key_length_bytes_);

    if ((!key_is_composite_)
        && tuple_store_.supportsUntypedGetAttributeValue(*indexed_attr_it)) {
      tuple_store_supports_untyped_ptr_ = true;
    }

    const Type &attr_type = relation_.getAttributeById(*indexed_attr_it).getType();
    if (attr_type.isNullable()) {
      key_is_nullable_ = true;
    }
    if (key_is_compressed_) {
      key_length_bytes_ += static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_)
                           .compressedGetCompressedAttributeSize(*indexed_attr_it);
    } else {
      key_length_bytes_ += attr_type.maximumByteLength();
    }
  }
  DEBUG_ASSERT(key_length_bytes_ > 0);
  key_tuple_id_pair_length_bytes_ = key_length_bytes_ + sizeof(tuple_id);

  // Compute the number of keys that can be stored in internal and leaf nodes.
  // Internal nodes are just a header and a list of keys.
  max_keys_internal_ = (kCSBTreeNodeSizeBytes - sizeof(NodeHeader)) / key_length_bytes_;
  // Leaf nodes are a header, plus a list of (key, tuple_id) pairs.
  max_keys_leaf_ = (kCSBTreeNodeSizeBytes - sizeof(NodeHeader)) / key_tuple_id_pair_length_bytes_;
  if ((max_keys_internal_ < 2) || (max_keys_leaf_ < 2)) {
    if (new_block) {
      throw CSBTreeKeyTooLarge();
    } else {
      return false;
    }
  }
  // The number of child nodes allocated to each half of a split internal node.
  small_half_num_children_ = (max_keys_internal_ + 1) >> 1;
  large_half_num_children_ = small_half_num_children_ + ((max_keys_internal_ + 1) & 0x1);

  small_half_num_keys_leaf_ = max_keys_leaf_ >> 1;
  large_half_num_keys_leaf_ = (max_keys_leaf_ >> 1) + (max_keys_leaf_ & 0x1);

  // Create the less-than comparator for this index's key.
  if (key_is_composite_) {
    key_comparator_.reset(new csbtree_internal::CompositeKeyLessComparator(*this, relation_));
  } else if (key_is_compressed_) {
    switch (static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_)
            .compressedGetCompressedAttributeSize(indexed_attribute_ids_.front())) {
      case 1:
        key_comparator_.reset(new csbtree_internal::CompressedCodeLessComparator<uint8_t>());
        break;
      case 2:
        key_comparator_.reset(new csbtree_internal::CompressedCodeLessComparator<uint16_t>());
        break;
      case 4:
        key_comparator_.reset(new csbtree_internal::CompressedCodeLessComparator<uint32_t>());
        break;
      default:
        FATAL_ERROR("Unexpected compressed key byte-length (not 1, 2, or 4) encountered "
                    "in CSBTreeIndexSubBlock::initialize()");
    }
  } else {
    const Type &attr_type = relation_.getAttributeById(indexed_attribute_ids_.front()).getType();
    key_comparator_.reset(Comparison::GetComparison(Comparison::kLess)
        .makeUncheckedComparatorForTypes(attr_type, attr_type));
  }

  node_group_size_bytes_ = kCSBTreeNodeSizeBytes * (max_keys_internal_ + 1);

  // Perform this computation on the order of bits.
  size_t num_node_groups = ((sub_block_memory_size_ - sizeof(int)) << 3)
                           / ((node_group_size_bytes_ << 3) + 1);

  // Compute the number of bytes needed for this SubBlock's header. The header
  // consists of the root node's node group number and a bitmap of free/used
  // node groups.
  size_t header_size_bytes = sizeof(int) + BitVector::BytesNeeded(num_node_groups);

  // Node groups start after the header, and should be aligned to
  // kCSBTreeNodeSizeBytes (i.e. cache lines). In some circumstances, the
  // alignment requirement forces us to use one less node group than would
  // otherwise be possible.
  node_groups_start_ = static_cast<char*>(sub_block_memory_) + header_size_bytes;
  if (reinterpret_cast<size_t>(node_groups_start_) & (kCSBTreeNodeSizeBytes - 1)) {
    node_groups_start_ = static_cast<char*>(node_groups_start_)
                         + kCSBTreeNodeSizeBytes
                         - (reinterpret_cast<size_t>(node_groups_start_) & (kCSBTreeNodeSizeBytes - 1));
  }

  // Adjust num_node_groups as necessary for aligned nodes.
  num_node_groups = (static_cast<char*>(sub_block_memory_) + sub_block_memory_size_
                    - static_cast<char*>(node_groups_start_)) / node_group_size_bytes_;
  if (num_node_groups == 0) {
    throw BlockMemoryTooSmall("CSBTreeIndex", sub_block_memory_size_);
  }

  // Set up the free/used node group bitmap and the free list.
  node_group_used_bitmap_.reset(new BitVector(static_cast<char*>(sub_block_memory_) + sizeof(int),
                                              num_node_groups));
  initialized_ = true;

  if (new_block) {
    clearIndex();
  } else {
    num_free_node_groups_ = num_node_groups - node_group_used_bitmap_->onesCount();
    if (num_free_node_groups_ > 0) {
      next_free_node_group_ = node_group_used_bitmap_->firstZero();
      DEBUG_ASSERT(static_cast<size_t>(next_free_node_group_) < node_group_used_bitmap_->size());
    }
  }

  return true;
}

void CSBTreeIndexSubBlock::clearIndex() {
  // Reset the free node group bitmap.
  DEBUG_ASSERT(node_group_used_bitmap_->size() > 0);
  node_group_used_bitmap_->clear();
  next_free_node_group_ = 0;
  num_free_node_groups_ = node_group_used_bitmap_->size();

  // Allocate the root node.
  setRootNodeGroupNumber(allocateNodeGroup());
  DEBUG_ASSERT(getRootNodeGroupNumber() >= 0);

  // Initialize the root node as an empty leaf node.
  NodeHeader *root_header = static_cast<NodeHeader*>(getRootNode());
  root_header->num_keys = 0;
  root_header->is_leaf = true;
  root_header->node_group_reference = kNodeGroupNone;
}

void* CSBTreeIndexSubBlock::makeKeyCopy(const tuple_id tuple) const {
  DEBUG_ASSERT(tuple_store_.hasTupleWithID(tuple));
  DEBUG_ASSERT(indexed_attribute_ids_.size() == indexed_attribute_offsets_.size());

  ScopedBuffer key_copy(key_length_bytes_);

  vector<attribute_id>::const_iterator attr_it = indexed_attribute_ids_.begin();
  for (vector<size_t>::const_iterator offset_it = indexed_attribute_offsets_.begin();
       offset_it != indexed_attribute_offsets_.end();
       ++attr_it, ++offset_it) {
    ScopedPtr<TypeInstance> attr_value(tuple_store_.getAttributeValueTyped(tuple, *attr_it));
    if (attr_value->isNull()) {
      return NULL;
    }
    attr_value->copyInto(static_cast<char*>(key_copy.get()) + *offset_it);
  }

  return key_copy.release();
}

const void* CSBTreeIndexSubBlock::getLeastKey(const void *node) const {
  if (static_cast<const NodeHeader*>(node)->is_leaf) {
    if (static_cast<const NodeHeader*>(node)->num_keys) {
      return static_cast<const char*>(node) + sizeof(NodeHeader);
    } else {
      return NULL;
    }
  } else {
    DEBUG_ASSERT(static_cast<const NodeHeader*>(node)->num_keys);
    const void *least_key = getLeastKey(getNode(static_cast<const NodeHeader*>(node)->node_group_reference, 0));
    if (least_key == NULL) {
      // If the leftmost child leaf was empty, can just use the first key here.
      return static_cast<const char*>(node) + sizeof(NodeHeader);
    }
    return least_key;
  }
}

void* CSBTreeIndexSubBlock::findLeaf(const void *node, const void *key) const {
  const NodeHeader *node_header = static_cast<const NodeHeader*>(node);
  if (node_header->is_leaf) {
    return const_cast<void*>(node);
  }
  for (uint16_t key_num = 0;
       key_num < node_header->num_keys;
       ++key_num) {
    if (key_comparator_->compareDataPtrs(key,
                                         static_cast<const char*>(node)
                                             + sizeof(NodeHeader)
                                             + key_num * key_length_bytes_)) {
      return findLeaf(getNode(node_header->node_group_reference, key_num), key);
    } else if (!key_comparator_->compareDataPtrs(static_cast<const char*>(node)
                                                     + sizeof(NodeHeader)
                                                     + key_num * key_length_bytes_,
                                                 key)) {
      // Handle the special case where duplicate keys are spread across
      // multiple nodes.

      // NOTE(chasseur): If duplicate keys are not allowed, this branch is
      // not necessary, and searches could be done slightly more efficiently.
      return findLeaf(getNode(node_header->node_group_reference, key_num), key);
    }
  }
  return findLeaf(getNode(node_header->node_group_reference, node_header->num_keys), key);
}

void* CSBTreeIndexSubBlock::findLeafWithComparators(
    const void *node,
    const void *literal,
    const UncheckedComparator &literal_less_key_comparator,
    const UncheckedComparator &key_less_literal_comparator) const {
  const NodeHeader *node_header = static_cast<const NodeHeader*>(node);
  if (node_header->is_leaf) {
    return const_cast<void*>(node);
  }
  for (uint16_t key_num = 0;
       key_num < node_header->num_keys;
       ++key_num) {
    if (literal_less_key_comparator.compareDataPtrs(literal,
                                                    static_cast<const char*>(node)
                                                        + sizeof(NodeHeader)
                                                        + key_num * key_length_bytes_)) {
      return findLeafWithComparators(getNode(node_header->node_group_reference, key_num),
                                     literal,
                                     literal_less_key_comparator,
                                     key_less_literal_comparator);
    } else if (!key_less_literal_comparator.compareDataPtrs(static_cast<const char*>(node)
                                                                + sizeof(NodeHeader)
                                                                + key_num * key_length_bytes_,
                                                            literal)) {
      // Handle the special case where duplicate keys are spread across
      // multiple nodes.

      // NOTE(chasseur): If duplicate keys are not allowed, this branch is
      // not necessary, and searches can be done slightly more efficiently.
      return findLeafWithComparators(getNode(node_header->node_group_reference, key_num),
                                     literal,
                                     literal_less_key_comparator,
                                     key_less_literal_comparator);
    }
  }
  return findLeafWithComparators(getNode(node_header->node_group_reference, node_header->num_keys),
                                 literal,
                                 literal_less_key_comparator,
                                 key_less_literal_comparator);
}

void* CSBTreeIndexSubBlock::getLeftmostLeaf() const {
  void* node = getRootNode();
  while (!static_cast<const NodeHeader*>(node)->is_leaf) {
    node = getNode(static_cast<const NodeHeader*>(node)->node_group_reference, 0);
  }
  return node;
}

template <typename CodeType>
CSBTreeIndexSubBlock::InsertReturnValue CSBTreeIndexSubBlock::compressedKeyAddEntryHelper(
    const tuple_id tuple,
    const std::uint32_t compressed_code,
    const NodeHeader &super_root,
    void *root_node) {
  CodeType actual_code = compressed_code;
  if (static_cast<NodeHeader*>(root_node)->is_leaf) {
    return leafInsertHelper(0,
                            tuple,
                            &actual_code,
                            &super_root,
                            root_node);
  } else {
    return internalInsertHelper(0,
                                tuple,
                                &actual_code,
                                &super_root,
                                root_node);
  }
}

CSBTreeIndexSubBlock::InsertReturnValue CSBTreeIndexSubBlock::internalInsertHelper(
    const int node_group_allocation_requirement,
    const tuple_id tuple,
    const void *key,
    const NodeHeader *parent_node_header,
    void *node) {
  DEBUG_ASSERT((node_group_allocation_requirement == 0) || (parent_node_header != NULL));

  NodeHeader *node_header = static_cast<NodeHeader*>(node);
  DEBUG_ASSERT(!node_header->is_leaf);

  // Find the child to insert into.
  uint16_t key_num;
  for (key_num = 0;
       key_num < node_header->num_keys;
       ++key_num) {
    if (key_comparator_->compareDataPtrs(key,
                                         static_cast<char*>(node)
                                             + sizeof(NodeHeader)
                                             + key_num * key_length_bytes_)) {
      break;
    }
  }

  // Insert into the appropriate child.
  void *child_node = getNode(node_header->node_group_reference, key_num);
  int child_node_group_allocation_requirement = 0;
  if (node_header->num_keys == max_keys_internal_) {
    // If the child node is split, this node must also be split.
    if (getRootNode() == node) {
      // If this node is the root, make sure there is additional space for a
      // new root.
      DEBUG_ASSERT(node_group_allocation_requirement == 0);
      child_node_group_allocation_requirement = 2;
    } else {
      child_node_group_allocation_requirement = node_group_allocation_requirement + 1;
    }
  } else {
    // This node can accomodate an additional key without splitting.
    child_node_group_allocation_requirement = 0;
  }
  InsertReturnValue child_return_value;
  if (static_cast<NodeHeader*>(child_node)->is_leaf) {
    child_return_value = leafInsertHelper(child_node_group_allocation_requirement,
                                          tuple,
                                          key,
                                          node_header,
                                          child_node);
  } else {
    child_return_value = internalInsertHelper(child_node_group_allocation_requirement,
                                              tuple,
                                              key,
                                              node_header,
                                              child_node);
  }

  if (child_return_value.new_node_group_id == kNodeGroupFull) {
    // Insertion failed (out of space).
    return child_return_value;
  }

  InsertReturnValue retval;
  bool child_split_across_groups = !child_return_value.left_split_group_smaller
                                   && (key_num == small_half_num_children_);
  if (child_return_value.new_node_group_id != kNodeGroupNone) {
    // A new node group was allocated, and this node must be split.
    DEBUG_ASSERT(child_return_value.split_node_least_key != NULL);
    DEBUG_ASSERT(node_header->num_keys == max_keys_internal_);

    const void *group_end = NULL;
    if (node_group_allocation_requirement) {
      // Parent node is full, must allocate new node group(s).
      // Should already by checked by the child:
      DEBUG_ASSERT(num_free_node_groups_ >= node_group_allocation_requirement);

      // Split the node group.
      group_end = splitNodeGroupHelper(parent_node_header, &node, &retval);
    } else {
      group_end = getNode(parent_node_header->node_group_reference, parent_node_header->num_keys + 1);
    }

    if (group_end == NULL) {
      retval.split_node_least_key = splitNodeAcrossGroups(node,
                                                          retval.new_node_group_id,
                                                          child_return_value.new_node_group_id,
                                                          child_return_value.left_split_group_smaller,
                                                          child_split_across_groups);
    } else {
      retval.split_node_least_key = splitNodeInGroup(node,
                                                     group_end,
                                                     child_return_value.new_node_group_id,
                                                     child_return_value.left_split_group_smaller,
                                                     child_split_across_groups);
    }

    if (child_split_across_groups) {
      // We're done here.
      return retval;
    }

    if (!child_return_value.left_split_group_smaller) {
      DEBUG_ASSERT(key_num >= large_half_num_children_);
      key_num -= large_half_num_children_;

      if (group_end == NULL) {
        node = getNode(retval.new_node_group_id, 0);
      } else {
        node = static_cast<char*>(node) + kCSBTreeNodeSizeBytes;
      }
    }
  }

  if (child_return_value.split_node_least_key != NULL) {
    // If the child was split, insert the new key.
    node_header = static_cast<NodeHeader*>(node);
    void *key_location = static_cast<char*>(node)
                         + sizeof(node_header)
                         + key_num * key_length_bytes_;
    // Move subsequent entries right if necessary.
    if (key_num < node_header->num_keys) {
      memmove(static_cast<char*>(key_location) + key_length_bytes_,
              key_location,
              (node_header->num_keys - key_num) * key_length_bytes_);
    }
    // Insert the new entry.
    memcpy(key_location, child_return_value.split_node_least_key, key_length_bytes_);
    // Increment the key count.
    ++(node_header->num_keys);
  }

  return retval;
}

CSBTreeIndexSubBlock::InsertReturnValue CSBTreeIndexSubBlock::leafInsertHelper(
    const int node_group_allocation_requirement,
    const tuple_id tuple,
    const void *key,
    const NodeHeader *parent_node_header,
    void *node) {
  InsertReturnValue retval;

  NodeHeader *node_header = static_cast<NodeHeader*>(node);
  DEBUG_ASSERT(node_header->is_leaf);

  if (node_header->num_keys == max_keys_leaf_) {
    // '*node' is full and must be split.
    const void *group_end = NULL;
    if (node_group_allocation_requirement) {
      // Parent node is full, must allocate new node group(s).
      if (num_free_node_groups_ < node_group_allocation_requirement) {
        // Not enough node groups to allocate, so insert must fail (although
        // more efficient packing may be possible if index is rebuilt).
        retval.new_node_group_id = kNodeGroupFull;
        return retval;
      }

      // Split the node group.
      group_end = splitNodeGroupHelper(parent_node_header, &node, &retval);
      DEBUG_ASSERT(static_cast<const NodeHeader*>(node)->is_leaf);
    } else {
      // If we are splitting the root node, make sure the caller can allocate a
      // new root.
      if (getRootNode() == node) {
        if (num_free_node_groups_ == 0) {
          retval.new_node_group_id = kNodeGroupFull;
          return retval;
        }
      }

      group_end = getNode(parent_node_header->node_group_reference, parent_node_header->num_keys + 1);
    }

    // This node group (now) has space for a new node. If node splits are
    // asymmetric (i.e. max_keys_leaf_ is odd), do the split such that the new
    // entry will go into the smaller split node, leaving the nodes balanced.
    if (key_comparator_->compareDataPtrs(key,
                                         static_cast<const char*>(node)
                                             + sizeof(NodeHeader)
                                             + (small_half_num_keys_leaf_) * key_tuple_id_pair_length_bytes_)) {
      // Insert in the first half.
      if (group_end == NULL) {
        retval.split_node_least_key = splitNodeAcrossGroups(node,
                                                            retval.new_node_group_id,
                                                            kNodeGroupNone,
                                                            true,
                                                            false);
      } else {
        retval.split_node_least_key = splitNodeInGroup(node, group_end, kNodeGroupNone, true, false);
      }
    } else {
      // Insert in the second half.
      // Note: The new key may be inserted at the first position in the split
      // node. The pointer 'retval.split_node_least_key' will remain correct
      // if this is the case, as splitNodeInGroup() returns a pointer to the
      // first leaf key's location.
      if (group_end == NULL) {
        retval.split_node_least_key = splitNodeAcrossGroups(node,
                                                            retval.new_node_group_id,
                                                            kNodeGroupNone,
                                                            false,
                                                            false);
        node = getNode(retval.new_node_group_id, 0);
      } else {
        retval.split_node_least_key = splitNodeInGroup(node, group_end, kNodeGroupNone, false, false);
        node = static_cast<char*>(node) + kCSBTreeNodeSizeBytes;
      }
    }
  }

  // Either splitting was not necessary, or it already occured. Insert the key.
  insertEntryInLeaf(tuple, key, node);
  return retval;
}

const void* CSBTreeIndexSubBlock::splitNodeGroupHelper(
    const NodeHeader *parent_node_header,
    void **node,
    InsertReturnValue *caller_return_value) {
  const void *center_node = getNode(parent_node_header->node_group_reference, small_half_num_children_);
  if (*node < center_node) {
    caller_return_value->left_split_group_smaller = true;
    caller_return_value->new_node_group_id = splitNodeGroup(parent_node_header, true, false);
    return getNode(parent_node_header->node_group_reference, small_half_num_children_);
  } else {
    caller_return_value->left_split_group_smaller = false;
    if (*node == center_node) {
      caller_return_value->new_node_group_id = splitNodeGroup(parent_node_header, false, true);
      return NULL;
    } else {
      caller_return_value->new_node_group_id = splitNodeGroup(parent_node_header, false, false);
      if (static_cast<const NodeHeader*>(*node)->node_group_reference >= 0) {
        *node = static_cast<char*>(getNode(caller_return_value->new_node_group_id, 0))
                + (static_cast<const char*>(*node) - static_cast<const char*>(center_node))
                - kCSBTreeNodeSizeBytes;
      } else {
        *node = static_cast<char*>(getNode(caller_return_value->new_node_group_id, 0))
                + (static_cast<const char*>(*node) - static_cast<const char*>(center_node));
      }
      return getNode(caller_return_value->new_node_group_id, small_half_num_children_);
    }
  }
}

int CSBTreeIndexSubBlock::splitNodeGroup(const NodeHeader *parent_node_header,
                                         const bool left_smaller,
                                         const bool will_split_node_across_groups) {
  DEBUG_ASSERT(!parent_node_header->is_leaf);
  DEBUG_ASSERT(parent_node_header->num_keys == max_keys_internal_);
  DEBUG_ASSERT(num_free_node_groups_ > 0);
  if (will_split_node_across_groups) {
    DEBUG_ASSERT(!left_smaller);
  }

  // Allocate a new node group.
  int new_node_group_id = allocateNodeGroup();
  DEBUG_ASSERT(new_node_group_id >= 0);
  void *copy_destination;
  if (will_split_node_across_groups) {
    copy_destination = getNode(new_node_group_id, 1);
  } else {
    copy_destination = getNode(new_node_group_id, 0);
  }

  NodeHeader *rightmost_remaining_node_header;
  // Move half of the nodes in the current group to the new group.
  if (left_smaller) {
    memcpy(copy_destination,
           getNode(parent_node_header->node_group_reference, small_half_num_children_),
           large_half_num_children_ * kCSBTreeNodeSizeBytes);
    rightmost_remaining_node_header
        = static_cast<NodeHeader*>(getNode(parent_node_header->node_group_reference,
                                           small_half_num_children_ - 1));
  } else {
    memcpy(copy_destination,
           getNode(parent_node_header->node_group_reference, large_half_num_children_),
           small_half_num_children_ * kCSBTreeNodeSizeBytes);
    rightmost_remaining_node_header
        = static_cast<NodeHeader*>(getNode(parent_node_header->node_group_reference,
                                           large_half_num_children_ - 1));
  }

  // If the split nodes are leaves, adjust the node_group_reference of the
  // rightmost remaining node.
  if (rightmost_remaining_node_header->is_leaf) {
    rightmost_remaining_node_header->node_group_reference = new_node_group_id;
  }

  return new_node_group_id;
}

const void* CSBTreeIndexSubBlock::splitNodeInGroup(void *node,
                                                   const void *group_end,
                                                   const int right_child_node_group,
                                                   const bool left_smaller,
                                                   const bool child_was_split_across_groups) {
  NodeHeader *node_header = static_cast<NodeHeader*>(node);
  if (child_was_split_across_groups) {
    DEBUG_ASSERT(!node_header->is_leaf);
    DEBUG_ASSERT(!left_smaller);
  }
  if (node_header->is_leaf) {
    DEBUG_ASSERT(right_child_node_group == kNodeGroupNone);
    DEBUG_ASSERT(node_header->num_keys == max_keys_leaf_);
  } else {
    DEBUG_ASSERT(right_child_node_group >= 0);
    DEBUG_ASSERT(node_header->num_keys == max_keys_internal_);
  }

  void *next_node = static_cast<char*>(node) + kCSBTreeNodeSizeBytes;
  if (group_end != next_node) {
    // Shift subsequent nodes right.
    memmove(static_cast<char*>(next_node) + kCSBTreeNodeSizeBytes,
            next_node,
            static_cast<const char*>(group_end) - static_cast<char*>(next_node));
  }

  // Do the split.
  NodeHeader *next_node_header = static_cast<NodeHeader*>(next_node);
  if (node_header->is_leaf) {
    // Set up the next node's header.
    if (left_smaller) {
      next_node_header->num_keys = large_half_num_keys_leaf_;
    } else {
      next_node_header->num_keys = small_half_num_keys_leaf_;
    }
    next_node_header->is_leaf = true;
    next_node_header->node_group_reference = node_header->node_group_reference;

    // Modify the current node's header.
    if (left_smaller) {
      node_header->num_keys = small_half_num_keys_leaf_;
    } else {
      node_header->num_keys = large_half_num_keys_leaf_;
    }
    node_header->node_group_reference = kNodeGroupNextLeaf;

    // Copy half the keys over.
    memcpy(static_cast<char*>(next_node) + sizeof(NodeHeader),
           static_cast<const char*>(node)
               + sizeof(NodeHeader)
               + (node_header->num_keys * key_tuple_id_pair_length_bytes_),
           next_node_header->num_keys * key_tuple_id_pair_length_bytes_);
    return static_cast<const char*>(next_node) + sizeof(NodeHeader);
  } else {
    // Set up the next node's header.
    if (left_smaller) {
      next_node_header->num_keys = large_half_num_children_ - 1;
    } else {
      if (child_was_split_across_groups) {
        next_node_header->num_keys = small_half_num_children_;
      } else {
        next_node_header->num_keys = small_half_num_children_ - 1;
      }
    }
    next_node_header->is_leaf = false;
    next_node_header->node_group_reference = right_child_node_group;

    // Modify the current node's header.
    if (left_smaller) {
      node_header->num_keys = small_half_num_children_ - 1;
    } else {
      node_header->num_keys = large_half_num_children_ - 1;
    }

    if (child_was_split_across_groups) {
      // Copy half the entries over.
      memcpy(static_cast<char*>(next_node) + sizeof(NodeHeader),
             static_cast<const char*>(node)
                 + sizeof(NodeHeader)
                 + ((node_header->num_keys) * key_length_bytes_),
             next_node_header->num_keys * key_length_bytes_);
    } else {
      // Copy half the keys over (shift by one for the leftmost child).
      memcpy(static_cast<char*>(next_node) + sizeof(NodeHeader),
             static_cast<const char*>(node)
                 + sizeof(NodeHeader)
                 + ((node_header->num_keys + 1) * key_length_bytes_),
             next_node_header->num_keys * key_length_bytes_);
    }
    // Push the middle key up.
    return getLeastKey(next_node);
  }
}

const void* CSBTreeIndexSubBlock::splitNodeAcrossGroups(void *node,
                                                        const int destination_group_number,
                                                        const int right_child_node_group,
                                                        const bool left_smaller,
                                                        const bool child_was_split_across_groups) {
  DEBUG_ASSERT(destination_group_number >= 0);
  DEBUG_ASSERT(static_cast<size_t>(destination_group_number) < node_group_used_bitmap_->size());
  DEBUG_ASSERT(node_group_used_bitmap_->getBit(destination_group_number));

  NodeHeader *node_header = static_cast<NodeHeader*>(node);
  if (child_was_split_across_groups) {
    DEBUG_ASSERT(!node_header->is_leaf);
    DEBUG_ASSERT(!left_smaller);
  }
  if (node_header->is_leaf) {
    DEBUG_ASSERT(right_child_node_group == kNodeGroupNone);
    DEBUG_ASSERT(node_header->num_keys == max_keys_leaf_);
    DEBUG_ASSERT(node_header->node_group_reference == destination_group_number);
  } else {
    DEBUG_ASSERT(right_child_node_group >= 0);
    DEBUG_ASSERT(node_header->num_keys == max_keys_internal_);
  }

  // Do the split.
  void *destination_node = getNode(destination_group_number, 0);
  NodeHeader *destination_node_header = static_cast<NodeHeader*>(destination_node);
  if (node_header->is_leaf) {
    // Set up the destination node's header.
    if (left_smaller) {
      destination_node_header->num_keys = large_half_num_keys_leaf_;
    } else {
      destination_node_header->num_keys = small_half_num_keys_leaf_;
    }
    destination_node_header->is_leaf = true;
    destination_node_header->node_group_reference = kNodeGroupNextLeaf;

    // Modify the current node's header.
    if (left_smaller) {
      node_header->num_keys = small_half_num_keys_leaf_;
    } else {
      node_header->num_keys = large_half_num_keys_leaf_;
    }

    // Copy half the entries over.
    memcpy(static_cast<char*>(destination_node) + sizeof(NodeHeader),
           static_cast<const char*>(node)
               + sizeof(NodeHeader)
               + (node_header->num_keys * key_tuple_id_pair_length_bytes_),
           destination_node_header->num_keys * key_tuple_id_pair_length_bytes_);
    return static_cast<const char*>(destination_node) + sizeof(NodeHeader);
  } else {
    // Set up the destination node's header.
    if (left_smaller) {
      destination_node_header->num_keys = large_half_num_children_ - 1;
    } else {
      if (child_was_split_across_groups) {
        destination_node_header->num_keys = small_half_num_children_;
      } else {
        destination_node_header->num_keys = small_half_num_children_ - 1;
      }
    }
    destination_node_header->is_leaf = false;
    destination_node_header->node_group_reference = right_child_node_group;

    // Modify the current node's header.
    if (left_smaller) {
      node_header->num_keys = small_half_num_children_ - 1;
    } else {
      node_header->num_keys = large_half_num_children_ - 1;
    }

    if (child_was_split_across_groups) {
      // Copy half the keys over.
      memcpy(static_cast<char*>(destination_node) + sizeof(NodeHeader),
             static_cast<const char*>(node)
                 + sizeof(NodeHeader)
                 + (node_header->num_keys * key_length_bytes_),
             destination_node_header->num_keys * key_length_bytes_);
    } else {
      // Copy half the keys over (shift by one for the leftmost child).
      memcpy(static_cast<char*>(destination_node) + sizeof(NodeHeader),
             static_cast<const char*>(node)
                 + sizeof(NodeHeader)
                 + ((node_header->num_keys + 1) * key_length_bytes_),
             destination_node_header->num_keys * key_length_bytes_);
    }
    // Push the middle key up.
    return getLeastKey(destination_node);
  }
}

void CSBTreeIndexSubBlock::insertEntryInLeaf(const tuple_id tuple, const void *key, void *node) {
  DEBUG_ASSERT(static_cast<NodeHeader*>(node)->is_leaf);

  const uint16_t num_keys = static_cast<NodeHeader*>(node)->num_keys;
  DEBUG_ASSERT(num_keys < max_keys_leaf_);

  char *current_key = static_cast<char*>(node) + sizeof(NodeHeader);
  for (uint16_t key_num = 0;
       key_num < num_keys;
       ++key_num, current_key += key_tuple_id_pair_length_bytes_) {
    if (key_comparator_->compareDataPtrs(key, current_key)) {
      // Shift subsequent entries right.
      memmove(current_key + key_tuple_id_pair_length_bytes_,
              current_key,
              (num_keys - key_num) * key_tuple_id_pair_length_bytes_);
      break;
    }
  }
  // Insert the new entry.
  memcpy(current_key, key, key_length_bytes_);
  *reinterpret_cast<tuple_id*>(current_key + key_length_bytes_) = tuple;
  // Increment the key count.
  ++(static_cast<NodeHeader*>(node)->num_keys);
}

template <typename CodeType>
void CSBTreeIndexSubBlock::compressedKeyRemoveEntryHelper(const tuple_id tuple,
                                                          const std::uint32_t compressed_code) {
  CodeType actual_code = compressed_code;

  removeEntryFromLeaf(tuple,
                      &actual_code,
                      findLeaf(getRootNode(), &actual_code));
}

void CSBTreeIndexSubBlock::removeEntryFromLeaf(const tuple_id tuple, const void *key, void *node) {
  DEBUG_ASSERT(static_cast<NodeHeader*>(node)->is_leaf);

  void *right_sibling;
  const uint16_t num_keys = static_cast<NodeHeader*>(node)->num_keys;
  // If node is totally empty, immediately chase the next sibling.
  if (num_keys == 0) {
    right_sibling = getRightSiblingOfLeafNode(node);
    if (right_sibling != NULL) {
      removeEntryFromLeaf(tuple, key, right_sibling);
      return;
    } else {
      FATAL_ERROR("CSBTree: attempted to remove nonexistent entry.");
    }
  }

  for (uint16_t key_num = 0;
       key_num < num_keys;
       ++key_num) {
    char* existing_key_ptr = static_cast<char*>(node)
                             + sizeof(NodeHeader)
                             + key_num * key_tuple_id_pair_length_bytes_;
    if (key_comparator_->compareDataPtrs(existing_key_ptr, key)) {
      // Haven't yet reached the target key.
      continue;
    } else if (key_comparator_->compareDataPtrs(key, existing_key_ptr)) {
      // Past the target key, but the target has not been found.
      FATAL_ERROR("CSBTree: attempted to remove nonexistent entry.");
    } else {
      // Key matches, so check tuple_id.
      if (tuple == *reinterpret_cast<const tuple_id*>(existing_key_ptr + key_length_bytes_)) {
        // Match found, remove the entry.
        if (key_num != num_keys - 1) {
          // Move subsequent entries forward.
          memmove(existing_key_ptr,
                  existing_key_ptr + key_tuple_id_pair_length_bytes_,
                  (num_keys - key_num - 1) * key_tuple_id_pair_length_bytes_);
        }
        // Decrement the key count.
        --(static_cast<NodeHeader*>(node)->num_keys);
        return;
      } else {
        // Not the correct tuple_id, but there may be others with the same key.
        continue;
      }
    }
  }

  // Proceed to next sibling.
  right_sibling = getRightSiblingOfLeafNode(node);
  if (right_sibling != NULL) {
    removeEntryFromLeaf(tuple, key, right_sibling);
    return;
  } else {
    FATAL_ERROR("CSBTree: attempted to remove nonexistent entry.");
  }
}

TupleIdSequence* CSBTreeIndexSubBlock::evaluateComparisonPredicateOnUncompressedKey(
    const Comparison::ComparisonID comp,
    const TypeInstance &right_literal) const {
  DEBUG_ASSERT(!key_is_compressed_);
  DEBUG_ASSERT(!key_is_composite_);

  // If the literal is not exactly the same type as the key, use custom
  // comparators.
  ScopedPtr<UncheckedComparator> literal_less_key_comparator;
  ScopedPtr<UncheckedComparator> key_less_literal_comparator;
  UncheckedComparator *literal_less_key_comparator_ptr;
  UncheckedComparator *key_less_literal_comparator_ptr;

  if (!relation_.getAttributeById(indexed_attribute_ids_.front()).getType().equals(right_literal.getType())) {
    literal_less_key_comparator.reset(
        Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(
            right_literal.getType(),
            relation_.getAttributeById(indexed_attribute_ids_.front()).getType()));
    literal_less_key_comparator_ptr = literal_less_key_comparator.get();

    key_less_literal_comparator.reset(
        Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(
            relation_.getAttributeById(indexed_attribute_ids_.front()).getType(),
            right_literal.getType()));
    key_less_literal_comparator_ptr = key_less_literal_comparator.get();
  } else {
    literal_less_key_comparator_ptr = key_comparator_.get();
    key_less_literal_comparator_ptr = key_comparator_.get();
  }

  switch (comp) {
    case Comparison::kEqual:
      return evaluateEqualPredicate(right_literal.getDataPtr(),
                                    *literal_less_key_comparator_ptr,
                                    *key_less_literal_comparator_ptr);
    case Comparison::kNotEqual:
      return evaluateNotEqualPredicate(right_literal.getDataPtr(),
                                       *literal_less_key_comparator_ptr,
                                       *key_less_literal_comparator_ptr);
    case Comparison::kLess:
      return evaluateLessPredicate<false>(right_literal.getDataPtr(),
                                          *literal_less_key_comparator_ptr,
                                          *key_less_literal_comparator_ptr);
    case Comparison::kLessOrEqual:
      return evaluateLessPredicate<true>(right_literal.getDataPtr(),
                                         *literal_less_key_comparator_ptr,
                                         *key_less_literal_comparator_ptr);
    case Comparison::kGreater:
      return evaluateGreaterPredicate<false>(right_literal.getDataPtr(),
                                             *literal_less_key_comparator_ptr,
                                             *key_less_literal_comparator_ptr);
    case Comparison::kGreaterOrEqual:
      return evaluateGreaterPredicate<true>(right_literal.getDataPtr(),
                                            *literal_less_key_comparator_ptr,
                                            *key_less_literal_comparator_ptr);
    default:
      FATAL_ERROR("Unknown Comparison in CSBTreeIndexSubBlock"
                  "::evaluateComparisonPredicateOnUncompressedKey()");
  }
}

TupleIdSequence* CSBTreeIndexSubBlock::evaluateComparisonPredicateOnCompressedKey(
    Comparison::ComparisonID comp,
    const TypeInstance &right_literal) const {
  DEBUG_ASSERT(key_is_compressed_);
  DEBUG_ASSERT(!key_is_composite_);

  // Stack variables to hold compressed codes as needed.
  uint8_t byte_code;
  uint16_t short_code;
  uint32_t word_code;

  const void *data_ptr;

  const CompressedTupleStorageSubBlock &compressed_tuple_store
      = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
  if (compressed_tuple_store.compressedAttributeIsDictionaryCompressed(indexed_attribute_ids_.front())) {
    const CompressionDictionary &dict
        = compressed_tuple_store.compressedGetDictionary(indexed_attribute_ids_.front());
    switch (comp) {
      case Comparison::kEqual:
        byte_code = short_code = word_code = dict.getCodeForTypedValue(right_literal);
        if (word_code == dict.numberOfCodes()) {
          return new TupleIdSequence();
        }
        break;
      case Comparison::kNotEqual:
        byte_code = short_code = word_code = dict.getCodeForTypedValue(right_literal);
        if (word_code == dict.numberOfCodes()) {
          return tuple_store_.getMatchesForPredicate(NULL);
        }
        break;
      default:
        {
          pair<uint32_t, uint32_t> limits = dict.getLimitCodesForComparisonTyped(comp, right_literal);
          if (limits.first == 0) {
            if (limits.second == dict.numberOfCodes()) {
              return tuple_store_.getMatchesForPredicate(NULL);
            } else {
              byte_code = short_code = word_code = limits.second;
              comp = Comparison::kLess;
            }
          } else if (limits.second == dict.numberOfCodes()) {
            byte_code = short_code = word_code = limits.first;
            comp = Comparison::kGreaterOrEqual;
          } else {
            FATAL_ERROR("CompressionDictionary::getLimitCodesForComparisonTyped() returned "
                        "limits which did not extend to either the minimum or maximum code "
                        "when called by CSBTreeIndexSubBlock::evaluateComparisonPredicateOnCompressedKey().");
          }
        }
        break;
    }
  } else {
    if (compressed_tuple_store.compressedComparisonIsAlwaysTrueForTruncatedAttribute(
        comp,
        indexed_attribute_ids_.front(),
        right_literal)) {
      return tuple_store_.getMatchesForPredicate(NULL);
    } else if (compressed_tuple_store.compressedComparisonIsAlwaysFalseForTruncatedAttribute(
        comp,
        indexed_attribute_ids_.front(),
        right_literal)) {
      return new TupleIdSequence();
    } else {
      switch (comp) {
        case Comparison::kEqual:
        case Comparison::kNotEqual:
          byte_code = short_code = word_code = right_literal.numericGetLongValue();
          break;
        // Adjustments for kLessOrEqual and kGreater make predicate evaluation
        // a bit more efficient (particularly in the presence of repeated
        // keys). Adding 1 will not overflow the code type, as a literal of
        // exactly the maximum possible value for the code type would have
        // already caused
        // compressedComparisonIsAlwaysTrueForTruncatedAttribute() to return
        // true for kLessOrEqual, or
        // compressedComparisonIsAlwaysFalseForTruncatedAttribute() to return
        // false for kGreater.
        case Comparison::kLessOrEqual:
          comp = Comparison::kLess;
          byte_code = short_code = word_code
              = 1 + CompressedTupleStorageSubBlock::GetEffectiveLiteralValueForComparisonWithTruncatedAttribute(
                  comp,
                  right_literal);
          break;
        case Comparison::kGreater:
          comp = Comparison::kGreaterOrEqual;
          byte_code = short_code = word_code
              = 1 + CompressedTupleStorageSubBlock::GetEffectiveLiteralValueForComparisonWithTruncatedAttribute(
                  comp,
                  right_literal);
          break;
        default:
          byte_code = short_code = word_code
              = CompressedTupleStorageSubBlock::GetEffectiveLiteralValueForComparisonWithTruncatedAttribute(
                  comp,
                  right_literal);
          break;
      }
    }
  }

  switch (compressed_tuple_store.compressedGetCompressedAttributeSize(indexed_attribute_ids_.front())) {
    case 1:
      data_ptr = &byte_code;
      break;
    case 2:
      data_ptr = &short_code;
      break;
    case 4:
      data_ptr = &word_code;
      break;
    default:
      FATAL_ERROR("Unexpected compressed key byte-length (not 1, 2, or 4) encountered "
                  "in CSBTreeIndexSubBlock::getMatchesForPredicate()");
  }

  switch (comp) {
    case Comparison::kEqual:
      return evaluateEqualPredicate(data_ptr,
                                    *key_comparator_,
                                    *key_comparator_);
    case Comparison::kNotEqual:
      return evaluateNotEqualPredicate(data_ptr,
                                       *key_comparator_,
                                       *key_comparator_);
    case Comparison::kLess:
      return evaluateLessPredicate<false>(data_ptr,
                                          *key_comparator_,
                                          *key_comparator_);
    case Comparison::kGreaterOrEqual:
      return evaluateGreaterPredicate<true>(data_ptr,
                                            *key_comparator_,
                                            *key_comparator_);
    default:
      // Note: kLessOrEqual and kGreater will already be adjusted to kLess or
      // KGreaterOrEqual.
      FATAL_ERROR("Unknown Comparison in CSBTreeIndexSubBlock"
                  "::evaluateComparisonPredicateOnCompressedKey()");
  }
}

TupleIdSequence* CSBTreeIndexSubBlock::evaluateEqualPredicate(
    const void *literal,
    const UncheckedComparator &literal_less_key_comparator,
    const UncheckedComparator &key_less_literal_comparator) const {
  ScopedPtr<TupleIdSequence> matches(new TupleIdSequence());

  bool match_found = false;
  const void *search_node = findLeafWithComparators(getRootNode(),
                                                    literal,
                                                    literal_less_key_comparator,
                                                    key_less_literal_comparator);
  while (search_node != NULL) {
    DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
    uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
    const char *key_ptr = static_cast<const char*>(search_node) + sizeof(NodeHeader);
    for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
      if (!match_found) {
        if (!key_less_literal_comparator.compareDataPtrs(key_ptr, literal)) {
          match_found = true;
        }
      }

      if (match_found) {
        if (literal_less_key_comparator.compareDataPtrs(literal, key_ptr)) {
          // End of matches.
          return matches.release();
        }
        matches->append(*reinterpret_cast<const tuple_id*>(key_ptr + key_length_bytes_));
      }
      key_ptr += key_tuple_id_pair_length_bytes_;
    }
    search_node = getRightSiblingOfLeafNode(search_node);
  }

  return matches.release();
}

TupleIdSequence* CSBTreeIndexSubBlock::evaluateNotEqualPredicate(
    const void *literal,
    const UncheckedComparator &literal_less_key_comparator,
    const UncheckedComparator &key_less_literal_comparator) const {
  ScopedPtr<TupleIdSequence> matches(new TupleIdSequence());

  const void *boundary_node = findLeafWithComparators(getRootNode(),
                                                      literal,
                                                      literal_less_key_comparator,
                                                      key_less_literal_comparator);
  const void *search_node = getLeftmostLeaf();

  // Fill in all tuples from leaves definitively less than the key.
  while (search_node != boundary_node) {
    DEBUG_ASSERT(search_node != NULL);
    DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
    uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
    const char *tuple_id_ptr = static_cast<const char*>(search_node)
                               + sizeof(NodeHeader)
                               + key_length_bytes_;
    for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
      matches->append(*reinterpret_cast<const tuple_id*>(tuple_id_ptr));
      tuple_id_ptr += key_tuple_id_pair_length_bytes_;
    }
    search_node = getRightSiblingOfLeafNode(search_node);
  }

  // Actually do comparisons in leaves that may contain the literal key.
  bool equal_found = false;
  bool past_equal = false;
  while (search_node != NULL) {
    DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
    uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
    const char *key_ptr = static_cast<const char*>(search_node) + sizeof(NodeHeader);
    for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
      if (!equal_found) {
        if (key_less_literal_comparator.compareDataPtrs(key_ptr, literal)) {
          // key < literal
          matches->append(*reinterpret_cast<const tuple_id*>(key_ptr + key_length_bytes_));
        } else {
          equal_found = true;
        }
      }

      if (equal_found) {
        if (literal_less_key_comparator.compareDataPtrs(literal, key_ptr)) {
          // literal < key
          // Fill in the rest of the keys from this leaf.
          for (uint16_t subsequent_num = entry_num;
               subsequent_num < num_keys;
               ++subsequent_num) {
            matches->append(*reinterpret_cast<const tuple_id*>(key_ptr + key_length_bytes_));
            key_ptr += key_tuple_id_pair_length_bytes_;
          }
          past_equal = true;
          break;
        }
      }
      key_ptr += key_tuple_id_pair_length_bytes_;
    }
    search_node = getRightSiblingOfLeafNode(search_node);
    if (past_equal) {
      break;
    }
  }

  // Fill in all tuples from leaves definitively greater than the key.
  while (search_node != NULL) {
    DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
    uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
    const char *tuple_id_ptr = static_cast<const char*>(search_node)
                               + sizeof(NodeHeader)
                               + key_length_bytes_;
    for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
      matches->append(*reinterpret_cast<const tuple_id*>(tuple_id_ptr));
      tuple_id_ptr += key_tuple_id_pair_length_bytes_;
    }
    search_node = getRightSiblingOfLeafNode(search_node);
  }

  return matches.release();
}

template <bool include_equal>
TupleIdSequence* CSBTreeIndexSubBlock::evaluateLessPredicate(
    const void *literal,
    const UncheckedComparator &literal_less_key_comparator,
    const UncheckedComparator &key_less_literal_comparator) const {
  ScopedPtr<TupleIdSequence> matches(new TupleIdSequence());

  const void *boundary_node = findLeafWithComparators(getRootNode(),
                                                      literal,
                                                      literal_less_key_comparator,
                                                      key_less_literal_comparator);
  const void *search_node = getLeftmostLeaf();

  // Fill in all tuples from leaves definitively less than the key.
  while (search_node != boundary_node) {
    DEBUG_ASSERT(search_node != NULL);
    DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
    uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
    const char *tuple_id_ptr = static_cast<const char*>(search_node)
                               + sizeof(NodeHeader)
                               + key_length_bytes_;
    for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
      matches->append(*reinterpret_cast<const tuple_id*>(tuple_id_ptr));
      tuple_id_ptr += key_tuple_id_pair_length_bytes_;
    }
    search_node = getRightSiblingOfLeafNode(search_node);
  }

  // Actually do comparisons in leaves that may contain the literal key.
  if (include_equal) {
    bool equal_found = false;
    while (search_node != NULL) {
      DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
      uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
      const char *key_ptr = static_cast<const char*>(search_node) + sizeof(NodeHeader);
      for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
        if (!equal_found) {
          if (key_less_literal_comparator.compareDataPtrs(key_ptr, literal)) {
            // key < literal
            matches->append(*reinterpret_cast<const tuple_id*>(key_ptr + key_length_bytes_));
          } else {
            equal_found = true;
          }
        }

        if (equal_found) {
          if (literal_less_key_comparator.compareDataPtrs(literal, key_ptr)) {
            // literal < key
            return matches.release();
          } else {
            matches->append(*reinterpret_cast<const tuple_id*>(key_ptr + key_length_bytes_));
          }
        }

        key_ptr += key_tuple_id_pair_length_bytes_;
      }
      search_node = getRightSiblingOfLeafNode(search_node);
    }
  } else {
    while (search_node != NULL) {
      DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
      uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
      const char *key_ptr = static_cast<const char*>(search_node) + sizeof(NodeHeader);
      for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
        if (key_less_literal_comparator.compareDataPtrs(key_ptr, literal)) {
          // key < literal
          matches->append(*reinterpret_cast<const tuple_id*>(key_ptr + key_length_bytes_));
        } else {
          return matches.release();
        }
        key_ptr += key_tuple_id_pair_length_bytes_;
      }
      search_node = getRightSiblingOfLeafNode(search_node);
    }
  }

  return matches.release();
}

template <bool include_equal>
TupleIdSequence* CSBTreeIndexSubBlock::evaluateGreaterPredicate(
    const void *literal,
    const UncheckedComparator &literal_less_key_comparator,
    const UncheckedComparator &key_less_literal_comparator) const {
  ScopedPtr<TupleIdSequence> matches(new TupleIdSequence());

  const void *search_node = findLeafWithComparators(getRootNode(),
                                                    literal,
                                                    literal_less_key_comparator,
                                                    key_less_literal_comparator);

  // Do comparisons in leaves that may contain the literal key.
  bool match_found = false;
  while (search_node != NULL) {
    DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
    uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
    const char *key_ptr = static_cast<const char*>(search_node) + sizeof(NodeHeader);
    for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
      if (include_equal) {
        if (!key_less_literal_comparator.compareDataPtrs(key_ptr, literal)) {
          match_found = true;
        }
      } else {
        if (literal_less_key_comparator.compareDataPtrs(literal, key_ptr)) {
          match_found = true;
        }
      }

      if (match_found) {
        // Fill in the matching entries from this leaf.
        for (uint16_t match_num = entry_num; match_num < num_keys; ++match_num) {
          matches->append(*reinterpret_cast<const tuple_id*>(key_ptr + key_length_bytes_));
          key_ptr += key_tuple_id_pair_length_bytes_;
        }
        break;
      }

      key_ptr += key_tuple_id_pair_length_bytes_;
    }

    search_node = getRightSiblingOfLeafNode(search_node);
    if (match_found) {
      break;
    }
  }

  // Fill in all tuples from leaves definitively greater than the key.
  while (search_node != NULL) {
    DEBUG_ASSERT(static_cast<const NodeHeader*>(search_node)->is_leaf);
    uint16_t num_keys = static_cast<const NodeHeader*>(search_node)->num_keys;
    const char *tuple_id_ptr = static_cast<const char*>(search_node)
                               + sizeof(NodeHeader)
                               + key_length_bytes_;
    for (uint16_t entry_num = 0; entry_num < num_keys; ++entry_num) {
      matches->append(*reinterpret_cast<const tuple_id*>(tuple_id_ptr));
      tuple_id_ptr += key_tuple_id_pair_length_bytes_;
    }
    search_node = getRightSiblingOfLeafNode(search_node);
  }

  return matches.release();
}

bool CSBTreeIndexSubBlock::rebuildSpaceCheck() const {
  DEBUG_ASSERT(node_group_used_bitmap_->size() > 0);
  if (tuple_store_.isEmpty()) {
    return true;
  }

  // Check that this sub block will be able to fit entries for all tuples.
  const tuple_id num_tuples = tuple_store_.numTuples();
  // If all tuples can fit in a single leaf, then the root will be sufficient.
  if (num_tuples > max_keys_leaf_) {
    int num_node_groups_needed = 1;  // 1 for the root.
    const int keys_in_leaf_node_group = max_keys_leaf_ * (max_keys_internal_ + 1);
    int num_node_groups_this_level = (num_tuples / keys_in_leaf_node_group)
                                     + (num_tuples % keys_in_leaf_node_group ? 1 : 0);
    num_node_groups_needed += num_node_groups_this_level;
    while (num_node_groups_this_level > 1) {
      num_node_groups_this_level = (num_node_groups_this_level / (max_keys_internal_ + 1))
                                   + (num_node_groups_this_level % (max_keys_internal_ + 1) ? 1 : 0);
      num_node_groups_needed += num_node_groups_this_level;
    }
    if (static_cast<size_t>(num_node_groups_needed) > node_group_used_bitmap_->size()) {
      return false;
    }
  }

  return true;
}

uint16_t CSBTreeIndexSubBlock::rebuildLeaves(std::vector<int> *used_node_groups) {
  DEBUG_ASSERT(static_cast<size_t>(num_free_node_groups_) == node_group_used_bitmap_->size() - 1);
  DEBUG_ASSERT(rebuildSpaceCheck());

  if (key_is_compressed_) {
    vector<csbtree_internal::CompressedEntryReference> entries;
    generateEntryReferencesFromCompressedCodes(&entries);
    return buildLeavesFromEntryReferences<csbtree_internal::CompressedEntryReference>(&entries,
                                                                                      used_node_groups);
  } else {
    vector<csbtree_internal::EntryReference> entries;
    // These scoped containers automatically deallocate heap-allocated key copies
    // when they go out of scope.
    PtrVector<ScopedBuffer> composite_key_buffers;
    PtrVector<TypeInstance> literal_typed_keys;

    if (key_is_composite_) {
      // Composite keys. Copies will be stored in composite_key_buffers.
      generateEntryReferencesFromCompositeKeys(&entries, &composite_key_buffers);
    } else if (tuple_store_supports_untyped_ptr_) {
      // No need to copy keys.
      generateEntryReferencesFromUntypedPtrs(&entries);
    } else {
      // Keys will be stored as LiteralTypeInstances in literal_typed_keys.
      generateEntryReferencesFromTypeInstances(&entries, &literal_typed_keys);
    }

    return buildLeavesFromEntryReferences<csbtree_internal::EntryReference>(&entries, used_node_groups);
  }
}

template <class EntryReferenceT>
std::uint16_t CSBTreeIndexSubBlock::buildLeavesFromEntryReferences(
    std::vector<EntryReferenceT> *entry_references,
    std::vector<int> *used_node_groups) {
  // Sort all entries by key.
  sort(entry_references->begin(),
       entry_references->end(),
       csbtree_internal::EntryReferenceComparator<EntryReferenceT>(*key_comparator_));

  // Build tree from packed leaves.
  int current_node_group_number = getRootNodeGroupNumber();
  used_node_groups->push_back(current_node_group_number);

  uint16_t current_node_number = 0;
  uint16_t current_key_number = 0;
  char *node_ptr = static_cast<char*>(getNode(current_node_group_number, current_node_number));
  // Set up the first node's header.
  // If this node is not totally full (i.e. it is the rightmost leaf),
  // num_keys will be reset to the correct value after the loop below.
  reinterpret_cast<NodeHeader*>(node_ptr)->num_keys = max_keys_leaf_;
  reinterpret_cast<NodeHeader*>(node_ptr)->is_leaf = true;
  reinterpret_cast<NodeHeader*>(node_ptr)->node_group_reference = kNodeGroupNone;

  // Build all the leaves.
  for (typename vector<EntryReferenceT>::const_iterator entry_it = entry_references->begin();
       entry_it != entry_references->end();
       ++entry_it) {
    if (current_key_number == max_keys_leaf_) {
      // At the end of this node, most move to the next.
      if (current_node_number == max_keys_internal_) {
        // At the end of this node group, must allocate a new one.
        int next_node_group_number = allocateNodeGroup();
        DEBUG_ASSERT(next_node_group_number >= 0);
        used_node_groups->push_back(next_node_group_number);
        reinterpret_cast<NodeHeader*>(node_ptr)->node_group_reference = next_node_group_number;
        current_node_group_number = next_node_group_number;
        current_node_number = 0;
        node_ptr = static_cast<char*>(getNode(current_node_group_number, current_node_number));
      } else {
        // Use the next node in the current group.
        reinterpret_cast<NodeHeader*>(node_ptr)->node_group_reference = kNodeGroupNextLeaf;
        ++current_node_number;
        node_ptr += kCSBTreeNodeSizeBytes;
      }
      // Set up new leaf node's header.
      // If this node is not totally full (i.e. it is the rightmost leaf),
      // num_keys will be reset to the correct value when this loop exits.
      reinterpret_cast<NodeHeader*>(node_ptr)->num_keys = max_keys_leaf_;
      reinterpret_cast<NodeHeader*>(node_ptr)->is_leaf = true;
      reinterpret_cast<NodeHeader*>(node_ptr)->node_group_reference = kNodeGroupNone;
      // Reset key number.
      current_key_number = 0;
    }
    // Insert the key.
    memcpy(node_ptr + sizeof(NodeHeader) + current_key_number * key_tuple_id_pair_length_bytes_,
           entry_it->getKeyPtr(),
           key_length_bytes_);
    // Set the tuple_id.
    *reinterpret_cast<tuple_id*>(node_ptr
                                 + sizeof(NodeHeader)
                                 + current_key_number * key_tuple_id_pair_length_bytes_
                                 + key_length_bytes_) = entry_it->getTupleID();
    ++current_key_number;
  }
  // Reset num_keys for the last leaf.
  reinterpret_cast<NodeHeader*>(node_ptr)->num_keys = current_key_number;
  return current_node_number + 1;
}

void CSBTreeIndexSubBlock::generateEntryReferencesFromUntypedPtrs(
    vector<csbtree_internal::EntryReference> *entry_references) const {
  DEBUG_ASSERT(!key_is_composite_);
  DEBUG_ASSERT(tuple_store_supports_untyped_ptr_);
  DEBUG_ASSERT(entry_references->empty());

  tuple_id null_count = 0;
  if (tuple_store_.isPacked()) {
    for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
      const void *key_ptr = tuple_store_.getAttributeValue(tid, indexed_attribute_ids_.front());
      // Don't insert a NULL key.
      if (key_ptr != NULL) {
        entry_references->push_back(csbtree_internal::EntryReference(key_ptr, tid));
      } else {
        ++null_count;
      }
    }
  } else {
    for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
      if (tuple_store_.hasTupleWithID(tid)) {
        const void *key_ptr = tuple_store_.getAttributeValue(tid, indexed_attribute_ids_.front());
        // Don't insert a NULL key.
        if (key_ptr != NULL) {
          entry_references->push_back(csbtree_internal::EntryReference(key_ptr, tid));
        } else {
          ++null_count;
        }
      }
    }
  }

  DEBUG_ASSERT(static_cast<vector<csbtree_internal::EntryReference>::size_type>(tuple_store_.numTuples())
               == entry_references->size() + null_count);
}

void CSBTreeIndexSubBlock::generateEntryReferencesFromCompressedCodes(
    std::vector<csbtree_internal::CompressedEntryReference> *entry_references) const {
  DEBUG_ASSERT(key_is_compressed_);
  // TODO(chasseur): Handle NULL in compressed blocks (currently unsupported,
  // but may be in the future).
  DEBUG_ASSERT(!key_is_nullable_);
  DEBUG_ASSERT(entry_references->empty());

  DEBUG_ASSERT(tuple_store_.isCompressed());
  const CompressedTupleStorageSubBlock &compressed_tuple_store
      = static_cast<const CompressedTupleStorageSubBlock&>(tuple_store_);
  DEBUG_ASSERT(compressed_tuple_store.compressedBlockIsBuilt());
  DEBUG_ASSERT(compressed_tuple_store.compressedAttributeIsDictionaryCompressed(indexed_attribute_ids_.front())
               || compressed_tuple_store.compressedAttributeIsTruncationCompressed(indexed_attribute_ids_.front()));

  if (tuple_store_.isPacked()) {
    for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
      entry_references->push_back(csbtree_internal::CompressedEntryReference(
          compressed_tuple_store.compressedGetCode(tid, indexed_attribute_ids_.front()),
          tid));
    }
  } else {
    for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
      if (tuple_store_.hasTupleWithID(tid)) {
        entry_references->push_back(csbtree_internal::CompressedEntryReference(
            compressed_tuple_store.compressedGetCode(tid, indexed_attribute_ids_.front()),
            tid));
      }
    }
  }

  DEBUG_ASSERT(static_cast<vector<csbtree_internal::CompressedEntryReference>::size_type>(tuple_store_.numTuples())
               == entry_references->size());
}

void CSBTreeIndexSubBlock::generateEntryReferencesFromTypeInstances(
    vector<csbtree_internal::EntryReference> *entry_references,
    PtrVector<TypeInstance, false> *literal_typed_keys) const {
  DEBUG_ASSERT(!key_is_composite_);
  DEBUG_ASSERT(!tuple_store_supports_untyped_ptr_);
  DEBUG_ASSERT(entry_references->empty());
  DEBUG_ASSERT(literal_typed_keys->empty());

  tuple_id null_count = 0;
  if (tuple_store_.isPacked()) {
    for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
      ScopedPtr<TypeInstance> literal_key(tuple_store_.getAttributeValueTyped(tid, indexed_attribute_ids_.front()));
      // Don't insert a NULL key.
      if (!literal_key->isNull()) {
        literal_typed_keys->push_back(literal_key.release());
        entry_references->push_back(csbtree_internal::EntryReference(literal_typed_keys->back().getDataPtr(), tid));
      } else {
        ++null_count;
      }
    }
  } else {
    for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
      if (tuple_store_.hasTupleWithID(tid)) {
        ScopedPtr<TypeInstance> literal_key(tuple_store_.getAttributeValueTyped(tid, indexed_attribute_ids_.front()));
        // Don't insert a NULL key.
        if (!literal_key->isNull()) {
          literal_typed_keys->push_back(literal_key.release());
          entry_references->push_back(csbtree_internal::EntryReference(literal_typed_keys->back().getDataPtr(), tid));
        } else {
          ++null_count;
        }
      }
    }
  }

  DEBUG_ASSERT(static_cast<vector<csbtree_internal::EntryReference>::size_type>(tuple_store_.numTuples())
               == entry_references->size() + null_count);
}

void CSBTreeIndexSubBlock::generateEntryReferencesFromCompositeKeys(
    vector<csbtree_internal::EntryReference> *entry_references,
    PtrVector<ScopedBuffer, false> *composite_key_buffers) const {
  DEBUG_ASSERT(key_is_composite_);
  DEBUG_ASSERT(entry_references->empty());
  DEBUG_ASSERT(composite_key_buffers->empty());

  tuple_id null_count = 0;
  if (tuple_store_.isPacked()) {
    for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
      void *key_copy = makeKeyCopy(tid);
      // Don't insert a NULL key.
      if (key_copy != NULL) {
        composite_key_buffers->push_back(new ScopedBuffer(makeKeyCopy(tid)));
        entry_references->push_back(csbtree_internal::EntryReference(composite_key_buffers->back().get(), tid));
      } else {
        ++null_count;
      }
    }
  } else {
    for (tuple_id tid = 0; tid <= tuple_store_.getMaxTupleID(); ++tid) {
      if (tuple_store_.hasTupleWithID(tid)) {
        void *key_copy = makeKeyCopy(tid);
        // Don't insert a NULL key.
        if (key_copy != NULL) {
          composite_key_buffers->push_back(new ScopedBuffer(makeKeyCopy(tid)));
          entry_references->push_back(csbtree_internal::EntryReference(composite_key_buffers->back().get(), tid));
        } else {
          ++null_count;
        }
      }
    }
  }

  DEBUG_ASSERT(static_cast<vector<csbtree_internal::EntryReference>::size_type>(tuple_store_.numTuples())
               == entry_references->size() + null_count);
}

uint16_t CSBTreeIndexSubBlock::rebuildInternalLevel(const std::vector<int> &child_node_groups,
                                                    uint16_t last_child_num_nodes,
                                                    std::vector<int> *used_node_groups) {
  DEBUG_ASSERT(last_child_num_nodes > 0);
  DEBUG_ASSERT(!child_node_groups.empty());

  std::vector<int>::const_iterator last_it = child_node_groups.end() - 1;
  std::vector<int>::const_iterator next_to_last_it = child_node_groups.end();
  uint16_t next_to_last_child_num_nodes = max_keys_internal_ + 1;

  if (child_node_groups.size() > 1) {
    next_to_last_it -= 2;
    if (last_child_num_nodes < large_half_num_children_) {
      // Rebalance last node groups as needed.
      DEBUG_ASSERT(child_node_groups.size() > 1);
      next_to_last_child_num_nodes = rebalanceNodeGroups(*next_to_last_it,
                                                         child_node_groups.back(),
                                                         last_child_num_nodes);
      last_child_num_nodes = large_half_num_children_;
    }
  }

  int current_node_group_number = allocateNodeGroup();
  DEBUG_ASSERT(current_node_group_number >= 0);
  used_node_groups->push_back(current_node_group_number);

  uint16_t current_node_number = 0;
  for (std::vector<int>::const_iterator it = child_node_groups.begin();
       it != child_node_groups.end();
       ++it) {
    if (current_node_number == max_keys_internal_ + 1) {
      // Advance to next node group.
      current_node_group_number = allocateNodeGroup();
      DEBUG_ASSERT(current_node_group_number >= 0);
      used_node_groups->push_back(current_node_group_number);
      current_node_number = 0;
    }

    if (it == next_to_last_it) {
      makeInternalNode(*it,
                       next_to_last_child_num_nodes,
                       getNode(current_node_group_number, current_node_number));
    } else if (it == last_it) {
      makeInternalNode(*it,
                       last_child_num_nodes,
                       getNode(current_node_group_number, current_node_number));
    } else {
      makeInternalNode(*it,
                       max_keys_internal_ + 1,
                       getNode(current_node_group_number, current_node_number));
    }
    ++current_node_number;
  }

  return current_node_number;
}

uint16_t CSBTreeIndexSubBlock::rebalanceNodeGroups(const int full_node_group_number,
                                                   const int underfull_node_group_number,
                                                   const uint16_t underfull_num_nodes) {
  DEBUG_ASSERT(underfull_num_nodes < large_half_num_children_);

  const uint16_t shift_nodes = large_half_num_children_ - underfull_num_nodes;
  const uint16_t full_group_remaining_nodes = max_keys_internal_ + 1 - shift_nodes;
  // Shift existing nodes in underfull node group right.
  memmove(getNode(underfull_node_group_number, shift_nodes),
          getNode(underfull_node_group_number, 0),
          underfull_num_nodes * kCSBTreeNodeSizeBytes);
  // Copy nodes from full node group over.
  memcpy(getNode(underfull_node_group_number, 0),
         getNode(full_node_group_number, full_group_remaining_nodes),
         shift_nodes * kCSBTreeNodeSizeBytes);

  // If the rebalanced nodes are leaves, correct the sibling references.
  NodeHeader *full_group_last_header
      = static_cast<NodeHeader*>(getNode(full_node_group_number, full_group_remaining_nodes - 1));
  if (full_group_last_header->is_leaf) {
    full_group_last_header->node_group_reference = underfull_node_group_number;
    static_cast<NodeHeader*>(getNode(underfull_node_group_number, shift_nodes - 1))->node_group_reference
        = kNodeGroupNextLeaf;
  }

  return full_group_remaining_nodes;
}

void CSBTreeIndexSubBlock::makeInternalNode(const int child_node_group_number,
                                            const uint16_t num_children,
                                            void *node) {
  DEBUG_ASSERT(num_children > 1);
  // Setup header.
  static_cast<NodeHeader*>(node)->num_keys = num_children - 1;
  static_cast<NodeHeader*>(node)->is_leaf = false;
  static_cast<NodeHeader*>(node)->node_group_reference = child_node_group_number;

  // Fill in keys.
  char *key_ptr = static_cast<char*>(node) + sizeof(NodeHeader);
  for (uint16_t child_num = 1; child_num < num_children; ++child_num) {
    DEBUG_ASSERT(static_cast<const NodeHeader*>(getNode(child_node_group_number, child_num))->num_keys > 0);
    // NOTE(chasseur): We could simply remember the least keys of all nodes
    // generated in the previous pass, but that is a time/space tradeoff
    // which is probably not be worth it.
    memcpy(key_ptr,
           getLeastKey(getNode(child_node_group_number, child_num)),
           key_length_bytes_);
    key_ptr += key_length_bytes_;
  }
}

int CSBTreeIndexSubBlock::allocateNodeGroup() {
  if (num_free_node_groups_ == 0) {
    // No more node groups are available.
    return kNodeGroupNone;
  } else {
    DEBUG_ASSERT(!node_group_used_bitmap_->getBit(next_free_node_group_));
    // Return the next free node group.
    int retval = next_free_node_group_;
    // Mark this node group as used and decrement the count of free node
    // groups.
    node_group_used_bitmap_->setBit(retval, true);
    --num_free_node_groups_;
    // If there are still free node groups remaining, locate the next one.
    if (num_free_node_groups_) {
      next_free_node_group_ = node_group_used_bitmap_->firstZero(retval + 1);
      DEBUG_ASSERT(static_cast<size_t>(next_free_node_group_) < node_group_used_bitmap_->size());
      return retval;
    } else {
      next_free_node_group_ = kNodeGroupNone;
    }
    return retval;
  }
}

void CSBTreeIndexSubBlock::deallocateNodeGroup(const int node_group_number) {
  DEBUG_ASSERT(node_group_number >= 0);
  DEBUG_ASSERT(static_cast<size_t>(node_group_number) < node_group_used_bitmap_->size());
  DEBUG_ASSERT(node_group_used_bitmap_->getBit(node_group_number));

  node_group_used_bitmap_->setBit(node_group_number, false);
  ++num_free_node_groups_;
  if (node_group_number < next_free_node_group_) {
    next_free_node_group_ = node_group_number;
  }
}

}  // namespace quickstep
