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

#include "storage/BasicColumnStoreTupleStorageSubBlock.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "expressions/ComparisonPredicate.hpp"
#include "expressions/Predicate.hpp"
#include "expressions/Scalar.hpp"
#include "storage/ColumnStoreUtil.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/StorageErrors.hpp"
#include "types/Comparison.hpp"
#include "types/Tuple.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"
#include "utility/ScopedBuffer.hpp"
#include "utility/ScopedPtr.hpp"

using std::lower_bound;
using std::memcpy;
using std::memmove;
using std::size_t;
using std::sort;
using std::upper_bound;
using std::vector;

using quickstep::column_store_util::ColumnStripeIterator;
using quickstep::column_store_util::SortColumnPredicateEvaluator;

namespace quickstep {

// Hide these helpers in an anonymous namespace:
namespace {

class SortColumnValueReference {
 public:
  SortColumnValueReference(const void *value, const tuple_id tuple)
      : value_(value), tuple_(tuple) {
  }

  inline const void* getValuePtr() const {
    return value_;
  }

  inline tuple_id getTupleID() const {
    return tuple_;
  }

 private:
  const void *value_;
  tuple_id tuple_;
};

class SortColumnValueReferenceComparator {
 public:
  explicit SortColumnValueReferenceComparator(const UncheckedComparator &comparator)
      : internal_comparator_(comparator) {
  }

  inline bool operator() (const SortColumnValueReference &left, const SortColumnValueReference &right) const {
    return internal_comparator_.compareDataPtrs(left.getValuePtr(), right.getValuePtr());
  }

 private:
  const UncheckedComparator &internal_comparator_;
};

}  // anonymous namespace


BasicColumnStoreTupleStorageSubBlock::BasicColumnStoreTupleStorageSubBlock(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description,
    const bool new_block,
    void *sub_block_memory,
    const std::size_t sub_block_memory_size)
    : TupleStorageSubBlock(relation,
                           description,
                           new_block,
                           sub_block_memory,
                           sub_block_memory_size),
      sorted_(true) {
  if (!DescriptionIsValid(relation_, description_)) {
    FATAL_ERROR("Attempted to construct a BasicColumnStoreTupleStorageSubBlock from an invalid description.");
  }

  sort_column_id_ = description_.GetExtension(BasicColumnStoreTupleStorageSubBlockDescription::sort_attribute_id);
  const Type &sort_column_type = relation_.getAttributeById(sort_column_id_).getType();
  sort_column_comparator_.reset(
      Comparison::GetComparison(Comparison::kLess).makeUncheckedComparatorForTypes(sort_column_type,
                                                                                   sort_column_type));

  if (sub_block_memory_size < sizeof(BasicColumnStoreHeader)) {
    throw BlockMemoryTooSmall("BasicColumnStoreTupleStorageSubBlock", sub_block_memory_size_);
  }

  // Determine the amount of tuples this sub-block can hold.
  max_tuples_ = (sub_block_memory_size_ - sizeof(BasicColumnStoreHeader)) / relation_.getFixedByteLength();
  if (max_tuples_ == 0) {
    throw BlockMemoryTooSmall("BasicColumnStoreTupleStorageSubBlock", sub_block_memory_size_);
  }

  // Determine column stripe locations.
  column_stripes_.resize(relation_.getMaxAttributeId() +  1, NULL);
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    column_stripes_[attr_it->getID()] = static_cast<char*>(sub_block_memory_)
                                        + sizeof(BasicColumnStoreHeader)
                                        + max_tuples_ * relation_.getFixedLengthAttributeOffset(attr_it->getID());
  }

  if (new_block) {
    getHeaderPtr()->num_tuples = 0;
  }
}

bool BasicColumnStoreTupleStorageSubBlock::DescriptionIsValid(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description) {
  // Make sure description is initialized and specifies BasicColumnStore.
  if (!description.IsInitialized()) {
    return false;
  }
  if (description.sub_block_type() != TupleStorageSubBlockDescription::BASIC_COLUMN_STORE) {
    return false;
  }
  // Make sure a sort_attribute_id is specified.
  if (!description.HasExtension(BasicColumnStoreTupleStorageSubBlockDescription::sort_attribute_id)) {
    return false;
  }

  // Make sure relation is not variable-length and contains no nullable attributes.
  if (relation.isVariableLength()) {
    return false;
  }
  if (relation.hasNullableAttributes()) {
    return false;
  }

  // Check that the specified sort attribute exists and can be ordered by LessComparison.
  attribute_id sort_attribute_id = description.GetExtension(
      BasicColumnStoreTupleStorageSubBlockDescription::sort_attribute_id);
  if (!relation.hasAttributeWithId(sort_attribute_id)) {
    return false;
  }
  const Type &sort_attribute_type = relation.getAttributeById(sort_attribute_id).getType();
  if (!Comparison::GetComparison(Comparison::kLess).canCompareTypes(sort_attribute_type,
                                                                    sort_attribute_type)) {
    return false;
  }

  return true;
}

std::size_t BasicColumnStoreTupleStorageSubBlock::EstimateBytesPerTuple(
    const CatalogRelation &relation,
    const TupleStorageSubBlockDescription &description) {
  DEBUG_ASSERT(DescriptionIsValid(relation, description));

  return relation.getFixedByteLength();
}

TupleStorageSubBlock::InsertResult BasicColumnStoreTupleStorageSubBlock::insertTuple(
    const Tuple &tuple,
    const AllowedTypeConversion atc) {
#ifdef QUICKSTEP_DEBUG
  paranoidInsertTypeCheck(tuple, atc);
#endif
  if (!hasSpaceToInsert(1)) {
    return InsertResult(-1, false);
  }

  // Binary search for the appropriate insert location.
  tuple_id insert_position = 0;
  {
    // Coerce the sort column value from 'tuple' if necessary.
    const void *sort_column_value = NULL;
    ScopedPtr<TypeInstance> converted_sort_column_value;
    if (atc == kNone) {
      sort_column_value = tuple.getAttributeValue(sort_column_id_).getDataPtr();
    } else {
      if (relation_.getAttributeById(sort_column_id_).getType().equals(
          tuple.getAttributeValue(sort_column_id_).getType())) {
        sort_column_value = tuple.getAttributeValue(sort_column_id_).getDataPtr();
      } else {
        converted_sort_column_value.reset(tuple.getAttributeValue(sort_column_id_)
            .makeCoercedCopy(relation_.getAttributeById(sort_column_id_).getType()));
        sort_column_value = converted_sort_column_value->getDataPtr();
      }
    }

    // Use std::upper_bound, which is a binary search when provided with
    // random-access iterators.
    ColumnStripeIterator insert_position_it
        = upper_bound(ColumnStripeIterator(column_stripes_[sort_column_id_],
                                           relation_.getAttributeById(sort_column_id_).getType().maximumByteLength(),
                                           0),
                      ColumnStripeIterator(column_stripes_[sort_column_id_],
                                           relation_.getAttributeById(sort_column_id_).getType().maximumByteLength(),
                                           getHeaderPtr()->num_tuples),
                      sort_column_value,
                      STLUncheckedComparatorWrapper(*sort_column_comparator_));
    insert_position = insert_position_it.getTuplePosition();
  }

  InsertResult retval(insert_position, insert_position != getHeaderPtr()->num_tuples);
  insertTupleAtPosition(tuple, atc, insert_position);

  return retval;
}

bool BasicColumnStoreTupleStorageSubBlock::insertTupleInBatch(const Tuple &tuple,
                                                              const AllowedTypeConversion atc) {
#ifdef QUICKSTEP_DEBUG
  paranoidInsertTypeCheck(tuple, atc);
#endif
  if (!hasSpaceToInsert(1)) {
    return false;
  }

  insertTupleAtPosition(tuple, atc, getHeaderPtr()->num_tuples);
  sorted_ = false;
  return true;
}

const void* BasicColumnStoreTupleStorageSubBlock::getAttributeValue(const tuple_id tuple,
                                                                    const attribute_id attr) const {
  DEBUG_ASSERT(hasTupleWithID(tuple));
  DEBUG_ASSERT(relation_.hasAttributeWithId(attr));
  return static_cast<const char*>(column_stripes_[attr])
         + (tuple * relation_.getAttributeById(attr).getType().maximumByteLength());
}

TypeInstance* BasicColumnStoreTupleStorageSubBlock::getAttributeValueTyped(const tuple_id tuple,
                                                                           const attribute_id attr) const {
  return relation_.getAttributeById(attr).getType().makeReferenceTypeInstance(getAttributeValue(tuple, attr));
}

bool BasicColumnStoreTupleStorageSubBlock::deleteTuple(const tuple_id tuple) {
  DEBUG_ASSERT(hasTupleWithID(tuple));

  BasicColumnStoreHeader *header = getHeaderPtr();

  if (tuple == header->num_tuples - 1) {
    // If deleting the last tuple, simply truncate.
    --(header->num_tuples);
    return false;
  } else {
    // Pack each column stripe.
    shiftTuples(tuple, tuple + 1, header->num_tuples - tuple - 1);
    --(header->num_tuples);
    return true;
  }
}

TupleIdSequence* BasicColumnStoreTupleStorageSubBlock::getMatchesForPredicate(const Predicate *predicate) const {
  if (predicate == NULL) {
    // No predicate, so pass through to base version to get all tuples.
    return TupleStorageSubBlock::getMatchesForPredicate(predicate);
  }

  TupleIdSequence *matches = SortColumnPredicateEvaluator::EvaluatePredicateForUncompressedSortColumn(
      *predicate,
      relation_,
      sort_column_id_,
      column_stripes_[sort_column_id_],
      getHeaderPtr()->num_tuples);

  if (matches == NULL) {
    return TupleStorageSubBlock::getMatchesForPredicate(predicate);
  } else {
    return matches;
  }
}

void BasicColumnStoreTupleStorageSubBlock::insertTupleAtPosition(
    const Tuple &tuple,
    const AllowedTypeConversion atc,
    const tuple_id position) {
  DEBUG_ASSERT(hasSpaceToInsert(1));
  DEBUG_ASSERT(position >= 0);
  DEBUG_ASSERT(position < max_tuples_);

  if (position != getHeaderPtr()->num_tuples) {
    // If not inserting in the last position, shift subsequent tuples back.
    shiftTuples(position + 1, position, getHeaderPtr()->num_tuples - position);
  }

  // Copy attribute values into place in the column stripes.
  Tuple::const_iterator value_it = tuple.begin();
  CatalogRelation::const_iterator attr_it = relation_.begin();

  switch (atc) {
    case kNone:
      while (value_it != tuple.end()) {
        value_it->copyInto(static_cast<char*>(column_stripes_[attr_it->getID()])
                           + position * attr_it->getType().maximumByteLength());

        ++value_it;
        ++attr_it;
      }
      break;
    case kSafe:
    case kUnsafe:
      while (value_it != tuple.end()) {
        if (value_it->getType().equals(attr_it->getType())) {
          value_it->copyInto(static_cast<char*>(column_stripes_[attr_it->getID()])
                             + position * attr_it->getType().maximumByteLength());
        } else {
          ScopedPtr<TypeInstance> converted_temp(value_it->makeCoercedCopy(attr_it->getType()));
          converted_temp->copyInto(static_cast<char*>(column_stripes_[attr_it->getID()])
                                   + position * attr_it->getType().maximumByteLength());
        }

        ++value_it;
        ++attr_it;
      }
      break;
  }

  ++(getHeaderPtr()->num_tuples);
}

void BasicColumnStoreTupleStorageSubBlock::shiftTuples(
    const tuple_id dest_position,
    const tuple_id src_tuple,
    const tuple_id num_tuples) {
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    size_t attr_length = attr_it->getType().maximumByteLength();
    memmove(static_cast<char*>(column_stripes_[attr_it->getID()]) + dest_position * attr_length,
            static_cast<const char*>(column_stripes_[attr_it->getID()]) + src_tuple * attr_length,
            attr_length * num_tuples);
  }
}

BasicColumnStoreTupleStorageSubBlock::BasicColumnStoreHeader*
    BasicColumnStoreTupleStorageSubBlock::getHeaderPtr() {
  return static_cast<BasicColumnStoreHeader*>(sub_block_memory_);
}

const BasicColumnStoreTupleStorageSubBlock::BasicColumnStoreHeader*
    BasicColumnStoreTupleStorageSubBlock::getHeaderPtr() const {
  return static_cast<const BasicColumnStoreHeader*>(sub_block_memory_);
}

// NOTE(chasseur): This implementation uses out-of-band memory up to the
// total size of tuples contained in this sub-block. It could be done with
// less memory, although the implementation would be more complex.
bool BasicColumnStoreTupleStorageSubBlock::rebuildInternal() {
  const tuple_id num_tuples = getHeaderPtr()->num_tuples;
  // Immediately return if 1 or 0 tuples.
  if (num_tuples <= 1) {
    sorted_ = true;
    return false;
  }

  // Determine the properly-sorted order of tuples.
  vector<SortColumnValueReference> sort_column_values;
  for (tuple_id tid = 0; tid < num_tuples; ++tid) {
    sort_column_values.push_back(SortColumnValueReference(getAttributeValue(tid, sort_column_id_),
                                                          tid));
  }
  sort(sort_column_values.begin(),
       sort_column_values.end(),
       SortColumnValueReferenceComparator(*sort_column_comparator_));

  // If a prefix of the total order of tuples is already sorted, don't bother
  // copying it around.
  tuple_id ordered_prefix_tuples = 0;
  for (vector<SortColumnValueReference>::const_iterator it = sort_column_values.begin();
       it != sort_column_values.end();
       ++it) {
    if (it->getTupleID() != ordered_prefix_tuples) {
      break;
    }
    ++ordered_prefix_tuples;
  }

  if (ordered_prefix_tuples == num_tuples) {
    // Already sorted.
    sorted_ = true;
    return false;
  }

  // Allocate buffers for each resorted column stripe which can hold exactly as
  // many values as needed.
  PtrVector<ScopedBuffer, true> column_stripe_buffers;
  for (attribute_id stripe_id = 0; stripe_id <= relation_.getMaxAttributeId(); ++stripe_id) {
    if (relation_.hasAttributeWithId(stripe_id)) {
      column_stripe_buffers.push_back(
          new ScopedBuffer((num_tuples - ordered_prefix_tuples)
                           * relation_.getAttributeById(stripe_id).getType().maximumByteLength()));
    } else {
      column_stripe_buffers.push_back(NULL);
    }
  }

  // Copy attribute values into the column stripe buffers in properly-sorted
  // order.
  for (tuple_id unordered_tuple_num = 0;
       unordered_tuple_num < num_tuples - ordered_prefix_tuples;
       ++unordered_tuple_num) {
    for (CatalogRelation::const_iterator attr_it = relation_.begin();
         attr_it != relation_.end();
         ++attr_it) {
      size_t attr_length = attr_it->getType().maximumByteLength();
      memcpy(static_cast<char*>(column_stripe_buffers[attr_it->getID()].get())
                 + unordered_tuple_num * attr_length,
             getAttributeValue(sort_column_values[ordered_prefix_tuples + unordered_tuple_num].getTupleID(),
                               attr_it->getID()),
             attr_length);
    }
  }

  // Overwrite the unsorted tails of the column stripes in this block with the
  // sorted values from the buffers.
  for (CatalogRelation::const_iterator attr_it = relation_.begin();
       attr_it != relation_.end();
       ++attr_it) {
    size_t attr_length = attr_it->getType().maximumByteLength();
    memcpy(static_cast<char*>(column_stripes_[attr_it->getID()]) + ordered_prefix_tuples * attr_length,
           column_stripe_buffers[attr_it->getID()].get(),
           (num_tuples - ordered_prefix_tuples) * attr_length);
  }

  sorted_ = true;
  return true;
}

}  // namespace quickstep
