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

// On windows, rand() only generates 16-bit ints, so we use rand_s() instead.
#ifdef _WIN32
#define _CRT_RAND_S
#endif

#include "experiments/storage_explorer/DataGenerator.hpp"

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <vector>

#include "catalog/CatalogAttribute.hpp"
#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "expressions/ComparisonPredicate.hpp"
#include "expressions/Predicate.hpp"
#include "expressions/TrivialPredicates.hpp"
#include "expressions/Scalar.hpp"
#include "storage/InsertDestination.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "storage/TupleStorageSubBlock.hpp"
#include "types/CharType.hpp"
#include "types/Comparison.hpp"
#include "types/IntType.hpp"
#include "types/Tuple.hpp"
#include "types/Type.hpp"
#include "types/TypeInstance.hpp"
#include "utility/ScopedPtr.hpp"

using std::cerr;
using std::fabs;
using std::ostringstream;
using std::pow;
using std::rand;
using std::size_t;
using std::srand;
using std::vector;

namespace quickstep {
namespace storage_explorer {

void DataGenerator::generateData(const std::size_t num_tuples,
                                 InsertDestination *destination,
                                 bool defer_rebuild) const {
  const CatalogRelation &relation = destination->getRelation();
  StorageBlock *current_block = destination->getBlockForInsertion();

  for (size_t tuple_num = 0; tuple_num < num_tuples; ++tuple_num) {
    ScopedPtr<Tuple> tuple(new Tuple(relation));
    generateValuesInTuple(tuple.get());

    while (!current_block->insertTupleInBatch(*tuple, kNone)) {
      // Block is full, so put it into "correct" state:
      if (!defer_rebuild) {
        if (!current_block->rebuild()) {
          FATAL_ERROR("DataGenerator::generateData() failed to rebuild a full StorageBlock.");
        }
      }

      destination->returnBlock(current_block, true);
      current_block = destination->getBlockForInsertion();
    }
  }

  // Rebuild the last block:
  if (!defer_rebuild) {
    if (!current_block->rebuild()) {
      FATAL_ERROR("DataGenerator::generateData() failed to rebuild a full StorageBlock.");
    }
  }
  destination->returnBlock(current_block, false);
}

void DataGenerator::generateDataIntoPartition(const std::size_t total_num_tuples,
                                              const attribute_id partition_value_column,
                                              const std::size_t partition_num,
                                              const std::size_t total_partitions,
                                              InsertDestination *destination) const {
  size_t gen_tuples = total_num_tuples / total_partitions;

  const CatalogRelation &relation = destination->getRelation();
  StorageBlock *current_block = destination->getBlockForInsertion();

  for (size_t tuple_num = 0; tuple_num < gen_tuples; ++tuple_num) {
    ScopedPtr<Tuple> tuple(new Tuple(relation));
    generateValuesInTupleForPartition(tuple.get(),
                                      partition_value_column,
                                      partition_num,
                                      total_partitions);

    while (!current_block->insertTupleInBatch(*tuple, kNone)) {
      // Block is full, so put it into "correct" state:
      if (!current_block->rebuild()) {
        FATAL_ERROR("DataGenerator::generateData() failed to rebuild a full StorageBlock.");
      }

      destination->returnBlock(current_block, true);
      current_block = destination->getBlockForInsertion();
    }
  }

  // Rebuild the last block:
  if (!current_block->rebuild()) {
    FATAL_ERROR("DataGenerator::generateData() failed to rebuild a full StorageBlock.");
  }
  destination->returnBlock(current_block, false);
}

void DataGenerator::generateDataIntoTupleStore(const std::size_t num_tuples,
                                               TupleStorageSubBlock *tuple_store) const {
  const CatalogRelation &relation = tuple_store->getRelation();
  for (size_t tuple_num = 0; tuple_num < num_tuples; ++tuple_num) {
    ScopedPtr<Tuple> tuple(new Tuple(relation));
    generateValuesInTuple(tuple.get());

    if (!tuple_store->insertTupleInBatch(*tuple, kNone)) {
      FATAL_ERROR("DataGenerator::generateDataIntoTupleStore() ran out of space in tuple store.");
    }
  }
}

StorageBlockLayout* DataGenerator::generateColumnstoreLayout(
    const CatalogRelation &relation,
    const std::size_t num_slots,
    const attribute_id column_store_sort_column,
    const std::vector<attribute_id> &index_on_columns) const {
  ScopedPtr<StorageBlockLayout> layout(new StorageBlockLayout(relation));
  StorageBlockLayoutDescription *layout_desc = layout->getDescriptionMutable();

  layout_desc->set_num_slots(num_slots);

  layout_desc->mutable_tuple_store_description()
      ->set_sub_block_type(TupleStorageSubBlockDescription::BASIC_COLUMN_STORE);
  layout_desc->mutable_tuple_store_description()
      ->SetExtension(BasicColumnStoreTupleStorageSubBlockDescription::sort_attribute_id,
                     column_store_sort_column);

  for (vector<attribute_id>::const_iterator it = index_on_columns.begin();
       it != index_on_columns.end();
       ++it) {
    IndexSubBlockDescription *index_desc = layout_desc->add_index_description();
    index_desc->set_sub_block_type(IndexSubBlockDescription::CSB_TREE);
    index_desc->AddExtension(CSBTreeIndexSubBlockDescription::indexed_attribute_id, *it);
  }

  layout->finalize();
  return layout.release();
}

StorageBlockLayout* DataGenerator::generateRowstoreLayout(
    const CatalogRelation &relation,
    const std::size_t num_slots,
    const std::vector<attribute_id> &index_on_columns) const {
  ScopedPtr<StorageBlockLayout> layout(new StorageBlockLayout(relation));
  StorageBlockLayoutDescription *layout_desc = layout->getDescriptionMutable();

  layout_desc->set_num_slots(num_slots);

  layout_desc->mutable_tuple_store_description()
      ->set_sub_block_type(TupleStorageSubBlockDescription::PACKED_ROW_STORE);

  for (vector<attribute_id>::const_iterator it = index_on_columns.begin();
       it != index_on_columns.end();
       ++it) {
    IndexSubBlockDescription *index_desc = layout_desc->add_index_description();
    index_desc->set_sub_block_type(IndexSubBlockDescription::CSB_TREE);
    index_desc->AddExtension(CSBTreeIndexSubBlockDescription::indexed_attribute_id, *it);
  }

  layout->finalize();
  return layout.release();
}

StorageBlockLayout* DataGenerator::generateCompressedColumnstoreLayout(
    const CatalogRelation &relation,
    const std::size_t num_slots,
    const attribute_id column_store_sort_column,
    const std::vector<attribute_id> &index_on_columns) const {
  ScopedPtr<StorageBlockLayout> layout(new StorageBlockLayout(relation));
  StorageBlockLayoutDescription *layout_desc = layout->getDescriptionMutable();

  layout_desc->set_num_slots(num_slots);

  layout_desc->mutable_tuple_store_description()
      ->set_sub_block_type(TupleStorageSubBlockDescription::COMPRESSED_COLUMN_STORE);
  layout_desc->mutable_tuple_store_description()->SetExtension(
      CompressedColumnStoreTupleStorageSubBlockDescription::sort_attribute_id,
      column_store_sort_column);

  // Attempt to compress all columns.
  for (CatalogRelation::const_iterator attr_it = relation.begin();
       attr_it != relation.end();
       ++attr_it) {
    layout_desc->mutable_tuple_store_description()->AddExtension(
        CompressedColumnStoreTupleStorageSubBlockDescription::compressed_attribute_id,
        attr_it->getID());
  }

  for (vector<attribute_id>::const_iterator it = index_on_columns.begin();
       it != index_on_columns.end();
       ++it) {
    IndexSubBlockDescription *index_desc = layout_desc->add_index_description();
    index_desc->set_sub_block_type(IndexSubBlockDescription::CSB_TREE);
    index_desc->AddExtension(CSBTreeIndexSubBlockDescription::indexed_attribute_id, *it);
  }

  layout->finalize();
  return layout.release();
}

StorageBlockLayout* DataGenerator::generateCompressedRowstoreLayout(
    const CatalogRelation &relation,
    const std::size_t num_slots,
    const std::vector<attribute_id> &index_on_columns) const {
  ScopedPtr<StorageBlockLayout> layout(new StorageBlockLayout(relation));
  StorageBlockLayoutDescription *layout_desc = layout->getDescriptionMutable();

  layout_desc->set_num_slots(num_slots);

  layout_desc->mutable_tuple_store_description()
      ->set_sub_block_type(TupleStorageSubBlockDescription::COMPRESSED_PACKED_ROW_STORE);

  // Attempt to compress all columns.
  for (CatalogRelation::const_iterator attr_it = relation.begin();
       attr_it != relation.end();
       ++attr_it) {
    layout_desc->mutable_tuple_store_description()->AddExtension(
        CompressedPackedRowStoreTupleStorageSubBlockDescription::compressed_attribute_id,
        attr_it->getID());
  }

  for (vector<attribute_id>::const_iterator it = index_on_columns.begin();
       it != index_on_columns.end();
       ++it) {
    IndexSubBlockDescription *index_desc = layout_desc->add_index_description();
    index_desc->set_sub_block_type(IndexSubBlockDescription::CSB_TREE);
    index_desc->AddExtension(CSBTreeIndexSubBlockDescription::indexed_attribute_id, *it);
  }

  layout->finalize();
  return layout.release();
}

void DataGenerator::AppendValueToTuple(Tuple* tuple, TypeInstance* value) {
  tuple->append(value);
}

void DataGenerator::SeedRandom() {
  srand(kRandomSeed);
}

int DataGenerator::GenerateRandomInt(const int range) {
#ifdef _WIN32
  // Workaround for windows, where rand() only produces 16-bit values.
  unsigned int rmax = (UINT_MAX / range) * range;
  unsigned int rand_val;
  if (rand_s(&rand_val)) {
    FATAL_ERROR("Call to windows rand_s() failed\n");
  }
  while (rand_val >= rmax) {
    if (rand_s(&rand_val)) {
      FATAL_ERROR("Call to windows rand_s() failed\n");
    }
  }
  return rand_val % range;
#else
  int rmax = (RAND_MAX / range) * range;
  int rand_val = rand();
  while (rand_val >= rmax) {
    rand_val = rand();
  }
  return rand_val % range;
#endif
}

Predicate* NumericDataGenerator::generatePredicate(const CatalogRelation &relation,
                                                   const attribute_id select_column,
                                                   const float selectivity) const {
  assert(0.0 <= selectivity);
  assert(selectivity <= 1.0);

  if (selectivity == 0.0) {
    return new FalsePredicate();
  }

  if (selectivity == 1.0) {
    return new TruePredicate();
  }

  int threshold_value = (1.0 - selectivity) * column_ranges_[select_column];

  // Columns with narrower ranges of values have a hard time getting exactly
  // the requested selectivity, so warn if we can't get within 1%.
  float actual_selectivity = 1.0 - (static_cast<float>(threshold_value)
                                    / static_cast<float>(column_ranges_[select_column]));
  if (fabs(actual_selectivity - selectivity) / selectivity > 0.05) {
    cerr << "WARNING: generatePredicate() invoked to generate a predicate with selectivity of "
         << selectivity
         << ", but actual predicate has selectivity "
         << actual_selectivity
         << "\n";
  }

  ScalarAttribute *scalar_attribute = new ScalarAttribute(relation.getAttributeById(select_column));
  ScalarLiteral *scalar_literal
      = new ScalarLiteral(IntType::InstanceNonNullable().makeLiteralTypeInstance(threshold_value));
  return new ComparisonPredicate(Comparison::GetComparison(Comparison::kEqual),
                                 scalar_attribute,
                                 scalar_literal);
}

void NumericDataGenerator::generateValuesInTuple(Tuple* tuple) const {
  for (vector<int>::const_iterator range_it = column_ranges_.begin();
       range_it != column_ranges_.end();
       ++range_it) {
    AppendValueToTuple(tuple,
                       IntType::InstanceNonNullable().makeLiteralTypeInstance(GenerateRandomInt(*range_it)));
  }
}

void NumericDataGenerator::generateValuesInTupleForPartition(Tuple* tuple,
                                                             const attribute_id partition_value_column,
                                                             const std::size_t partition_num,
                                                             const std::size_t total_partitions) const {
  for (attribute_id current_attr = 0;
       static_cast<std::vector<int>::size_type>(current_attr) < column_ranges_.size();
       ++current_attr) {
    if (current_attr == partition_value_column) {
      int range_width = column_ranges_[current_attr] / total_partitions;
      int range_offset = range_width * partition_num;
      AppendValueToTuple(tuple,
                         IntType::InstanceNonNullable().makeLiteralTypeInstance(
                             range_offset + GenerateRandomInt(range_width)));
    } else {
      AppendValueToTuple(tuple,
                         IntType::InstanceNonNullable().makeLiteralTypeInstance(
                             GenerateRandomInt(column_ranges_[current_attr])));
    }
  }
}

CatalogRelation* NumericDataGenerator::generateRelationHelper(const std::string &relation_name) const {
  ScopedPtr<CatalogRelation> relation(new CatalogRelation(NULL, relation_name));

  for (unsigned int column_num = 0; column_num < column_ranges_.size(); ++column_num) {
    ostringstream attr_name;
    attr_name << "intcol" << column_num;

    ScopedPtr<CatalogAttribute> attribute(new CatalogAttribute(relation.get(),
                                                               attr_name.str(),
                                                               Type::GetType(Type::kInt, false)));
    relation->addAttribute(attribute.release());
  }

  return relation.release();
}

NarrowEDataGenerator::NarrowEDataGenerator() {
  for (int column_num = 0; column_num < 10; ++column_num) {
    column_ranges_.push_back(static_cast<int>(pow(static_cast<double>(2),
                                                  static_cast<double>(column_num + 1) * 2.7)));
  }
}

CatalogRelation* NarrowEDataGenerator::generateRelation() const {
  return generateRelationHelper("NarrowE");
}

WideEDataGenerator::WideEDataGenerator() {
  for (int column_num = 0; column_num < 50; ++column_num) {
    column_ranges_.push_back(static_cast<int>(pow(static_cast<double>(2),
                                                  4.0 + static_cast<double>(column_num + 1) * 0.46)));
  }
}

CatalogRelation* WideEDataGenerator::generateRelation() const {
  return generateRelationHelper("WideE");
}

NarrowUDataGenerator::NarrowUDataGenerator() {
  for (int column_num = 0; column_num < 10; ++column_num) {
    column_ranges_.push_back(100000000);
  }
}

CatalogRelation* NarrowUDataGenerator::generateRelation() const {
  return generateRelationHelper("NarrowU");
}

CatalogRelation* StringsDataGenerator::generateRelation() const {
  ScopedPtr<CatalogRelation> relation(new CatalogRelation(NULL, "Strings"));

  for (int column_num = 0; column_num < 10; ++column_num) {
    ostringstream attr_name;
    attr_name << "stringcol" << column_num;

    ScopedPtr<CatalogAttribute> attribute(new CatalogAttribute(relation.get(),
                                                               attr_name.str(),
                                                               Type::GetType(Type::kChar, 20, false)));
    relation->addAttribute(attribute.release());
  }

  return relation.release();
}

Predicate* StringsDataGenerator::generatePredicate(const CatalogRelation &relation,
                                                   const attribute_id select_column,
                                                   const float selectivity) const {
  assert(0.0 <= selectivity);
  assert(selectivity <= 1.0);

  if (selectivity == 0.0) {
    return new FalsePredicate();
  }

  if (selectivity == 1.0) {
    return new TruePredicate();
  }

  int threshold_value = (1.0 - selectivity) * kFiveCharInt;

  // Columns with narrower ranges of values have a hard time getting exactly
  // the requested selectivity, so warn if we can't get within 1%.
  float actual_selectivity = 1.0 - (static_cast<float>(threshold_value)
                                    / static_cast<float>(kFiveCharInt));
  if (fabs(actual_selectivity - selectivity) / selectivity > 0.01) {
    cerr << "WARNING: generatePredicate() invoked to generate a predicate with selectivity of "
         << selectivity
         << ", but actual predicate has selectivity "
         << actual_selectivity
         << "\n";
  }

  char literal_buffer[6];
  literal_buffer[5] = '\0';
  GenerateFiveChars(threshold_value, literal_buffer);

  ScalarAttribute *scalar_attribute = new ScalarAttribute(relation.getAttributeById(select_column));
  ScalarLiteral *scalar_literal
      = new ScalarLiteral(CharType::InstanceNonNullable(5).makeLiteralTypeInstance(literal_buffer));
  return new ComparisonPredicate(Comparison::GetComparison(Comparison::kGreaterOrEqual),
                                 scalar_attribute,
                                 scalar_literal);
}

void StringsDataGenerator::generateValuesInTuple(Tuple* tuple) const {
  for (int column_num = 0; column_num < 10; ++column_num) {
    char literal_buffer[21];
    literal_buffer[20] = '\0';
    for (int stride = 0; stride < 4; ++stride) {
      GenerateFiveChars(GenerateRandomInt(kFiveCharInt),
                        literal_buffer + (stride * 5));
    }
    AppendValueToTuple(tuple,
                       CharType::InstanceNonNullable(20).makeLiteralTypeInstance(literal_buffer));
  }
}

void StringsDataGenerator::generateValuesInTupleForPartition(Tuple* tuple,
                                                             const attribute_id partition_value_column,
                                                             const std::size_t partition_num,
                                                             const std::size_t total_partitions) const {
  for (int column_num = 0; column_num < 10; ++column_num) {
    char literal_buffer[21];
    literal_buffer[20] = '\0';
    for (int stride = 0; stride < 4; ++stride) {
      if ((column_num == partition_value_column) && (stride == 0)) {
        int range_width = kFiveCharInt / total_partitions;
        int range_offset = range_width * partition_num;
        GenerateFiveChars(GenerateRandomInt(range_width) + range_offset,
                          literal_buffer + (stride * 5));
      } else {
        GenerateFiveChars(GenerateRandomInt(kFiveCharInt),
                          literal_buffer + (stride * 5));
      }
    }
    AppendValueToTuple(tuple,
                       CharType::InstanceNonNullable(20).makeLiteralTypeInstance(literal_buffer));
  }
}

void StringsDataGenerator::GenerateFiveChars(const int mapped_int, char *dest) {
  const int six_bits_mask = 63;
  for (int pos = 0; pos < 5; ++pos) {
    unsigned char idx = (mapped_int >> ((4 - pos) * 6)) & six_bits_mask;
    if (idx == 0) {
      // A space.
      dest[pos] = ' ';
    } else if (idx == 1) {
      // A period.
      dest[pos] = '.';
    } else if (idx < 12) {
      // A digit.
      dest[pos] = 48 + idx - 2;
    } else if (idx < 38) {
      // A capital letter.
      dest[pos] = 65 + idx - 12;
    } else {
      // A lower-case letter.
      dest[pos] = 97 + idx - 38;
    }
  }
}

}  // namespace storage_explorer
}  // namespace quickstep
