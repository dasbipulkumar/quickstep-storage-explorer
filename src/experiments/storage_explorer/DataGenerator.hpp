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

#ifndef QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_DATA_GENERATOR_HPP_
#define QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_DATA_GENERATOR_HPP_

#include <cstddef>
#include <string>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogRelation;
class InsertDestination;
class Predicate;
class StorageBlockLayout;
class Tuple;
class TupleStorageSubBlock;
class TypeInstance;

namespace storage_explorer {

/**
 * @brief Object with methods to create a relation for test data, generate
 *        various types of StorageBlockLayouts for physical data storage, and
 *        randomly generate tuples. Implementations exist for each of the 4
 *        test tables.
 **/
class DataGenerator {
 public:
  DataGenerator() {
  }

  virtual ~DataGenerator() {
  }

  /**
   * @brief Create a relation for this DataGenerator's table schema.
   *
   * @return A new relation.
   **/
  virtual CatalogRelation* generateRelation() const = 0;

  /**
   * @brief Randomly generate tuples into the specified destination.
   *
   * @param num_tuples The total number of tuples to generate.
   * @param destination An InsertDestination which will be used to provide
   *        blocks which generated tuples are inserted into.
   * @param defer_rebuild If true, the rebuild() method will not be called on
   *        blocks that tuples are inserted into, leaving blocks in a
   *        potentially inconsistent state.
   **/
  void generateData(const std::size_t num_tuples,
                    InsertDestination *destination,
                    bool defer_rebuild = false) const;

  /**
   * @brief Randomly generate tuples into a particular value-range partition of
   *        a table.
   *
   * @param total_num_tuples The total number of tuples being generated in ALL
   *        partitions.
   * @param partition_value_column The column whose values the table is
   *        partitioned on.
   * @param partition_num The partition_number, out of total_partitions, to
   *        generate tuples for.
   * @param total_partitions The total number of partitions the table is
   *        divided into.
   * @param destination An InsertDestination which will be used to provide
   *        blocks which generated tuples are inserted into.
   **/
  void generateDataIntoPartition(const std::size_t total_num_tuples,
                                 const attribute_id partition_value_column,
                                 const std::size_t partition_num,
                                 const std::size_t total_partitions,
                                 InsertDestination *destination) const;

  /**
   * @brief Randomly generate tuples directly into a TupleStorageSubBlock
   *        (effectively a file in file-based experiments).
   *
   * @param num_tuples The number of tuples to generate.
   * @param tuple_store The TupleStorageSubBlock to insert generated tuples
   *        into.
   **/
  void generateDataIntoTupleStore(const std::size_t num_tuples,
                                  TupleStorageSubBlock *tuple_store) const;

  /**
   * @brief Generate an uncompressed column-store layout, optionally with
   *        indices.
   *
   * @param relation The relation to generate a layout for, previously created
   *        by generateRelation().
   * @param num_slots The number of StorageManager slots blocks should take up.
   * @param column_store_sort_column The ID of the column to sort on.
   * @param index_on_columns A vector of IDs of columns to build
   *        CSBTreeIndexSubBlocks on.
   * @return An uncompressed column-store layout.
   **/
  StorageBlockLayout* generateColumnstoreLayout(
      const CatalogRelation &relation,
      const std::size_t num_slots,
      const attribute_id column_store_sort_column,
      const std::vector<attribute_id> &index_on_columns) const;

  /**
   * @brief Generate an uncompressed row-store layout, optionally with indices.
   *
   * @param relation The relation to generate a layout for, previously created
   *        by generateRelation().
   * @param num_slots The number of StorageManager slots blocks should take up.
   * @param index_on_columns A vector of IDs of columns to build
   *        CSBTreeIndexSubBlocks on.
   * @return An uncompressed row-store layout.
   **/
  StorageBlockLayout* generateRowstoreLayout(
      const CatalogRelation &relation,
      const std::size_t num_slots,
      const std::vector<attribute_id> &index_on_columns) const;

  /**
   * @brief Generate a compressed column-store layout, optionally with indices.
   *
   * @param relation The relation to generate a layout for, previously created
   *        by generateRelation().
   * @param num_slots The number of StorageManager slots blocks should take up.
   * @param column_store_sort_column The ID of the column to sort on.
   * @param index_on_columns A vector of IDs of columns to build
   *        CSBTreeIndexSubBlocks on.
   * @return A compressed column-store layout.
   **/
  StorageBlockLayout* generateCompressedColumnstoreLayout(
      const CatalogRelation &relation,
      const std::size_t num_slots,
      const attribute_id column_store_sort_column,
      const std::vector<attribute_id> &index_on_columns) const;

  /**
   * @brief Generate a compressed row-store layout, optionally with indices.
   *
   * @param relation The relation to generate a layout for, previously created
   *        by generateRelation().
   * @param num_slots The number of StorageManager slots blocks should take up.
   * @param index_on_columns A vector of IDs of columns to build
   *        CSBTreeIndexSubBlocks on.
   * @return A compressed row-store layout.
   **/
  StorageBlockLayout* generateCompressedRowstoreLayout(
      const CatalogRelation &relation,
      const std::size_t num_slots,
      const std::vector<attribute_id> &index_on_columns) const;

  /**
   * @brief Generate a predicate which selects on the data generated by this
   *        DataGenerator.
   *
   * @param relation The relation the predicate applies to, previously created
   *        by generateRelation().
   * @param select_column The ID of the column the predicate will select on.
   * @param selectivity The desired selectivity of the predicate (expressed as
   *        a fraction in the range 0.0-1.0).
   **/
  virtual Predicate* generatePredicate(const CatalogRelation &relation,
                                       const attribute_id select_column,
                                       const float selectivity) const = 0;

 protected:
  static void AppendValueToTuple(Tuple* tuple, TypeInstance* value);
  virtual void generateValuesInTuple(Tuple* tuple) const = 0;
  virtual void generateValuesInTupleForPartition(Tuple* tuple,
                                                 const attribute_id partition_value_column,
                                                 const std::size_t partition_num,
                                                 const std::size_t total_partitions) const = 0;

  static void SeedRandom();
  static int GenerateRandomInt(const int range);

 private:
  // We always use the same RNG seed so experiments are exactly repeatable.
  static const unsigned int kRandomSeed = 42;

  DISALLOW_COPY_AND_ASSIGN(DataGenerator);
};

/**
 * @brief Intermediate class containing common functionality for the 3 numeric
 *        tables (narrow-u, narrow-e, and wide-e).
 **/
class NumericDataGenerator : public DataGenerator {
 public:
  NumericDataGenerator() {
  }

  virtual ~NumericDataGenerator() {
  }

  Predicate* generatePredicate(const CatalogRelation &relation,
                               const attribute_id select_column,
                               const float selectivity) const;

 protected:
  void generateValuesInTuple(Tuple* tuple) const;
  void generateValuesInTupleForPartition(Tuple* tuple,
                                         const attribute_id partition_value_column,
                                         const std::size_t partition_num,
                                         const std::size_t total_partitions) const;

  CatalogRelation* generateRelationHelper(const std::string &relation_name) const;

  std::vector<int> column_ranges_;

 private:
  DISALLOW_COPY_AND_ASSIGN(NumericDataGenerator);
};

/**
 * @brief Implementation of DataGenerator for the narrow-e table.
 **/
class NarrowEDataGenerator : public NumericDataGenerator {
 public:
  NarrowEDataGenerator();

  CatalogRelation* generateRelation() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(NarrowEDataGenerator);
};

/**
 * @brief Implementation of DataGenerator for the wide-e table.
 **/
class WideEDataGenerator : public NumericDataGenerator {
 public:
  WideEDataGenerator();

  CatalogRelation* generateRelation() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(WideEDataGenerator);
};

/**
 * @brief Implementation of DataGenerator for the narrow-u table.
 **/
class NarrowUDataGenerator : public NumericDataGenerator {
 public:
  NarrowUDataGenerator();

  CatalogRelation* generateRelation() const;

 private:
  DISALLOW_COPY_AND_ASSIGN(NarrowUDataGenerator);
};

/**
 * @brief Implementation of DataGenerator for the strings table.
 **/
class StringsDataGenerator : public DataGenerator {
 public:
  StringsDataGenerator() {
  }

  CatalogRelation* generateRelation() const;

  Predicate* generatePredicate(const CatalogRelation &relation,
                               const attribute_id select_column,
                               const float selectivity) const;

 protected:
  void generateValuesInTuple(Tuple* tuple) const;
  void generateValuesInTupleForPartition(Tuple* tuple,
                                         const attribute_id partition_value_column,
                                         const std::size_t partition_num,
                                         const std::size_t total_partitions) const;

 private:
  static const int kFiveCharInt = 1 << 30;

  static void GenerateFiveChars(const int mapped_int, char *dest);

  DISALLOW_COPY_AND_ASSIGN(StringsDataGenerator);
};

}  // namespace storage_explorer
}  // namespace quickstep

#endif  // QUICKSTEP_EXPERIMENTS_STORAGE_EXPLORER_DATA_GENERATOR_HPP_
