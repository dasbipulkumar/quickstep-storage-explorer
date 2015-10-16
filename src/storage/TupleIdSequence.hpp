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

#ifndef QUICKSTEP_STORAGE_TUPLE_ID_SEQUENCE_HPP_
#define QUICKSTEP_STORAGE_TUPLE_ID_SEQUENCE_HPP_

#include <algorithm>
#include <vector>

#include "storage/StorageBlockInfo.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief A list of Tuple IDs, used to communicate information about multiple
 *        tuples between SubBlocks.
 **/
class TupleIdSequence {
 public:
  typedef std::vector<tuple_id>::size_type size_type;
  typedef std::vector<tuple_id>::const_iterator const_iterator;
  typedef std::vector<tuple_id>::const_reference const_reference;

  /**
   * @brief Constructor.
   **/
  TupleIdSequence()
      : sorted_(true) {
  }

  /**
   * @brief Destructor.
   **/
  virtual ~TupleIdSequence() {
  }

  inline void append(const tuple_id tuple) {
    if (sorted_ && (!internal_vector_.empty()) && (tuple < internal_vector_.back())) {
      sorted_ = false;
    }
    internal_vector_.push_back(tuple);
  }

  inline bool empty() const {
    return internal_vector_.empty();
  }

  inline size_type size() const {
    return internal_vector_.size();
  }

  inline const_iterator begin() const {
    return internal_vector_.begin();
  }

  inline const_iterator end() const {
    return internal_vector_.end();
  }

  inline const_reference operator[] (const size_type n) const {
    return internal_vector_[n];
  }

  inline const_reference front() const {
    return internal_vector_.front();
  }

  inline const_reference back() const {
    return internal_vector_.back();
  }

  /**
   * @brief Determine if this sequence is sorted.
   *
   * @return Whether the internal tuple_ids are sorted in ascending order.
   **/
  bool isSorted() const {
    return sorted_;
  }

  /**
   * @brief Sort internal tuple_ids into ascending order.
   **/
  void sort() {
    if (!sorted_) {
      std::sort(internal_vector_.begin(), internal_vector_.end());
      sorted_ = true;
    }
  }

 private:
  std::vector<tuple_id> internal_vector_;
  bool sorted_;

  DISALLOW_COPY_AND_ASSIGN(TupleIdSequence);
};

/**
 * @brief A wrapper for the results of a call to
 *        TupleStorageSubBlock::bulkInsertTuples().
 **/
struct BulkInsertResult {
  /**
   * @brief True if other tuples in the TupleStorageSubBlock had their ids
   *        mutated (requiring that indexes be rebuilt), false otherwise.
   **/
  bool ids_mutated;

  /**
   * @brief The IDs of the inserted tuples.
   **/
  TupleIdSequence *sequence;
};

/**
 * @brief A wrapper for results from searching an index for tuples matching
 *        a predicate.
 **/
struct IndexSearchResult {
  /**
   * @brief True if the tuples in this result set are a superset of the tuples
   *        matching the predicate (necessitating checking for each tuple),
   *        false if they are exactly the matching set.
   **/
  bool is_superset;

  /**
   * @brief The IDs of tuples matching a predicate, or a superset thereof.
   **/
  TupleIdSequence *sequence;
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_TUPLE_ID_SEQUENCE_HPP_
