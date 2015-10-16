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

#ifndef QUICKSTEP_STORAGE_TUPLE_REFERENCE_HPP_
#define QUICKSTEP_STORAGE_TUPLE_REFERENCE_HPP_

#include "storage/StorageBlockInfo.hpp"

namespace quickstep {

class TupleStorageSubBlock;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief A reference to a particular tuple in a StorageBlock.
 **/
class TupleReference {
 public:
  /**
   * @brief Default constructor for a TupleReference which doesn't actually
   *        reference anything.
   **/
  TupleReference()
      : tuple_store_(NULL), tuple_id_(-1) {
  }

  /**
   * @brief Constructor explicitly specifying the TupleStorageSubBlock and
   *        tuple to reference.
   *
   * @param tuple_store The TupleStorageSubBlock the tuple resides in.
   * @param tuple_id_arg The ID of the tuple in tuple_store to reference.
   **/
  TupleReference(const TupleStorageSubBlock &tuple_store, const tuple_id tuple_id_arg)
      : tuple_store_(&tuple_store), tuple_id_(tuple_id_arg) {
  }

  /**
   * @brief Copy constructor.
   **/
  TupleReference(const TupleReference &other)
      : tuple_store_(other.tuple_store_), tuple_id_(other.tuple_id_) {
  }

  /**
   * @brief Assignment operator.
   **/
  TupleReference& operator=(const TupleReference &rhs) {
    tuple_store_ = rhs.tuple_store_;
    tuple_id_ = rhs.tuple_id_;
    return *this;
  }

  /**
   * @brief Get the TupleStorageSubBlock the referenced tuple resides in.
   * @warning This will segfault for a default-constructed TupleReference,
   *          which doesn't actually point to anything.
   **/
  inline const TupleStorageSubBlock& getTupleStore() const {
    return *tuple_store_;
  }

  /**
   * @brief Manually set the TupleStorageSubBlock this TupleReference refers
   *        to.
   * @warning Setting this without also calling setTupleID() properly may cause
   *          this TupleReference to become invalid.
   **/
  inline void setTupleStore(const TupleStorageSubBlock &tuple_store) {
    tuple_store_ = &tuple_store;
  }

  /**
   * @brief Get the ID of the referenced tuple.
   *
   * @return The ID of the referenced tuple in its TupleStorageSubBlock, or -1
   *         if this TupleReference doesn't actually point to anything.
   **/
  inline tuple_id getTupleID() const {
    return tuple_id_;
  }

  /**
   * @brief Manually set the ID of the tuple this TupleReference refers to.
   * @warning Setting this without also calling setTupleStore() properly may
   *          cause this TupleReference to become invalid.
   **/
  inline void setTupleID(const tuple_id tuple_id_arg) {
    tuple_id_ = tuple_id_arg;
  }

 private:
  const TupleStorageSubBlock *tuple_store_;
  tuple_id tuple_id_;
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_TUPLE_REFERENCE_HPP_
