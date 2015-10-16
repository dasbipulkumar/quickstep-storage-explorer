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

#ifndef QUICKSTEP_TYPES_TUPLE_HPP_
#define QUICKSTEP_TYPES_TUPLE_HPP_

#include <cstddef>
#include <exception>
#include <string>
#include <vector>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/TupleReference.hpp"
#include "types/TypeInstance.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/PtrVector.hpp"

namespace quickstep {

class CatalogRelation;
class Scalar;
class TupleStorageSubBlock;

namespace storage_explorer {
class DataGenerator;
}

template <typename T> class PtrList;

/** \addtogroup Types
 *  @{
 */

/**
 * @brief Exception thrown when attempting to insert a value whose type does
 *        not match that of the corresponding column.
 **/
class WrongAttributeValueType : public std::exception {
 public:
  /**
   * @brief Constructor
   *
   * @param attribute_name The name of the attribute whose type did not match
   *        that of the provided value.
   **/
  explicit WrongAttributeValueType(const std::string &attribute_name)
      : message_("WrongAttributeValueType: Attempted to INSERT a value whose type didn't match the type of column ") {
    message_.append(attribute_name);
  }

  ~WrongAttributeValueType() throw() {}

  virtual const char* what() const throw() {
    return message_.c_str();
  }

 private:
  std::string message_;
};

/**
 * @brief A representation of a single tuple, i.e. a list of values
 *        corresponding to the attributes of a relation.
 **/
class Tuple {
 public:
  typedef PtrVector<TypeInstance>::const_iterator const_iterator;
  typedef PtrVector<TypeInstance>::size_type size_type;

  /**
   * @brief Constructor which builds a Tuple by projecting attributes from a
   *        tuple in a StorageBlock.
   *
   * @param tuple_store The TupleStorageSubBlock which contains the physical
   *        tuple to project from.
   * @param tid The tuple_id of the source tuple in tuple_store.
   * @param projection_list The attributes to project.
   **/
  Tuple(const TupleStorageSubBlock &tuple_store,
        const tuple_id tid,
        const std::vector<attribute_id> &projection_list);

  /**
   * @brief Constructor which builds a Tuple by evaluating Scalars for a tuple
   *       in a StorageBlock.
   *
   * @param tuple_store The TupleStorageSubBlock which contains the physical
   *        tuple to evaluate Scalars for.
   * @param tid The tuple_id of the source tuple in tuple_store.
   * @param selection A list of Scalar expressions to evaluate for the tuple.
   **/
  Tuple(const TupleStorageSubBlock &tuple_store,
        const tuple_id tid,
        const PtrList<Scalar> &selection);

  /**
   * @brief Constructor which makes a copy (i.e. all LiteralTypeInstances) of a
   *        tuple in a StorageBlock with some attribute values replaced by
   *        updated values.
   *
   * @param tuple_store The TupleStorageSubBlock which contains the physical
   *        tuple to copy.
   * @param tid The tuple_id of the source tuple in tuple_store.
   * @param updated_values A map of updated attribute values which will replace
   *        the corresponding original values in this copy. This Tuple takes
   *        ownership of the LiteralTypeInstances in updated_values.
   **/
  Tuple(const TupleStorageSubBlock &tuple_store,
        const tuple_id tid,
        const CompatUnorderedMap<attribute_id, LiteralTypeInstance*>::unordered_map &updated_values);

  /**
   * @brief Destructor.
   **/
  ~Tuple() {
  }

  /**
   * @brief Make a complete copy of this Tuple, copying each individual
   *        attribute value as a new LiteralTypeInstance.
   *
   * @return A deep copy of this Tuple.
   **/
  Tuple* clone() const;

  /**
   * @brief Make a complete copy of this Tuple, coercing individual attribute
   *        values as necessary to match the types of attributes in a
   *        particular relation.
   * @warning It is an error to call this method with a relation which this
   *          Tuple can not be completely coerced to.
   *
   * @param relation The target relation for the cloned tuple.
   * @return A deep copy of this Tuple, with attribute values coerced as
   *         necessary to match the exact types of attributes in relation.
   **/
  Tuple* cloneAsInstanceOfRelation(const CatalogRelation &relation) const;

  /**
   * @brief Get the value of the specified attribute.
   * @warning This is only safe if gapsInAttributeSequence() is false for the
   *          relation this tuple belongs to.
   *
   * @param attr The id of the attribute to get.
   * @return The attribute's value in this tuple.
   **/
  const TypeInstance& getAttributeValue(const attribute_id attr) const {
    DEBUG_ASSERT(attr >= 0);
    // The cast supresses a warning about comparing signed and unsigned types.
    DEBUG_ASSERT(static_cast<std::vector<TypeInstance*>::size_type>(attr) < attributes_.size());
    return attributes_[attr];
  }

  std::size_t getByteSize() const;

  /**
   * @brief Get an iterator at the beginning of the attribute values in this
   *        Tuple.
   *
   * @return An iterator at the beginning of this Tuple.
   **/
  const_iterator begin() const {
    return attributes_.begin();
  }

  /**
   * @brief Get an iterator one-past-the-end of the attribute values in this
   *        Tuple.
   *
   * @return An iterator one-past-the-end of this Tuple.
   **/
  const_iterator end() const {
    return attributes_.end();
  }

  /**
   * @brief Get the number of attributes in this Tuple.
   *
   * @return The number of attributes in this Tuple.
   **/
  size_type size() const {
    return attributes_.size();
  }

 private:
  /**
   * @brief Constructor which does not create any attributes, nor pre-reserve
   *        space.
   * @warning This is only used by clone(), and should not otherwise be used.
   **/
  Tuple() {
  }

  /**
   * @brief Constructor which does not create any attributes.
   * @warning This is only used by TextScanWorkUnit and various unit tests and
   *          should not otherwise be used.
   *
   * @param relation The relation which this Tuple belongs to.
   **/
  explicit Tuple(const CatalogRelation &relation);

  /**
   * @brief Append a value to this Tuple.
   * @warning This is only used by TextScanWorkUnit and
   *          CSBTreeIndexSubBlockTest and should not otherwise be used.
   **/
  void append(TypeInstance *item) {
    attributes_.push_back(item);
  }

  PtrVector<TypeInstance> attributes_;

  friend class storage_explorer::DataGenerator;

  DISALLOW_COPY_AND_ASSIGN(Tuple);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_TUPLE_HPP_
