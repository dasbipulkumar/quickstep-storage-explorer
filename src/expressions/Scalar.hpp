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

#ifndef QUICKSTEP_EXPRESSIONS_SCALAR_HPP_
#define QUICKSTEP_EXPRESSIONS_SCALAR_HPP_

#include <utility>

#include "catalog/CatalogTypedefs.hpp"
#include "storage/StorageBlockInfo.hpp"
#include "storage/TupleReference.hpp"
#include "types/TypeInstance.hpp"
#include "utility/ContainerCompat.hpp"
#include "utility/Macros.hpp"
#include "utility/ScopedPtr.hpp"

namespace quickstep {

class CatalogAttribute;
class CatalogDatabase;
class TupleStorageSubBlock;
class Type;

/** \addtogroup Expressions
 *  @{
 */

/**
 * @brief Base class for anything which evaluates to a Scalar value.
 **/
class Scalar {
 public:
  /**
   * @brief The possible provenance of Scalar values.
   **/
  enum ScalarDataSource {
    kLiteral = 0,
    kAttribute,
    kNumScalarDataSources  // Not a real ScalarDataSource, exists for counting purposes.
  };

  /**
   * @brief Virtual destructor.
   **/
  virtual ~Scalar() {
  }

  /**
   * @brief Make a deep copy of this Scalar.
   *
   * @return A cloned copy of this Scalar.
   **/
  virtual Scalar* clone() const = 0;

  /**
   * @brief Get the type of scalar value represented.
   *
   * @return The Type of the Scalar.
   **/
  virtual const Type& getType() const = 0;

  /**
   * @brief Get the provenance of this scalar value.
   *
   * @return The source of this Scalar's data.
   **/
  virtual ScalarDataSource getDataSource() const = 0;

  /**
   * @brief Get this Scalar's value for the given tuple in a
   *        TupleStorageSubBlock.
   * @note This only works for Scalars which can be evaluated for a single
   *       table. Use getValueForMultipleTuples() where necessary.
   * @note If the symbol QUICKSTEP_DEBUG is defined, then the
   *       TupleStorageSubBlock's relation and the attribute's type will be be
   *       checked for correctness. Otherwise, such checks should be performed
   *       elsewhere (i.e. by the query optimizer).
   *
   * @param tuple_store The TupleStorageSubBlock which contains the tuple to
   *        evaluate this Scalar for.
   * @param tuple The ID of the tuple in tupleStore to evaluate this Scalar
   *        for.
   * @return The value of this scalar for the given tuple in the given
   *         TupleStorageSubBlock. Should be deleted by caller when no longer
   *         in use.
   **/
  virtual TypeInstance* getValueForSingleTuple(const TupleStorageSubBlock &tuple_store,
                                               const tuple_id tuple) const = 0;

  /**
   * @brief Determine whether this Scalar's value is static (i.e. whether it is
   *        the same regardless of tuple).
   *
   * @return Whether this Scalar's value is static.
   **/
  virtual bool hasStaticValue() const {
    return false;
  }

  /**
   * @brief Get this Scalar's static value.
   * @warning hasStaticValue() should be called first to check whether a static
   *          value actually exists.
   *
   * @return This Scalar's static value.
   **/
  virtual const LiteralTypeInstance& getStaticValue() const;

  /**
   * @brief Determine whether this Scalar supports the getDataPtrFor() method
   *        with a given TupleStorageSubBlock.
   *
   * @param tuple_store The TupleStorageSubBlock which getDataPtrFor() would be
   *        called with.
   * @return Whether this Scalar supports getDataPtrFor().
   **/
  virtual bool supportsDataPtr(const TupleStorageSubBlock &tuple_store) const {
    return false;
  }

  /**
   * @brief Get an untyped pointer to the underlying data represented by this
   *        Scalar.
   * @warning supportsDataPtr() should be called first to check whether this
   *          Scalar actually supports this method.
   *
   * @param tuple_store The TupleStorageSubBlock which contains the tuple to
   *        evaluate this Scalar for.
   * @param tuple The ID of the tuple in tuple_store to evaluate this Scalar
   *        for.
   * @return An untyped pointer to this Scalar's underlying data.
   **/
  virtual const void* getDataPtrFor(const TupleStorageSubBlock &tuple_store, const tuple_id tuple) const;

 protected:
  Scalar() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(Scalar);
};


/**
 * @brief Scalars which are literal values from a SQL statement.
 **/
class ScalarLiteral : public Scalar {
 public:
  /**
   * @brief Constructor.
   *
   * @param lit A LiteralTypeInstance to wrap, which this ScalarLiteral takes
   *        ownership of.
   **/
  explicit ScalarLiteral(LiteralTypeInstance *lit)
      : internal_literal_(lit) {
  }

  ~ScalarLiteral() {
  }

  Scalar* clone() const;

  const Type& getType() const;

  ScalarDataSource getDataSource() const {
    return kLiteral;
  }

  TypeInstance* getValueForSingleTuple(const TupleStorageSubBlock &tupleStore, const tuple_id tuple) const;

  bool hasStaticValue() const {
    return true;
  }

  const LiteralTypeInstance& getStaticValue() const {
    return *internal_literal_;
  }

  bool supportsDataPtr(const TupleStorageSubBlock &tuple_store) const {
    return true;
  }

  const void* getDataPtrFor(const TupleStorageSubBlock &tuple_store, const tuple_id tuple) const;

 private:
  ScopedPtr<LiteralTypeInstance> internal_literal_;

  DISALLOW_COPY_AND_ASSIGN(ScalarLiteral);
};

/**
 * @brief Scalars which are attribute values from tuples.
 **/
class ScalarAttribute : public Scalar {
 public:
  /**
   * @brief Constructor.
   *
   * @param attribute The attribute to use.
   **/
  explicit ScalarAttribute(const CatalogAttribute &attribute)
      : attribute_(&attribute) {
  }

  Scalar* clone() const;

  const Type& getType() const;

  ScalarDataSource getDataSource() const {
    return kAttribute;
  }

  TypeInstance* getValueForSingleTuple(const TupleStorageSubBlock &tupleStore, const tuple_id tuple) const;

  bool supportsDataPtr(const TupleStorageSubBlock &tuple_store) const;

  const void* getDataPtrFor(const TupleStorageSubBlock &tuple_store, const tuple_id tuple) const;

  const CatalogAttribute& getAttribute() const {
    return *attribute_;
  }

 protected:
  // Ordinarily we would use a reference, but we need a pointer to support
  // deferred-setting when deserializing from JSON.
  const CatalogAttribute *attribute_;

  DISALLOW_COPY_AND_ASSIGN(ScalarAttribute);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_EXPRESSIONS_SCALAR_HPP_
