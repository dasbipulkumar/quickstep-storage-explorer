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

#ifndef QUICKSTEP_TYPES_TYPE_INSTANCE_HPP_
#define QUICKSTEP_TYPES_TYPE_INSTANCE_HPP_

#include <cstddef>
#include <iostream>

#include "utility/CstdintCompat.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

class LiteralTypeInstance;
class ReferenceTypeInstance;
class Type;
class TypeInstance;

/** \addtogroup Types
 *  @{
 */

/**
 * @brief Write the data in a TypeInstance to an ostream in human-readable
 *        form.
 *
 * @param out The ostream to write to.
 * @param ti The TypeInstance to write.
 * @return out, after writing.
 **/
std::ostream& operator<< (std::ostream &out, const TypeInstance &ti);


/**
 * @brief A data item instance of Type.
 **/
class TypeInstance {
 public:
  /**
   * @brief Constructor.
   *
   * @param type The Type which this instance belongs to.
   **/
  explicit TypeInstance(const Type &type)
      : type_(type) {
  }

  /**
   * @brief Virtual destructor.
   **/
  virtual ~TypeInstance() {
  }

  /**
   * @brief Get the Type which this instance belongs to.
   *
   * @return The Type which this instance belongs to.
   **/
  const Type& getType() const {
    return type_;
  }

  /**
   * @brief Determine whether this TypeInstance is a literal or a reference.
   *
   * @return True if this is a LiteralTypeInstance, false if this is a
   *         ReferenceTypeInstance.
   **/
  virtual bool isLiteral() const = 0;

  /**
   * @brief Determine whether this TypeInstance is a NULL value.
   *
   * @return Whether this TypeInstance is NULL.
   **/
  virtual bool isNull() const = 0;

  /**
   * @brief Get a pointer to the data underlying this TypeInstance.
   *
   * @return A pointer to the data item underyling this TypeInstance (a NULL
   *         pointer if isNull() is true).
   **/
  virtual const void* getDataPtr() const = 0;

  /**
   * @brief Determine the size of this particular data item in bytes.
   *
   * @return The size of this data item in bytes (0 if isNull() is true).
   **/
  std::size_t getInstanceByteLength() const;

  /**
   * @brief Make a literal copy of this data item with the same Type.
   *
   * @return A literal copy of this TypeInstance.
   **/
  virtual LiteralTypeInstance* makeCopy() const = 0;

  /**
   * @brief Make a reference to this TypeInstance's underlying data.
   * @warning The ReferenceTypeInstance produced by this method is only valid
   *          so long as this TypeInstance is valid (i.e. as long as a
   *          LiteralTypeInstance exists, or as long as the underlying pointer
   *          of a ReferenceTypeInstance remains valid).
   * @note This is almost always as cheap or cheaper than makeCopy(), because a
   *       ReferenceTypeInstance is always 24 bytes, whereas
   *       LiteralTypeInstance is at least 24 bytes and in many cases larger.
   *       The only exception is when isNull() is true, in which case
   *       makeCopy() will produce a NullLiteralTypeInstance, which is only 16
   *       bytes.
   *
   * @return A reference to this TypeInstance's data.
   **/
  ReferenceTypeInstance* makeReference() const;

  /**
   * @brief Make a literal copy of this data item, coerced to a different Type.
   * @warning It is an error to call this method with a coerced_type that can
   *          not actually be coerced to. Type::isCoercibleTo() should be
   *          checked first.
   *
   * @param coerced_type The type to coerce this item to.
   * @return A literal copy of this TypeInstance, coerced to coerced_type.
   **/
  LiteralTypeInstance* makeCoercedCopy(const Type &coerced_type) const;

  /**
   * @brief Copy this TypeInstance's underlying data into a specified memory
   *        location.
   * @warning This method is potentially unsafe. The caller is responsible for
   *          ensuring that destination can fit the value and is safe to write.
   * @warning This method must not be used if isNull() is true.
   *
   * @param destination A memory location to write data into.
   **/
  void copyInto(void *destination) const;

  /**
   * @brief Determine if this TypeInstance supports the Numeric interface (i.e.
   *        whether it belongs to one of the Numeric types and is not a NULL
   *        literal).
   * @note The Numeric interface consists of the four methods
   *       numericGetIntValue(), numericGetLongValue(), numericGetFloatValue(),
   *       and numericGetDoubleValue().
   * @warning A ReferenceTypeInstance belonging to a nullable Numeric type will
   *          return true for supportsNumericInterface(), even if the
   *          particular ReferenceTypeInstance represents a NULL. Attempting to
   *          use a numeric interface method on such a ReferenceTypeInstance
   *          will cause a segmentation fault. To avoid this, when dealing with
   *          nullable types, isNull() should always be checked before using
   *          any Numeric interface methods.
   *
   * @return True if the Numeric interface is usable, false otherwise.
   **/
  virtual bool supportsNumericInterface() const {
    return false;
  }

  /**
   * @brief For Numeric TypeInstances, get this TypeInstance's value as a C++
   *        int.
   * @warning supportsNumericInterface() must be true and isNull() must be
   *          false for this method to be usable.
   *
   * @return The value as an int.
   **/
  virtual int numericGetIntValue() const {
    FATAL_ERROR("Used a Numeric interface method on a non-numeric TypeInstance.");
  }

  /**
   * @brief For Numeric TypeInstances, get this TypeInstance's value as an
   *        int64_t.
   * @warning supportsNumericInterface() must be true and isNull() must be
   *          false for this method to be usable.
   *
   * @return The value as an int64_t.
   **/
  virtual std::int64_t numericGetLongValue() const {
    FATAL_ERROR("Used a Numeric interface method on a non-numeric TypeInstance.");
  }

  /**
   * @brief For Numeric TypeInstances, get this TypeInstance's value as a C++
   *        float.
   * @warning supportsNumericInterface() must be true and isNull() must be
   *          false for this method to be usable.
   *
   * @return The value as a float.
   **/
  virtual float numericGetFloatValue() const {
    FATAL_ERROR("Used a Numeric interface method on a non-numeric TypeInstance.");
  }

  /**
   * @brief For Numeric TypeInstances, get this TypeInstance's value as a C++
   *        double.
   * @warning supportsNumericInterface() must be true and isNull() must be
   *          false for this method to be usable.
   *
   * @return The value as a double.
   **/
  virtual double numericGetDoubleValue() const {
    FATAL_ERROR("Used a Numeric interface method on a non-numeric TypeInstance.");
  }

  /**
   * @brief Determine if this TypeInstance supports the AsciiString interface
   *        (i.e. whether it belongs to one of the AsciiString types and is not
   *        a NULL literal).
   * @note The AsciiString interface consists of the four methods
   *       asciiStringGuaranteedNullTerminated(), asciiStringNullTerminated(),
   *       asciiStringMaximumLength(), and asciiStringLength().
   * @note If this method returns true, getDataPtr() may be cast to const char*
   *       and used as a C-string, but take care when using functions that
   *       assume strings are null-terminated and see
   *       asciiStringGuaranteedNullTerminated() below.
   * @warning A ReferenceTypeInstance belonging to a nullable AsciiString type
   *          will return true for supportsAsciiStringInterface(), even if the
   *          particular ReferenceTypeInstance represents a NULL. Attempting to
   *          use an AsciiString interface method on such a
   *          ReferenceTypeInstance will cause a segmentation fault. To avoid
   *          this, when dealing with nullable types, isNull() should always be
   *          checked before using any AsciiString interface methods.
   *
   * @return True if the AsciiString interface is usable, false otherwise.
   **/
  virtual bool supportsAsciiStringInterface() const {
    return false;
  }

  /**
   * @brief Determine whether the string represented by this type instance is
   *        guaranteed to have a terminating NULL-character.
   * @note It is possible that the string represented by this TypeInstance is
   *       NULL-terminated even if this method returns false (e.g. any
   *       CHAR instance which is less than the maximum length for its Type).
   *       This method indicates whether ALL instances of this instance's Type
   *       are guaranteed to have NULL-terminator (as is the case for VARCHAR).
   * @note Also see asciiStringNullTerminated() below.
   * @warning supportsAsciiStringInterface() must be true and isNull() must be
   *          false for this method to be usable.
   *
   * @return Whether the underlying string is guaranteed to have a
   *         NULL-terminator.
   **/
  virtual bool asciiStringGuaranteedNullTerminated() const {
    FATAL_ERROR("Used an AsciiString interface method on a non-AsciiString TypeInstance.");
  }

  /**
   * @brief Actually determine whether the string represented by this
   *        particular type instance has a terminating NULL-character.
   * @note If asciiStringGuaranteedNullTerminated() is false, then this method
   *       actually scans the string for a NULL-terminator.
   * @warning supportsAsciiStringInterface() must be true and isNull() must be
   *          false for this method to be usable.
   *
   * @return Whether the underlying string is has a NULL-terminator.
   **/
  virtual bool asciiStringNullTerminated() const {
    FATAL_ERROR("Used an AsciiString interface method on a non-AsciiString TypeInstance.");
  }

  /**
   * @brief Determine the maximum length of a string represented by this
   *        TypeInstance (i.e. the length parameter of the CharType or
   *        VarCharType this instance belongs to).
   * @warning supportsAsciiStringInterface() must be true and isNull() must be
   *          false for this method to be usable.
   *
   * @return The maximum possible length of the underlying string.
   **/
  virtual std::size_t asciiStringMaximumLength() const {
    FATAL_ERROR("Used an AsciiString interface method on a non-AsciiString TypeInstance.");
  }

  /**
   * @brief Determine the actual length of the ASCII string this TypeInstance
   *        represents.
   * @warning supportsAsciiStringInterface() must be true and isNull() must be
   *          false for this method to be usable.
   *
   * @return The length of the underlying string (not counting a
   *         NULL-terminator, where applicable).
   **/
  virtual std::size_t asciiStringLength() const {
    FATAL_ERROR("Used an AsciiString interface method on a non-AsciiString TypeInstance.");
  }

 protected:
  /**
   * @brief A method which wraps putToStreamUnsafe, checking for a null value.
   *
   * @param stream An ostream to write to.
   **/
  void putToStream(std::ostream *stream) const;

  /**
   * @brief A helper method used to write this TypeInstance to an ostream in
   *        human-readable form.
   *
   * @param stream An ostream to write to.
   **/
  virtual void putToStreamUnsafe(std::ostream *stream) const = 0;

 private:
  const Type &type_;

  friend std::ostream& operator<< (std::ostream &out, const TypeInstance &ti);

  DISALLOW_COPY_AND_ASSIGN(TypeInstance);
};

/**
 * @brief A TypeInstance which represents a reference to a data item in memory
 *        initialized and owned by someone else.
 **/
class ReferenceTypeInstance : public TypeInstance {
 public:
  /**
   * @brief Constructor.
   * @warning A ReferenceTypeInstance is only valid so long as the data pointer
   *          is valid.
   *
   * @param type The Type which this instance belongs to.
   * @param data A pointer to a data item of type.
   **/
  ReferenceTypeInstance(const Type &type, const void *data);

  bool isLiteral() const {
    return false;
  }

  bool isNull() const {
    return (data_ == NULL);
  }

  const void* getDataPtr() const {
    return data_;
  }

 protected:
  const void *data_;

 private:
  DISALLOW_COPY_AND_ASSIGN(ReferenceTypeInstance);
};


/**
 * @brief A TypeInstance which represents a literal value.
 **/
class LiteralTypeInstance : public TypeInstance {
 public:
  /**
   * @brief Constructor
   *
   * @param type The Type which this instance belongs to.
   **/
  explicit LiteralTypeInstance(const Type &type):
    TypeInstance(type) {
  }

  bool isLiteral() const {
    return true;
  }

  virtual bool isNull() const {
    return false;
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LiteralTypeInstance);
};

/**
 * @brief A special LiteralTypeInstance for a NULL value.
 * @warning This class doesn't implement any of Numeric or AsciiString
 *          interface methods, even if an instance belongs to a Numeric or
 *          AsciiString type, respectively. isNull() should be checked before
 *          attempting to use any method of the Numeric or AsciiString
 *          interfaces when dealing with an instance of a nullable type.
 **/
class NullLiteralTypeInstance : public LiteralTypeInstance {
 public:
  /**
   * @brief Constructor.
   *
   * @param type The Type which this instance belongs to.
   **/
  explicit NullLiteralTypeInstance(const Type &type);

  bool isNull() const {
    return true;
  }

  const void* getDataPtr() const {
    return NULL;
  }

  LiteralTypeInstance* makeCopy() const {
    return new NullLiteralTypeInstance(getType());
  }

  void putToStreamUnsafe(std::ostream *stream) const {
    *stream << "NULL";
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(NullLiteralTypeInstance);
};

/**
 * @brief A LiteralTypeInstance whose internal data is in dynamically-allocated
 *        memory which it owns.
 **/
class PtrBasedLiteralTypeInstance : public LiteralTypeInstance {
 public:
  /**
   * @brief Constructor.
   *
   * @param type The Type which this instance belongs to.
   * @param data A pointer to memory containing the literal data, which
   *        becomes owned by this PtrBasedLiteralTypeInstance.
   **/
  PtrBasedLiteralTypeInstance(const Type &type, void *data):
    LiteralTypeInstance(type), data_(data) {
  }

  /**
   * @brief Destructor which frees data memory, if any.
   **/
  ~PtrBasedLiteralTypeInstance();

  const void* getDataPtr() const {
    return data_;
  }

 protected:
  void *data_;

 private:
  DISALLOW_COPY_AND_ASSIGN(PtrBasedLiteralTypeInstance);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_TYPE_INSTANCE_HPP_
