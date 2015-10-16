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

#ifndef QUICKSTEP_TYPES_OPERATION_HPP_
#define QUICKSTEP_TYPES_OPERATION_HPP_

#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

/**
 * @brief An operation which can be applied to typed values. Each exact
 *        concrete Operation is a singleton.
 **/
class Operation {
 public:
  /**
   * @brief Categories of intermediate supertypes of Operations.
   **/
  enum OperationSuperTypeID {
    kComparison = 0,
    kNumOperationSuperTypeIDs  // Not a real OperationSuperTypeID, exists for counting purposes.
  };

  /**
   * @brief Names of operation super-types in the same order as
   *        OperationSuperTypeID.
   * @note Defined out-of-line in Comparison.cpp
   **/
  static const char *kOperationSuperTypeNames[kNumOperationSuperTypeIDs];

  /**
   * @brief Virtual destructor.
   **/
  virtual ~Operation() {
  }

  /**
   * @brief Determine what supertype this Operation belongs to.
   *
   * @return The ID of the supertype this Operation belongs to.
   **/
  virtual OperationSuperTypeID getOperationSuperTypeID() const = 0;

  /**
   * @brief Get the name of this Operation.
   *
   * @return The human-readable name of this Operation.
   **/
  virtual const char* getName() const = 0;

  /**
   * @brief Get the short name of this Operation (i.e. a mathematical symbol).
   *
   * @return The short name of this Operation.
   **/
  virtual const char* getShortName() const = 0;

 protected:
  Operation() {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(Operation);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_OPERATION_HPP_
