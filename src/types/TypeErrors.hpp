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

#ifndef QUICKSTEP_TYPES_TYPE_ERRORS_HPP_
#define QUICKSTEP_TYPES_TYPE_ERRORS_HPP_

#include <exception>
#include <string>

namespace quickstep {

class Type;
class Operation;

/** \addtogroup Types
 *  @{
 */

/**
 * @brief Exception thrown when attempting to reconstruct a Type from malformed
 *        JSON.
 **/
class TypeJSONReconstructionError : public std::exception {
 public:
  virtual const char* what() const throw() {
    return "TypeJSONReconstructionError: Attempted to reconstruct a Type from a malformed JSON representation";
  }
};

/**
 * @brief Exception thrown when attempting to reconstruct an Operation from
 *        malformed JSON.
 **/
class OperationJSONReconstructionError : public std::exception {
 public:
  virtual const char* what() const throw() {
    return "OperationJSONReconstructionError: Attempted to reconstruct an "
           "Operation from a malformed JSON representation";
  }
};

/**
 * @brief Exception thrown when attempting to reconstruct a LiteralTypeInstance
 *        from malformed JSON.
 **/
class LiteralTypeInstanceJSONReconstructionError : public std::exception {
 public:
  virtual const char* what() const throw() {
    return "LiteralTypeInstanceJSONReconstructionError: Attempted to "
           "reconstruct a LiteralTypeInstance from a malformed JSON representation";
  }
};

/**
 * @brief Exception thrown when attempting to apply an Operation to arguments
 *        of unsupported Types.
 **/
class OperationInapplicableToType : public std::exception {
 public:
  /**
   * @brief Constructor.
   *
   * @param op The Operation which failed to apply.
   * @param num_types The number of arguments to op.
   * @param ... Pointers to Types of the arguments supplied to op.
   **/
  OperationInapplicableToType(const Operation &op, const int num_types, ...);

  ~OperationInapplicableToType() throw() {}

  virtual const char* what() const throw() {
    return message_.c_str();
  }

 private:
  std::string message_;
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_TYPE_ERRORS_HPP_
