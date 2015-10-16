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

#ifndef QUICKSTEP_STORAGE_STORAGE_ERRORS_HPP_
#define QUICKSTEP_STORAGE_STORAGE_ERRORS_HPP_

#include <cstddef>
#include <exception>
#include <string>

namespace quickstep {

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief Exception thrown when the memory provided to a block or subblock is
 *        too small for even basic metadata.
 **/
class BlockMemoryTooSmall : public std::exception {
 public:
  /**
   * @brief Constructor.
   *
   * @param block_type The type of block or subblock that we were attempting
   *                   to create.
   * @param block_size The number of bytes allocated to the block or subblock.
   **/
  BlockMemoryTooSmall(const std::string &block_type, const std::size_t block_size);

  ~BlockMemoryTooSmall() throw() {
  }

  virtual const char* what() const throw() {
    return message_.c_str();
  }

 private:
  std::string message_;
};

/**
 * @brief Exception throw when a re-loaded block appears to be corrupted.
 **/
class MalformedBlock : public std::exception {
 public:
  virtual const char* what() const throw() {
    return "MalformedBlock: A reconstituted block appears to be malformed";
  }
};

/**
 * @brief Exception thrown when attempting to insert a tuple which is so large
 *        that it can't fit in an empty block.
 **/
class TupleTooLargeForBlock : public std::exception {
 public:
  /**
   * @brief Constructor.
   *
   * @param tuple_size The size of the huge tuple in bytes.
   **/
  explicit TupleTooLargeForBlock(const std::size_t tuple_size);

  ~TupleTooLargeForBlock() throw() {
  }

  virtual const char* what() const throw() {
    return message_.c_str();
  }

  /**
   * @brief Get the size of the tuple that caused this exception.
   *
   * @param return The size of the tuple that caused this exception, in bytes.
   **/
  std::size_t getTupleSize() const {
    return tuple_size_;
  }

 private:
  std::size_t tuple_size_;
  std::string message_;
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_STORAGE_ERRORS_HPP_
