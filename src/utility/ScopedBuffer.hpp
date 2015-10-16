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

#ifndef QUICKSTEP_UTILITY_SCOPED_BUFFER_HPP_
#define QUICKSTEP_UTILITY_SCOPED_BUFFER_HPP_

#include <cstddef>
#include <cstdlib>

#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief A class which manages an untyped buffer of heap memory which is freed
 *        when it goes out of scope.
 **/
class ScopedBuffer {
 public:
  /**
   * @brief Constructor which allocates a new chunk of memory of the specified
   *        size.
   *
   * @param alloc_size The number of bytes of memory to allocate.
   **/
  explicit ScopedBuffer(const std::size_t alloc_size) {
    internal_ptr_ = std::malloc(alloc_size);
  }

  /**
   * @brief Constructor which takes ownership of an existing chunk of memory.
   * @warning memory MUST be allocated with malloc(), not with new or on the
   *          stack.
   *
   * @param memory The memory to take ownership of.
   **/
  explicit ScopedBuffer(void *memory = NULL)
      : internal_ptr_(memory) {
  }

  /**
   * @brief Destructor which frees the memory held in this buffer.
   **/
  ~ScopedBuffer() {
    if (internal_ptr_ != NULL) {
      std::free(internal_ptr_);
    }
  }

  /**
   * @brief Free the buffer memory and create a new buffer of the specified
   *        size.
   *
   * @param alloc_size The number of bytes of memory to allocate.
   **/
  void reset(const std::size_t alloc_size) {
    if (internal_ptr_ != NULL) {
      std::free(internal_ptr_);
    }
    internal_ptr_ = std::malloc(alloc_size);
  }

  /**
   * @brief Free the buffer memory and take ownership of the specified chunk of
   *        memory.
   * @warning memory MUST be allocated with malloc(), not with new or on the
   *          stack.
   *
   * @param memory The memory to take ownership of.
   **/
  void reset(void *memory = NULL) {
    if (internal_ptr_ != NULL) {
      std::free(internal_ptr_);
    }
    internal_ptr_ = memory;
  }

  /**
   * @brief Release ownership of the memory owned by this ScopedBuffer, and
   *        return a pointer to it.
   * @warning The caller becomes responsible for managing the memory returned
   *          by this method, and should free it when it is no longer in use to
   *          avoid memory leaks.
   *
   * @return A pointer to the memory previously owned by this ScopedBuffer.
   *         NULL if empty() was true before the call.
   **/
  void* release() {
    void *memory = internal_ptr_;
    internal_ptr_ = NULL;
    return memory;
  }

  /**
   * @brief Check whether this ScopedBuffer is empty, i.e. whether it currently
   *        owns any memory.
   *
   * @return Whether this ScopedBuffer is empty.
   **/
  inline bool empty() const {
    return internal_ptr_ == NULL;
  }

  /**
   * @brief Get a pointer to the memory owned by this ScopedBuffer.
   * @warning Do not call free() on the memory returned by this method. Use
   *          reset() or delete the ScopedBuffer instead.
   *
   * @return A pointer the memory managed by this ScopedBuffer.
   **/
  inline void* get() const {
    return internal_ptr_;
  }

 private:
  void *internal_ptr_;

  DISALLOW_COPY_AND_ASSIGN(ScopedBuffer);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_SCOPED_BUFFER_HPP_
