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

#ifndef QUICKSTEP_UTILITY_SCOPED_PTR_HPP_
#define QUICKSTEP_UTILITY_SCOPED_PTR_HPP_

#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief An extremely basic (non-copyable, non-moveable) smart-pointer class
 *        that implements scoped pointer ownership.
 **/
template <typename T>
class ScopedPtr {
 public:
  /**
   * @brief Constructor.
   *
   * @param A pointer to an object to take ownership of (NULL to leave this
   *        ScopedPtr "empty").
   **/
  explicit ScopedPtr(T *ptr = NULL)
      : internal_ptr_(ptr) {
  }

  /**
   * @brief Destructor which deletes the object this ScopedPtr points to, if
   *        any.
   **/
  ~ScopedPtr() {
    if (internal_ptr_ != NULL) {
      delete internal_ptr_;
    }
  }

  /**
   * @brief Delete the object this ScopedPtr points to, if any, and optionally
   *        take ownership of a new object.
   *
   * @param A pointer to an object to take ownership of (NULL to leave this
   *        ScopedPtr "empty");
   **/
  void reset(T *ptr = NULL) {
    if (internal_ptr_ != NULL) {
      delete internal_ptr_;
    }
    internal_ptr_ = ptr;
  }

  /**
   * @brief Release ownership of the pointed-to object (if any) and return a
   *        pointer to it.
   * @warning The caller becomes responsible for managing the object released
   *          by this call and deleting it when it will no longer be used.
   *
   * @return A pointer to the object held by this ScopedPtr, or NULL if empty()
   *         was true before the call.
   **/
  T* release() {
    T *ptr = internal_ptr_;
    internal_ptr_ = NULL;
    return ptr;
  }

  /**
   * @brief Check whether this ScopedPtr is empty (doesn't contain an object).
   **/
  inline bool empty() const {
    return internal_ptr_ == NULL;
  }

  /** 
   * @brief Get a pointer to the object owned by this ScopedPtr.
   *
   * @return a pointer to the object owned by this ScopedPtr (NULL if empty()
   *         is true).
   **/
  inline T* get() const {
    return internal_ptr_;
  }

  /**
   * @brief Dereference operator.
   * @warning This will segfault if empty() is true.
   **/
  inline T& operator*() const {
    return *internal_ptr_;
  }

  /**
   * @brief Dereference operator.
   * @warning This will segfault if empty() is true.
   **/
  inline T* operator->() const {
    return internal_ptr_;
  }

 private:
  T *internal_ptr_;

  DISALLOW_COPY_AND_ASSIGN(ScopedPtr);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_SCOPED_PTR_HPP_
