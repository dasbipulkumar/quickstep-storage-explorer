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

#ifndef QUICKSTEP_UTILITY_PTR_LIST_HPP_
#define QUICKSTEP_UTILITY_PTR_LIST_HPP_

#include <list>

#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief A templated STL-style container class which holds a list of pointers
 *        to objects which it owns.
 **/
template <class T>
class PtrList {
 public:
  class PtrListConstIterator;

  class PtrListIterator : public std::iterator<std::forward_iterator_tag, T, int> {
   public:
    PtrListIterator() {
    }

    PtrListIterator(const PtrListIterator& other)
        : internal_iterator_(other.internal_iterator_) {
    }

    PtrListIterator& operator=(const PtrListIterator& other) {
      if (this != &other) {
        internal_iterator_ = other.internal_iterator_;
      }
      return *this;
    }

    inline PtrListIterator& operator++() {
      ++internal_iterator_;
      return *this;
    }

    PtrListIterator operator++(int) {  // NOLINT - increment operator doesn't need named param
      PtrListIterator result(*this);
      ++(*this);
      return result;
    }

    inline PtrListIterator& operator--() {
      --internal_iterator_;
      return *this;
    }

    PtrListIterator operator--(int) {  // NOLINT - decrement operator doesn't need named param
      PtrListIterator result(*this);
      --(*this);
      return result;
    }

    inline bool operator==(const PtrListIterator& other) const {
      return internal_iterator_ == other.internal_iterator_;
    }

    inline bool operator!=(const PtrListIterator& other) const {
      return internal_iterator_ != other.internal_iterator_;
    }

    inline T& operator*() const {
      return **internal_iterator_;
    }

    inline T* operator->() const {
      return *internal_iterator_;
    }

   private:
    explicit PtrListIterator(typename std::list<T*>::iterator internal_iterator)
        : internal_iterator_(internal_iterator) {
    }

    typename std::list<T*>::iterator internal_iterator_;

    friend class PtrList;
    friend class PtrListConstIterator;
  };


  class PtrListConstIterator : public std::iterator<std::input_iterator_tag, const T, int> {
   public:
    PtrListConstIterator() {
    }

    PtrListConstIterator(const PtrListConstIterator& other)
        : internal_iterator_(other.internal_iterator_) {
    }

    PtrListConstIterator(const PtrListIterator& other)  // NOLINT - allow implicit conversion
        : internal_iterator_(other.internal_iterator_) {
    }

    PtrListConstIterator& operator=(const PtrListConstIterator& other) {
      if (this != &other) {
        internal_iterator_ = other.internal_iterator_;
      }
      return *this;
    }

    PtrListConstIterator& operator=(const PtrListIterator& other) {
      internal_iterator_ = other.internal_iterator_;
      return *this;
    }

    inline PtrListConstIterator& operator++() {
      ++internal_iterator_;
      return *this;
    }

    PtrListConstIterator operator++(int) {  // NOLINT - increment operator doesn't need named param
      PtrListConstIterator result(*this);
      ++(*this);
      return result;
    }

    inline PtrListConstIterator& operator--() {
      --internal_iterator_;
      return *this;
    }

    PtrListConstIterator operator--(int) {  // NOLINT - decrement operator doesn't need named param
      PtrListConstIterator result(*this);
      --(*this);
      return result;
    }

    inline bool operator==(const PtrListConstIterator& other) const {
      return internal_iterator_ == other.internal_iterator_;
    }

    inline bool operator!=(const PtrListConstIterator& other) const {
      return internal_iterator_ != other.internal_iterator_;
    }

    inline const T& operator*() const {
      return **internal_iterator_;
    }

    inline const T* operator->() const {
      return *internal_iterator_;
    }

   private:
    explicit PtrListConstIterator(typename std::list<T*>::const_iterator internal_iterator)
        : internal_iterator_(internal_iterator) {
    }

    typename std::list<T*>::const_iterator internal_iterator_;

    friend class PtrList;
  };

  typedef PtrListIterator iterator;
  typedef PtrListConstIterator const_iterator;

  PtrList() {
  }

  ~PtrList() {
    for (typename std::list<T*>::iterator it = internal_list_.begin();
         it != internal_list_.end();
         ++it) {
      delete *it;
    }
  }

  // Add an element
  inline void push_back(T* elt) {
    internal_list_.push_back(elt);
  }

  inline void push_front(T* elt) {
    internal_list_.push_front(elt);
  }

  // Get cardinality of list
  inline std::size_t size() const {
    return internal_list_.size();
  }

  // Test whether list is empty
  inline bool empty() const {
    return internal_list_.empty();
  }

  // Iterators
  iterator begin() {
    return PtrListIterator(internal_list_.begin());
  }

  iterator end() {
    return PtrListIterator(internal_list_.end());
  }

  const_iterator begin() const {
    return PtrListConstIterator(internal_list_.begin());
  }

  const_iterator end() const {
    return PtrListConstIterator(internal_list_.end());
  }

  void splice(iterator position, PtrList<T> &source) {  // NOLINT - Non-const reference like STL.
    internal_list_.splice(position.internal_iterator_, source.internal_list_);
  }

  /**
   * @brief Clear contents but do not release memory. Don't call this unless
   *        you know what you're doing.
   **/
  void clearWithoutRelease() {
    internal_list_.clear();
  }

 private:
  std::list<T*> internal_list_;

  DISALLOW_COPY_AND_ASSIGN(PtrList);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_PTR_LIST_HPP_
