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

#ifndef QUICKSTEP_UTILITY_PTR_VECTOR_HPP_
#define QUICKSTEP_UTILITY_PTR_VECTOR_HPP_

#include <iterator>
#include <stdexcept>
#include <vector>

#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief A vector of pointers to objects, which are automatically deleted when
 *        the PtrVector goes out of scope.
 **/
template <typename T, bool null_allowed = false>
class PtrVector {
 public:
  class PtrVectorConstIterator;

  /**
   * @brief Iterator over the contents of a PtrVector.
   * @warning If null_allowed is true, then always check that isNull() is false
   *          before attempting to dereference.
   **/
  class PtrVectorIterator : public std::iterator<std::random_access_iterator_tag, T> {
   public:
    typedef typename std::iterator<std::random_access_iterator_tag, T>::difference_type difference_type;

    PtrVectorIterator() {
    }

    PtrVectorIterator(const PtrVectorIterator& other)
        : internal_iterator_(other.internal_iterator_) {
    }

    PtrVectorIterator& operator=(const PtrVectorIterator& other) {
      if (this != &other) {
        internal_iterator_ = other.internal_iterator_;
      }
      return *this;
    }

    // Comparisons.
    inline bool operator==(const PtrVectorIterator& other) const {
      return internal_iterator_ == other.internal_iterator_;
    }

    inline bool operator!=(const PtrVectorIterator& other) const {
      return internal_iterator_ != other.internal_iterator_;
    }

    inline bool operator<(const PtrVectorIterator& other) const {
      return internal_iterator_ < other.internal_iterator_;
    }

    inline bool operator<=(const PtrVectorIterator& other) const {
      return internal_iterator_ <= other.internal_iterator_;
    }

    inline bool operator>(const PtrVectorIterator& other) const {
      return internal_iterator_ > other.internal_iterator_;
    }

    inline bool operator>=(const PtrVectorIterator& other) const {
      return internal_iterator_ >= other.internal_iterator_;
    }

    // Increment/decrement.
    inline PtrVectorIterator& operator++() {
      ++internal_iterator_;
      return *this;
    }

    PtrVectorIterator operator++(int) {  // NOLINT - increment operator doesn't need named param
      PtrVectorIterator result(*this);
      ++(*this);
      return result;
    }

    inline PtrVectorIterator& operator--() {
      --internal_iterator_;
      return *this;
    }

    PtrVectorIterator operator--(int) {  // NOLINT - decrement operator doesn't need named param
      PtrVectorIterator result(*this);
      --(*this);
      return result;
    }

    // Compound assignment.
    inline PtrVectorIterator& operator+=(difference_type n) {
      internal_iterator_ += n;
      return *this;
    }

    inline PtrVectorIterator& operator-=(difference_type n) {
      internal_iterator_ -= n;
      return *this;
    }

    // Note: + operator with difference_type on the left is defined out-of-line.
    PtrVectorIterator operator+(difference_type n) const {
      return PtrVectorIterator(internal_iterator_ + n);
    }

    PtrVectorIterator operator-(difference_type n) const {
      return PtrVectorIterator(internal_iterator_ - n);
    }

    difference_type operator-(const PtrVectorIterator &other) const {
      return PtrVectorIterator(internal_iterator_ - other.internal_iterator_);
    }

    // Dereference.
    /**
     * @brief Check whether the element at the current iterator position is
     *        NULL.
     * @return Whether the element at the current iterator position is NULL.
     **/
    inline bool isNull() const {
      return (null_allowed && (*internal_iterator_ == NULL));
    }

    inline T& operator*() const {
      if (null_allowed) {
        DEBUG_ASSERT(!isNull());
      }
      return **internal_iterator_;
    }

    inline T* operator->() const {
      if (null_allowed) {
        DEBUG_ASSERT(!isNull());
      }
      return *internal_iterator_;
    }

    // Offset dereference. Potentially unsafe if null_allowed.
    T& operator[](difference_type n) const {
      if (null_allowed) {
        DEBUG_ASSERT(internal_iterator_[n] != NULL);
      }
      return *(internal_iterator_[n]);
    }

   private:
    explicit PtrVectorIterator(const typename std::vector<T*>::iterator &internal_iterator)
        : internal_iterator_(internal_iterator) {
    }

    typename std::vector<T*>::iterator internal_iterator_;

    friend class PtrVector;
    friend class PtrVectorConstIterator;
  };

  /**
   * @brief Const iterator over the contents of a PtrVector.
   * @warning If null_allowed is true, then always check that isNull() is false
   *          before attempting to dereference.
   **/
  class PtrVectorConstIterator : public std::iterator<std::input_iterator_tag, const T> {
   public:
    typedef typename std::iterator<std::input_iterator_tag, T>::difference_type difference_type;

    PtrVectorConstIterator() {
    }

    PtrVectorConstIterator(const PtrVectorConstIterator& other)
        : internal_iterator_(other.internal_iterator_) {
    }

    PtrVectorConstIterator(const PtrVectorIterator& other)  // NOLINT - allow implicit conversion.
        : internal_iterator_(other.internal_iterator_) {
    }

    PtrVectorConstIterator& operator=(const PtrVectorConstIterator& other) {
      if (this != &other) {
        internal_iterator_ = other.internal_iterator_;
      }
      return *this;
    }

    PtrVectorConstIterator& operator=(const PtrVectorIterator& other) {
      internal_iterator_ = other.internal_iterator_;
      return *this;
    }

    // Comparisons.
    inline bool operator==(const PtrVectorConstIterator& other) const {
      return internal_iterator_ == other.internal_iterator_;
    }

    inline bool operator!=(const PtrVectorConstIterator& other) const {
      return internal_iterator_ != other.internal_iterator_;
    }

    inline bool operator<(const PtrVectorConstIterator& other) const {
      return internal_iterator_ < other.internal_iterator_;
    }

    inline bool operator<=(const PtrVectorConstIterator& other) const {
      return internal_iterator_ <= other.internal_iterator_;
    }

    inline bool operator>(const PtrVectorConstIterator& other) const {
      return internal_iterator_ > other.internal_iterator_;
    }

    inline bool operator>=(const PtrVectorConstIterator& other) const {
      return internal_iterator_ >= other.internal_iterator_;
    }

    // Increment/decrement.
    inline PtrVectorConstIterator& operator++() {
      ++internal_iterator_;
      return *this;
    }

    PtrVectorConstIterator operator++(int) {  // NOLINT - increment operator doesn't need named param
      PtrVectorConstIterator result(*this);
      ++(*this);
      return result;
    }

    inline PtrVectorConstIterator& operator--() {
      --internal_iterator_;
      return *this;
    }

    PtrVectorConstIterator operator--(int) {  // NOLINT - decrement operator doesn't need named param
      PtrVectorConstIterator result(*this);
      --(*this);
      return result;
    }

    // Compound assignment.
    inline PtrVectorConstIterator& operator+=(difference_type n) {
      internal_iterator_ += n;
      return *this;
    }

    inline PtrVectorConstIterator& operator-=(difference_type n) {
      internal_iterator_ -= n;
      return *this;
    }

    // Note: + operator with difference_type on the left is defined out-of-line.
    PtrVectorConstIterator operator+(difference_type n) const {
      return PtrVectorConstIterator(internal_iterator_ + n);
    }

    PtrVectorConstIterator operator-(difference_type n) const {
      return PtrVectorConstIterator(internal_iterator_ - n);
    }

    difference_type operator-(const PtrVectorConstIterator &other) const {
      return PtrVectorConstIterator(internal_iterator_ - other.internal_iterator_);
    }

    // Dereference.
    /**
     * @brief Check whether the element at the current iterator position is
     *        NULL.
     * @return Whether the element at the current iterator position is NULL.
     **/
    inline bool isNull() const {
      return (null_allowed && (*internal_iterator_ == NULL));
    }

    inline const T& operator*() const {
      if (null_allowed) {
        DEBUG_ASSERT(!isNull());
      }
      return **internal_iterator_;
    }

    inline const T* operator->() const {
      if (null_allowed) {
        DEBUG_ASSERT(!isNull());
      }
      return *internal_iterator_;
    }

    // Offset dereference. Potentially unsafe if null_allowed.
    const T& operator[](difference_type n) const {
      if (null_allowed) {
        DEBUG_ASSERT(internal_iterator_[n] != NULL);
      }
      return *(internal_iterator_[n]);
    }

   private:
    explicit PtrVectorConstIterator(const typename std::vector<T*>::const_iterator &internal_iterator)
        : internal_iterator_(internal_iterator) {
    }

    typename std::vector<T*>::const_iterator internal_iterator_;

    friend class PtrVector;
  };

  /**
   * @brief Input iterator over the contents of a PtrVector which automatically
   *        skips over NULL entries.
   **/
  class PtrVectorConstSkipIterator : public std::iterator<std::input_iterator_tag, const T> {
   public:
    typedef typename std::iterator<std::input_iterator_tag, T>::difference_type difference_type;

    PtrVectorConstSkipIterator()
        : parent_vector_(NULL) {
    }

    PtrVectorConstSkipIterator(const PtrVectorConstSkipIterator& other)
        : internal_iterator_(other.internal_iterator_),
          parent_vector_(other.parent_vector_) {
    }

    PtrVectorConstSkipIterator& operator=(const PtrVectorConstSkipIterator& other) {
      if (this != &other) {
        internal_iterator_ = other.internal_iterator_;
        parent_vector_ = other.parent_vector_;
      }
      return *this;
    }

    // Comparisons.
    inline bool operator==(const PtrVectorConstSkipIterator& other) const {
      return internal_iterator_ == other.internal_iterator_;
    }

    inline bool operator!=(const PtrVectorConstSkipIterator& other) const {
      return internal_iterator_ != other.internal_iterator_;
    }

    inline bool operator<(const PtrVectorConstSkipIterator& other) const {
      return internal_iterator_ < other.internal_iterator_;
    }

    inline bool operator<=(const PtrVectorConstSkipIterator& other) const {
      return internal_iterator_ <= other.internal_iterator_;
    }

    inline bool operator>(const PtrVectorConstSkipIterator& other) const {
      return internal_iterator_ > other.internal_iterator_;
    }

    inline bool operator>=(const PtrVectorConstSkipIterator& other) const {
      return internal_iterator_ >= other.internal_iterator_;
    }

    // Increment/decrement.
    inline PtrVectorConstSkipIterator& operator++() {
      ++internal_iterator_;
      while (internal_iterator_ != parent_vector_->end()) {
        if (*internal_iterator_ != NULL) {
          break;
        }
        ++internal_iterator_;
      }
      return *this;
    }

    PtrVectorConstSkipIterator operator++(int) {  // NOLINT - increment operator doesn't need named param
      PtrVectorConstSkipIterator result(*this);
      ++(*this);
      return result;
    }

    inline const T& operator*() const {
      return **internal_iterator_;
    }

    inline const T* operator->() const {
      return *internal_iterator_;
    }

   private:
    explicit PtrVectorConstSkipIterator(const typename std::vector<T*>::const_iterator &internal_iterator,
                                        const std::vector<T*> *parent_vector)
        : internal_iterator_(internal_iterator), parent_vector_(parent_vector) {
      while ((internal_iterator_ != parent_vector_->end()) && (*internal_iterator_ == NULL)) {
        ++internal_iterator_;
      }
    }

    typename std::vector<T*>::const_iterator internal_iterator_;
    const std::vector<T*> *parent_vector_;

    friend class PtrVector;
  };

  typedef typename std::vector<T*>::size_type size_type;
  typedef T value_type;
  typedef PtrVectorIterator iterator;
  typedef PtrVectorConstIterator const_iterator;
  typedef PtrVectorConstSkipIterator const_skip_iterator;

  PtrVector() {
  }

  ~PtrVector() {
    for (typename std::vector<T*>::iterator it = internal_vector_.begin();
         it != internal_vector_.end();
         ++it) {
      if (!null_allowed || (*it != NULL)) {
        delete *it;
      }
    }
  }

  inline size_type size() const {
    return internal_vector_.size();
  }

  inline size_type max_size() const {
    return internal_vector_.max_size();
  }

  inline size_type capacity() const {
    return internal_vector_.capacity();
  }

  inline bool empty() const {
    return internal_vector_.empty();
  }

  /**
   * @brief Check whether this vector contains any actual objects. Unlike
   *        empty(), this returns true if the vector has some elements, but
   *        they are all NULL.
   *
   * @return Whether this PtrVector is empty of actual objects.
   **/
  bool emptyNullCheck() const {
    if (null_allowed) {
      for (typename std::vector<T*>::const_iterator it = internal_vector_.begin();
           it != internal_vector_.end();
           ++it) {
        if (*it != NULL) {
          return false;
        }
      }
      return true;
    } else {
      return empty();
    }
  }

  inline void reserve(size_type n) {
    internal_vector_.reserve(n);
  }

  // Iterators
  iterator begin() {
    return iterator(internal_vector_.begin());
  }

  iterator end() {
    return iterator(internal_vector_.end());
  }

  const_iterator begin() const {
    return const_iterator(internal_vector_.begin());
  }

  const_iterator end() const {
    return const_iterator(internal_vector_.end());
  }

  /**
   * @brief Get an iterator at the beginning of this PtrVector which
   *        automatically skips over NULL entries.
   * 
   * @return An iterator at the beginning of this PtrVector which automatically
   *         skips over NULL entries.
   **/
  const_skip_iterator begin_skip() const {
    return const_skip_iterator(internal_vector_.begin(), &internal_vector_);
  }

  /**
   * @brief Get an iterator at one past the end of this PtrVector which
   *        automatically skips over NULL entries.
   * 
   * @return An iterator at one past the end of this PtrVector which
   *         automatically skips over NULL entries.
   **/
  const_skip_iterator end_skip() const {
    return const_skip_iterator(internal_vector_.end(), &internal_vector_);
  }

  /**
   * @brief Check whether the element at the specified position is NULL.
   *
   * @param n The position in this PtrVector to check.
   * @return Whether the element at position n is NULL.
   **/
  inline bool elementIsNull(const size_type n) const {
    if (null_allowed && (internal_vector_[n] == NULL)) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * @brief Check whether the element at the specified position is NULL.
   * @note This is similar to elementIsNull(), but uses std::vector::at(),
   *       which throws std::out_of_range exceptions.
   *
   * @param n The position in this PtrVector to check.
   * @return Whether the element at position n is NULL.
   **/
  inline bool elementIsNullAt(const size_type n) const {
    if (null_allowed && (internal_vector_.at(n) == NULL)) {
      return true;
    } else {
      return false;
    }
  }

  inline T& front() {
    if (null_allowed) {
      typename std::vector<T*>::iterator it = internal_vector_.begin();
      while ((it != internal_vector_.end()) && (*it == NULL)) {
        ++it;
      }
      return **it;
    } else {
      return *(internal_vector_.front());
    }
  }

  inline const T& front() const {
    if (null_allowed) {
      typename std::vector<T*>::const_iterator it = internal_vector_.begin();
      while ((it != internal_vector_.end()) && (*it == NULL)) {
        ++it;
      }
      return **it;
    } else {
      return *(internal_vector_.front());
    }
  }

  inline T& back() {
    if (null_allowed) {
      typename std::vector<T*>::reverse_iterator it = internal_vector_.rbegin();
      while ((it != internal_vector_.rend()) && (*it == NULL)) {
        ++it;
      }
      return **it;
    } else {
      return *(internal_vector_.back());
    }
  }

  inline const T& back() const {
    if (null_allowed) {
      typename std::vector<T*>::const_reverse_iterator it = internal_vector_.rbegin();
      while ((it != internal_vector_.rend()) && (*it == NULL)) {
        ++it;
      }
      return **it;
    } else {
      return *(internal_vector_.back());
    }
  }

  inline T& operator[](const size_type n) {
    if (null_allowed) {
      DEBUG_ASSERT(!elementIsNull(n));
    }
    return *(internal_vector_[n]);
  }

  inline const T& operator[](const size_type n) const {
    if (null_allowed) {
      DEBUG_ASSERT(!elementIsNull(n));
    }
    return *(internal_vector_[n]);
  }

  inline T& at(const size_type n) {
    if (null_allowed) {
      DEBUG_ASSERT(!elementIsNullAt(n));
    }
    return *(internal_vector_.at(n));
  }

  inline const T& at(const size_type n) const {
    if (null_allowed) {
      DEBUG_ASSERT(!elementIsNullAt(n));
    }
    return *(internal_vector_.at(n));
  }

  inline void push_back(T *value_ptr) {
    if (!null_allowed) {
      DEBUG_ASSERT(value_ptr != NULL);
    }
    internal_vector_.push_back(value_ptr);
  }

  /**
   * @brief Delete an element and set it to null. Only usable if null_allowed
   *        is true.
   * @param n The position of the element to delete.
   **/
  void deleteElement(const size_type n) {
    DEBUG_ASSERT(null_allowed);
    if (internal_vector_[n] != NULL) {
      delete internal_vector_[n];
      internal_vector_[n] = NULL;
    }
  }

  /**
   * @brief Delete the last element and truncate the vector. Only usable if
   *        null_allowed is false.
   **/
  void removeBack() {
    DEBUG_ASSERT(!null_allowed);
    DEBUG_ASSERT(!internal_vector_.empty());
    delete internal_vector_.back();
    internal_vector_.resize(internal_vector_.size() - 1);
  }

  /**
   * @brief Get a const reference to the internal vector of pointers.
   *
   * @return A const reference to the internal vector of pointers.
   **/
  const std::vector<T*>& getInternalVector() const {
    return internal_vector_;
  }

  /**
   * @brief Get a mutable pointer to the internal vector of pointers.
   * @warning Only call this if you really know what you are doing.
   *
   * @return A mutable pointer to the internal vector of pointers.
   **/
  std::vector<T*>* getInternalVectorMutable() {
    return &internal_vector_;
  }

 private:
  std::vector<T*> internal_vector_;

  DISALLOW_COPY_AND_ASSIGN(PtrVector);
};

template <typename T>
typename PtrVector<T>::PtrVectorIterator operator+(
    const typename PtrVector<T>::PtrVectorIterator::difference_type n,
    const typename PtrVector<T>::PtrVectorIterator &it) {
  return it + n;
}

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_PTR_VECTOR_HPP_
