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

#ifndef QUICKSTEP_TYPES_ASCII_STRING_COMPARATORS_HPP_
#define QUICKSTEP_TYPES_ASCII_STRING_COMPARATORS_HPP_

#include <cstddef>
#include <cstring>
#include <functional>

#include "types/strnlen.hpp"
#include "types/TypeInstance.hpp"
#include "utility/Macros.hpp"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

/**
 * @brief Base class for UncheckedComparators which compare strings.
 **/
template <template <typename T> class ComparisonFunctor,  // NOLINT - ComparisonFunctor is not a real class
          bool left_nullable, bool left_null_terminated, bool left_longer,
          bool right_nullable, bool right_null_terminated, bool right_longer>
class AsciiStringUncheckedComparator : public UncheckedComparator {
 public:
  /**
   * @brief Constructor.
   *
   * @param left_length The maximum length of the left string to compare.
   * @param right_length The maximum length of the right string to compare.
   **/
  AsciiStringUncheckedComparator(const std::size_t left_length, const std::size_t right_length)
      : left_length_(left_length), right_length_(right_length) {
  }

  virtual ~AsciiStringUncheckedComparator() {
  }

  inline bool compareTypeInstances(const TypeInstance &left, const TypeInstance &right) const {
    return compareDataPtrs(left.getDataPtr(), right.getDataPtr());
  }

  inline bool compareDataPtrs(const void *left, const void *right) const {
    // Any comparison with NULL is false.
    if ((left_nullable && (left == NULL)) || (right_nullable && (right == NULL))) {
      return false;
    } else {
      return comparison_functor_(strcmpHelper(static_cast<const char*>(left), static_cast<const char*>(right)), 0);
    }
  }

  inline bool compareTypeInstanceWithDataPtr(const TypeInstance &left, const void *right) const {
    return compareDataPtrs(left.getDataPtr(), right);
  }

  inline bool compareDataPtrWithTypeInstance(const void *left, const TypeInstance &right) const {
    return compareDataPtrs(left, right.getDataPtr());
  }

 private:
  /**
   * @brief Compare left and right strings like strcmp, but safely handle cases
   *        where one of the strings might not be NULL-terminated.
   *
   * @param left The left string to compare.
   * @param right The right string to compare.
   **/
  inline int strcmpHelper(const char *left, const char *right) const {
    DEBUG_ASSERT(left != NULL);
    DEBUG_ASSERT(right != NULL);

    if (left_null_terminated && right_null_terminated) {
      return std::strcmp(left, right);
    } else {
      if (right_longer) {
        int res = std::strncmp(left, right, left_length_);
        if (res) {
          return res;
        } else {
          if (strnlen(right, right_length_) > left_length_) {
            return -1;
          } else {
            return res;
          }
        }
      } else if (left_longer) {
        int res = std::strncmp(left, right, right_length_);
        if (res) {
          return res;
        } else {
          if (strnlen(left, left_length_) > right_length_) {
            return 1;
          } else {
            return res;
          }
        }
      } else {
        return std::strncmp(left, right, left_length_);
      }
    }
  }

  std::size_t left_length_, right_length_;
  ComparisonFunctor<int> comparison_functor_;

  DISALLOW_COPY_AND_ASSIGN(AsciiStringUncheckedComparator);
};

/**
 * @brief The equals UncheckedComparator for strings.
 **/
template <bool left_nullable, bool left_null_terminated, bool left_longer,
          bool right_nullable, bool right_null_terminated, bool right_longer>
class EqualAsciiStringUncheckedComparator
    : public AsciiStringUncheckedComparator<std::equal_to,
                                            left_nullable, left_null_terminated, left_longer,
                                            right_nullable, right_null_terminated, right_longer> {
 public:
  /**
   * @brief Constructor
   *
   * @param left_length The maximum length of the left string to compare.
   * @param right_length The maximum length of the right string to compare.
   **/
  EqualAsciiStringUncheckedComparator(const std::size_t left_length, const std::size_t right_length)
      : AsciiStringUncheckedComparator<std::equal_to,
                                       left_nullable, left_null_terminated, left_longer,
                                       right_nullable, right_null_terminated, right_longer>(left_length,
                                                                                            right_length) {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(EqualAsciiStringUncheckedComparator);
};

/**
 * @brief The not-equal UncheckedComparator for strings.
 **/
template <bool left_nullable, bool left_null_terminated, bool left_longer,
          bool right_nullable, bool right_null_terminated, bool right_longer>
class NotEqualAsciiStringUncheckedComparator
    : public AsciiStringUncheckedComparator<std::not_equal_to,
                                            left_nullable, left_null_terminated, left_longer,
                                            right_nullable, right_null_terminated, right_longer> {
 public:
  /**
   * @brief Constructor.
   *
   * @param left_length The maximum length of the left string to compare.
   * @param right_length The maximum length of the right string to compare.
   **/
  NotEqualAsciiStringUncheckedComparator(const std::size_t left_length, const std::size_t right_length)
      : AsciiStringUncheckedComparator<std::not_equal_to,
                                       left_nullable, left_null_terminated, left_longer,
                                       right_nullable, right_null_terminated, right_longer>(left_length,
                                                                                            right_length) {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(NotEqualAsciiStringUncheckedComparator);
};

/**
 * @brief The less-than UncheckedComparator for strings.
 **/
template <bool left_nullable, bool left_null_terminated, bool left_longer,
          bool right_nullable, bool right_null_terminated, bool right_longer>
class LessAsciiStringUncheckedComparator
    : public AsciiStringUncheckedComparator<std::less,
                                            left_nullable, left_null_terminated, left_longer,
                                            right_nullable, right_null_terminated, right_longer> {
 public:
  /**
   * @brief Constructor.
   *
   * @param left_length The maximum length of the left string to compare.
   * @param right_length The maximum length of the right string to compare.
   **/
  LessAsciiStringUncheckedComparator(const std::size_t left_length, const std::size_t right_length)
      : AsciiStringUncheckedComparator<std::less,
                                       left_nullable, left_null_terminated, left_longer,
                                       right_nullable, right_null_terminated, right_longer>(left_length,
                                                                                            right_length) {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LessAsciiStringUncheckedComparator);
};

/**
 * @brief The less-than-or-equal UncheckedComparator for strings.
 **/
template <bool left_nullable, bool left_null_terminated, bool left_longer,
          bool right_nullable, bool right_null_terminated, bool right_longer>
class LessOrEqualAsciiStringUncheckedComparator
    : public AsciiStringUncheckedComparator<std::less_equal,
                                            left_nullable, left_null_terminated, left_longer,
                                            right_nullable, right_null_terminated, right_longer> {
 public:
  /**
   * @brief Constructor.
   *
   * @param left_length The maximum length of the left string to compare.
   * @param right_length The maximum length of the right string to compare.
   **/
  LessOrEqualAsciiStringUncheckedComparator(const std::size_t left_length, const std::size_t right_length)
      : AsciiStringUncheckedComparator<std::less_equal,
                                       left_nullable, left_null_terminated, left_longer,
                                       right_nullable, right_null_terminated, right_longer>(left_length,
                                                                                            right_length) {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(LessOrEqualAsciiStringUncheckedComparator);
};

/**
 * @brief The greater-than UncheckedComparator for strings.
 **/
template <bool left_nullable, bool left_null_terminated, bool left_longer,
          bool right_nullable, bool right_null_terminated, bool right_longer>
class GreaterAsciiStringUncheckedComparator
    : public AsciiStringUncheckedComparator<std::greater,
                                            left_nullable, left_null_terminated, left_longer,
                                            right_nullable, right_null_terminated, right_longer> {
 public:
  /**
   * @brief Constructor.
   *
   * @param left_length The maximum length of the left string to compare.
   * @param right_length The maximum length of the right string to compare.
   **/
  GreaterAsciiStringUncheckedComparator(const std::size_t left_length, const std::size_t right_length)
      : AsciiStringUncheckedComparator<std::greater,
                                       left_nullable, left_null_terminated, left_longer,
                                       right_nullable, right_null_terminated, right_longer>(left_length,
                                                                                            right_length) {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(GreaterAsciiStringUncheckedComparator);
};

/**
 * @brief The greater-than-or-equal UncheckedComparator for strings.
 **/
template <bool left_nullable, bool left_null_terminated, bool left_longer,
          bool right_nullable, bool right_null_terminated, bool right_longer>
class GreaterOrEqualAsciiStringUncheckedComparator
    : public AsciiStringUncheckedComparator<std::greater_equal,
                                            left_nullable, left_null_terminated, left_longer,
                                            right_nullable, right_null_terminated, right_longer> {
 public:
  /**
   * @brief Constructor.
   *
   * @param left_length The maximum length of the left string to compare.
   * @param right_length The maximum length of the right string to compare.
   **/
  GreaterOrEqualAsciiStringUncheckedComparator(const std::size_t left_length, const std::size_t right_length)
      : AsciiStringUncheckedComparator<std::greater_equal,
                                       left_nullable, left_null_terminated, left_longer,
                                       right_nullable, right_null_terminated, right_longer>(left_length,
                                                                                            right_length) {
  }

 private:
  DISALLOW_COPY_AND_ASSIGN(GreaterOrEqualAsciiStringUncheckedComparator);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_ASCII_STRING_COMPARATORS_HPP_
