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

#ifndef QUICKSTEP_UTILITY_BIT_VECTOR_HPP_
#define QUICKSTEP_UTILITY_BIT_VECTOR_HPP_

#include <cstddef>
#include <cstring>

#include "utility/BitManipulation.hpp"
#include "utility/Macros.hpp"
#include "utility/UtilityConfig.h"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief An interface for using a region of memory as vector of bits (i.e.
 *        bools).
 **/
class BitVector {
 public:
  /**
   * @brief Constructor
   * @note This constructor can be used to create a new BitVector, or to
   *       reconstitute and access a BitVector which was created earlier. If
   *       creating a new BitVector, clear() should be called after the
   *       constructor.
   *
   * @param memory_location The location of memory to use for the BitVector.
   *        Use BytesNeeded() to determine how much memory is needed.
   * @param num_bits The length of the BitVector in bits.
   **/
  BitVector(void *memory_location, const std::size_t num_bits)
      : data_array_(static_cast<std::size_t*>(memory_location)),
        num_bits_(num_bits),
        data_array_size_((num_bits >> kHigherOrderShift) + (num_bits & kLowerOrderMask ? 1 : 0)) {
    DEBUG_ASSERT(data_array_ != NULL);
    DEBUG_ASSERT(num_bits_ > 0);
  }

  /**
   * @brief Calculate the number of bytes needed to store a bit vector of the
   *        given number of bits.
   *
   * @param num_bits The desired length of a BitVector in bits.
   * @return The number of bytes needed for the BitVector.
   **/
  static std::size_t BytesNeeded(const std::size_t num_bits) {
    if (num_bits & kLowerOrderMask) {
      return ((num_bits >> kHigherOrderShift) + 1) * sizeof(std::size_t);
    } else {
      return (num_bits >> kHigherOrderShift) * sizeof(std::size_t);
    }
  }

  /**
   * @brief Get the length of this BitVector in bits.
   *
   * @return The length of this BitVector in bits.
   **/
  inline std::size_t size() const {
    return num_bits_;
  }

  /**
   * @brief Clear this BitVector, setting all bits to zero.
   **/
  void clear() {
    std::memset(data_array_, 0, BytesNeeded(num_bits_));
  }

  /**
   * @brief Get the value of a single bit.
   *
   * @param bit_num The desired bit in this BitVector.
   * @return The value of the bit at bit_num.
   **/
  inline bool getBit(const std::size_t bit_num) const {
    DEBUG_ASSERT(bit_num < num_bits_);
    return (data_array_[bit_num >> kHigherOrderShift] >> (bit_num & kLowerOrderMask)) & 0x1;
  }

  /**
   * @brief Set the value of a single bit.
   *
   * @param bit_num The desired bit in this BitVector.
   * @param value The new value to set for the bit at bit_num.
   **/
  inline void setBit(const std::size_t bit_num, bool value) {
    DEBUG_ASSERT(bit_num < num_bits_);
    if (value) {
      data_array_[bit_num >> kHigherOrderShift]
          |= (static_cast<std::size_t>(0x1) << (bit_num & kLowerOrderMask));
    } else {
      data_array_[bit_num >> kHigherOrderShift]
          &= ~(static_cast<std::size_t>(0x1) << (bit_num & kLowerOrderMask));
    }
  }

  /**
   * @brief Count the total number of 1-bits in this BitVector.
   *
   * @return The number of ones in this BitVector.
   **/
  std::size_t onesCount() const {
    std::size_t count = 0;
    for (std::size_t position = 0;
         position < data_array_size_;
         ++position) {
#ifdef QUICKSTEP_SIZE_T_64BIT
      count += population_count_64(data_array_[position]);
#else
      count += population_count_32(data_array_[position]);
#endif
    }

    return count;
  }

  /**
   * @brief Find the first 1-bit (at or after the specified position) in this
   *        BitVector.
   *
   *
   * @param position The first bit to search for a one.
   * @return The position of the first one (at or after position) in this
   *         BitVector.
   **/
  std::size_t firstOne(std::size_t position = 0) const {
    DEBUG_ASSERT(position < num_bits_);
    if (position & kLowerOrderMask) {
      if ((position >> kHigherOrderShift) == data_array_size_ - 1) {
        return firstOneSlow(position);
      } else {
        std::size_t stride_end = (position & ~kLowerOrderMask) + kSizeTBits;
        for (;
             position < stride_end;
             ++position) {
          if (getBit(position)) {
            return position;
          }
        }
      }
    }

    for (std::size_t array_idx = position >> kHigherOrderShift;
         array_idx < data_array_size_;
         ++array_idx) {
      if (data_array_[array_idx]) {
        return (array_idx << kHigherOrderShift)
#ifdef QUICKSTEP_SIZE_T_64BIT
               + trailing_zero_count_64(data_array_[array_idx]);
#else
               + trailing_zero_count_32(data_array_[array_idx]);
#endif
      }
    }

    return num_bits_;
  }

  /**
   * @brief Find the first 0-bit (at or after the specified position) in this
   *        BitVector.
   *
   *
   * @param position The first bit to search for a zero.
   * @return The position of the first zero (at or after position) in this
   *         BitVector.
   **/
  std::size_t firstZero(std::size_t position = 0) const {
    DEBUG_ASSERT(position < num_bits_);
    if (position & kLowerOrderMask) {
      if ((position >> kHigherOrderShift) == data_array_size_ - 1) {
        return firstZeroSlow(position);
      } else {
        std::size_t stride_end = (position & ~kLowerOrderMask) + kSizeTBits;
        for (;
             position < stride_end;
             ++position) {
          if (!getBit(position)) {
            return position;
          }
        }
      }
    }

    for (std::size_t array_idx = position >> kHigherOrderShift;
         array_idx < data_array_size_ - ((num_bits_ & kLowerOrderMask) ? 1 : 0);
         ++array_idx) {
      std::size_t inverse = ~data_array_[array_idx];
      if (inverse) {
        return (array_idx << kHigherOrderShift)
#ifdef QUICKSTEP_SIZE_T_64BIT
               + trailing_zero_count_64(inverse);
#else
               + trailing_zero_count_32(inverse);
#endif
      }
    }

    if (num_bits_ & kLowerOrderMask) {
      position = (data_array_size_ - 1) << kHigherOrderShift;
    } else {
      return num_bits_;
    }

    return firstZeroSlow(position);
  }

 private:
  // This works as long as the bit-width of size_t is power of 2:
  static const std::size_t kLowerOrderMask = (sizeof(std::size_t) << 3) - 1;
  // This works for 32-bit or 64-bit size_t:
  static const std::size_t kHigherOrderShift = sizeof(std::size_t) == 4 ? 5 : 6;
  static const std::size_t kSizeTBits = sizeof(std::size_t) << 3;

  inline std::size_t firstOneSlow(std::size_t position) const {
    for (; position < num_bits_; ++position) {
      if (getBit(position)) {
        return position;
      }
    }

    return position;
  }

  inline std::size_t firstZeroSlow(std::size_t position) const {
    for (; position < num_bits_; ++position) {
      if (!getBit(position)) {
        return position;
      }
    }

    return position;
  }

  std::size_t *data_array_;
  const std::size_t num_bits_;
  const std::size_t data_array_size_;

  DISALLOW_COPY_AND_ASSIGN(BitVector);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_BIT_VECTOR_HPP_
