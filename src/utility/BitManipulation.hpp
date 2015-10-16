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

#ifndef QUICKSTEP_UTILITY_BIT_MANIPULATION_HPP_
#define QUICKSTEP_UTILITY_BIT_MANIPULATION_HPP_

#include "utility/CstdintCompat.hpp"
#include "utility/UtilityConfig.h"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief Count the number of ones in a 32-bit word, using a fast assembly
 *        instruction if possible.
 *
 * @param word A 32-bit word to count ones in.
 * @return The number of 1-bits in word.
 **/
inline int population_count_32(std::uint32_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_POPCOUNT
  return __builtin_popcount(word);
#else
  int count = 0;
  while (word) {
    if (word & 0x1U) {
      ++count;
    }
    word >>= 1;
  }
  return count;
#endif
}

/**
 * @brief Count the number of ones in a 64-bit word, using a fast assembly
 *        instruction if possible.
 *
 * @param word A 64-bit word to count ones in.
 * @return The number of 1-bits in word.
 **/
#ifdef QUICKSTEP_LONG_64BIT
inline int population_count_64(std::uint64_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_POPCOUNT
  return __builtin_popcountl(word);
#else
  int count = 0;
  while (word) {
    if (word & 0x1U) {
      ++count;
    }
    word >>= 1;
  }
  return count;
#endif
}
#else
inline int population_count_64(std::uint64_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_POPCOUNT
  return __builtin_popcountll(word);
#else
  int count = 0;
  while (word) {
    if (word & 0x1U) {
      ++count;
    }
    word >>= 1;
  }
  return count;
#endif
}
#endif


/**
 * @brief Count the number of leading zeroes before the first one in a 32-bit
 *        word, using a fast assembly instruction if possible.
 * @note This can also be used to count leading ones before the first zero by
 *       bit-flipping word as ~word.
 * @warning The result is undefined if word is zero.
 *
 * @param word A 32-bit word to count leading zeroes in.
 * @return The number leading 0-bits before the first 1-bit in word.
 **/
inline int leading_zero_count_32(std::uint32_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_CLZ
  return __builtin_clz(word);
#else
  if (word) {
    int count = 0;
    while (!(word & 0x80000000U)) {
      ++count;
      word <<= 1;
    }
    return count;
  } else {
    return 32;
  }
#endif
}

/**
 * @brief Count the number of leading zeroes before the first one in a 64-bit
 *        word, using a fast assembly instruction if possible.
 * @note This can also be used to count leading ones before the first zero by
 *       bit-flipping word as ~word.
 * @warning The result is undefined if word is zero.
 *
 * @param word A 64-bit word to count leading zeroes in.
 * @return The number leading 0-bits before the first 1-bit in word.
 **/
#ifdef QUICKSTEP_LONG_64BIT
inline int leading_zero_count_64(std::uint64_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_CLZ
  return __builtin_clzl(word);
#else
  if (word) {
    int count = 0;
    while (!(word & 0x8000000000000000UL)) {
      ++count;
      word <<= 1;
    }
    return count;
  } else {
    return 64;
  }
#endif
}
#else
inline int leading_zero_count_64(std::uint64_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_CLZ
  return __builtin_clzll(word);
#else
  if (word) {
    int count = 0;
    while (!(word & 0x8000000000000000ULL)) {
      ++count;
      word <<= 1;
    }
    return count;
  } else {
    return 64;
  }
#endif
}
#endif


/**
 * @brief Count the number of trailing zeroes behind the last one in a 32-bit
 *        word, using a fast assembly instruction if possible.
 * @note This can also be used to count trailing ones behind the first zero by
 *       bit-flipping word as ~word.
 * @warning The result is undefined if word is zero.
 *
 * @param word A 32-bit word to count trailing zeroes in.
 * @return The number trailing 0-bits behind the last 1-bit in word.
 **/
inline int trailing_zero_count_32(std::uint32_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_CTZ
  return __builtin_ctz(word);
#else
  if (word) {
    int count = 0;
    while (!(word & 0x1U)) {
      ++count;
      word >>= 1;
    }
    return count;
  } else {
    return 32;
  }
#endif
}

/**
 * @brief Count the number of trailing zeroes behind the last one in a 64-bit
 *        word, using a fast assembly instruction if possible.
 * @note This can also be used to count trailing ones behind the first zero by
 *       bit-flipping word as ~word.
 * @warning The result is undefined if word is zero.
 *
 * @param word A 64-bit word to count trailing zeroes in.
 * @return The number trailing 0-bits behind the last 1-bit in word.
 **/
#ifdef QUICKSTEP_LONG_64BIT
inline int trailing_zero_count_64(std::uint64_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_CTZ
  return __builtin_ctzl(word);
#else
  if (word) {
    int count = 0;
    while (!(word & 0x1U)) {
      ++count;
      word >>= 1;
    }
    return count;
  } else {
    return 64;
  }
#endif
}
#else
inline int trailing_zero_count_64(std::uint64_t word) {
#ifdef QUICKSTEP_HAVE_BUILTIN_CTZ
  return __builtin_ctzll(word);
#else
  if (word) {
    int count = 0;
    while (!(word & 0x1U)) {
      ++count;
      word >>= 1;
    }
    return count;
  } else {
    return 64;
  }
#endif
}
#endif

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_BIT_MANIPULATION_HPP_
