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

#ifndef QUICKSTEP_UTILITY_CONTAINER_COMPAT_HPP_
#define QUICKSTEP_UTILITY_CONTAINER_COMPAT_HPP_

#include "utility/CstdintCompat.hpp"
#include "utility/UtilityConfig.h"

// The following lint exemptions are for include order. We need to include
// UtilityConfig.h first to know what else to include.

#ifdef QUICKSTEP_HAVE_UNORDERED_SET
#include <functional>     // NOLINT
#include <unordered_set>  // NOLINT
#include <utility>        // NOLINT
#else
#include <set>            // NOLINT
#endif

#ifdef QUICKSTEP_HAVE_UNORDERED_MAP
#include <unordered_map>  // NOLINT
#else
#include <map>            // NOLINT
#endif

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

#ifdef QUICKSTEP_HAVE_UNORDERED_SET
/**
 * @brief A hash function for std::pair, based on Boost's hash_combine.
 * @note This exists because C++11 does not support hashing pairs directly.
 **/
template <typename T1, typename T2>
class PairHasher {
 public:
#ifdef QUICKSTEP_SIZE_T_64BIT
  static const std::size_t kGoldenInverse = UINT64_C(0x9e3779b97f4a7c15);
#else
  static const std::size_t kGoldenInverse = 0x9e3779b9U;
#endif

  std::size_t operator()(const std::pair<T1, T2> &arg) const {
    std::size_t hash_val = std::hash<T1>()(arg.first);
    // TODO(chasseur): These shifts may not be optimal for 64-bit size_t.
    hash_val ^= std::hash<T2>()(arg.second) + kGoldenInverse + (hash_val << 6) + (hash_val >> 2);
    return hash_val;
  }
};

/**
 * @brief Template wrapper for unordered_set when C++11 is supported, vanilla
 *        set otherwise. Should be used in any case a set is needed and a
 *        specific order is not required.
 **/
template <typename T>
class CompatUnorderedSet {
 public:
  typedef std::unordered_set<T> unordered_set;
};

// Specialization for pairs:
template <typename T1, typename T2>
class CompatUnorderedSet<std::pair<T1, T2> > {
 public:
  typedef std::unordered_set<std::pair<T1, T2>, PairHasher<T1, T2> > unordered_set;
};

#else
template <typename T>
class CompatUnorderedSet {
 public:
  typedef std::set<T> unordered_set;
};
#endif

/**
 * @brief Template wrapper for unordered_map when C++11 is supported, vanilla
 *        map otherwise. Should be used in any case a map is needed and a
 *        specific order is not required.
 **/
template <typename K, typename V>
class CompatUnorderedMap {
 public:
#ifdef QUICKSTEP_HAVE_UNORDERED_MAP
  typedef std::unordered_map<K, V> unordered_map;
#else
  typedef std::map<K, V> unordered_map;
#endif
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_CONTAINER_COMPAT_HPP_
