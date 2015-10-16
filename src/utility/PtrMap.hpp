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

#ifndef QUICKSTEP_UTILITY_PTR_MAP_HPP_
#define QUICKSTEP_UTILITY_PTR_MAP_HPP_

#include <stdexcept>
#include <utility>

#include "utility/ContainerCompat.hpp"

namespace quickstep {

/** \addtogroup Utility
 *  @{
 */

/**
 * @brief A map (unordered_map where possible) whose values are pointers which
 *        are automatically deleted when the map goes out of scope.
 * @note Iterators are regular iterators from the underlying map type, meaning
 *       that they are iterators of pair<const K, V*>.
 **/
template <typename K, typename V>
class PtrMap {
 public:
  typedef typename CompatUnorderedMap<K, V*>::unordered_map::iterator iterator;
  typedef typename CompatUnorderedMap<K, V*>::unordered_map::const_iterator const_iterator;
  typedef typename CompatUnorderedMap<K, V*>::unordered_map::size_type size_type;

  PtrMap() {
  }

  ~PtrMap() {
    for (typename CompatUnorderedMap<K, V*>::unordered_map::iterator it = internal_map_.begin();
         it != internal_map_.end();
         ++it) {
      delete it->second;
    }
  }

  inline bool empty() const {
    return internal_map_.empty();
  }

  inline size_type size() const {
    return internal_map_.size();
  }

  // Iterators.
  iterator begin() {
    return internal_map_.begin();
  }

  const_iterator begin() const {
    return internal_map_.begin();
  }

  iterator end() {
    return internal_map_.end();
  }

  const_iterator end() const {
    return internal_map_.end();
  }

  // Insertion.
  std::pair<iterator, bool> insert(const K &key, V *value) {
    return internal_map_.insert(std::pair<const K, V*>(key, value));
  }

  // Lookup.
  inline iterator find(const K &key) {
    return internal_map_.find(key);
  }

  inline const_iterator find(const K &key) const {
    return internal_map_.find(key);
  }

  // Element access.
  inline V& at(const K &key) {
    iterator it = find(key);
    if (it != end()) {
      return *(it->second);
    } else {
      throw std::out_of_range("PtrMap::at() with nonexistent key");
    }
  }

  inline const V& at(const K &key) const {
    const_iterator it = find(key);
    if (it != end()) {
      return *(it->second);
    } else {
      throw std::out_of_range("PtrMap::at() with nonexistent key");
    }
  }

 private:
  typename CompatUnorderedMap<K, V*>::unordered_map internal_map_;

  DISALLOW_COPY_AND_ASSIGN(PtrMap);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_UTILITY_PTR_MAP_HPP_
