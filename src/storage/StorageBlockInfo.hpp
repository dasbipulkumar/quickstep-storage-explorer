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

#ifndef QUICKSTEP_STORAGE_STORAGE_BLOCK_INFO_HPP_
#define QUICKSTEP_STORAGE_STORAGE_BLOCK_INFO_HPP_

#include <climits>
#include <cstddef>

namespace quickstep {

class TupleStorageSubBlock;

/** \addtogroup Storage
 *  @{
 */

/**
 * @brief A globally unique identifier for a StorageBlock.
 **/
typedef int block_id;

/**
 * @brief Identifies a single tuple within a StorageBlock.
 **/
typedef int tuple_id;
const tuple_id kAllTuples = -1;  // Special value indicating all tuples in a block.
const tuple_id kMaxTupleID = INT_MAX;

/**
 * @brief Codes for the different implementations of TupleStorageSubBlock.
 **/
enum TupleStorageSubBlockType {
  kPackedRowStore = 0,
  kBasicColumnStore,
  kCompressedPackedRowStore,
  kCompressedColumnStore,
  kNumTupleStorageSubBlockTypes  // Not an actual TupleStorageSubBlockType, exists for counting purposes.
};

/**
 * @brief Names corresponsing to TupleStorageSubBlockType.
 * @note Defined out-of-line in StorageBlockInfo.cpp
 **/
extern const char *kTupleStorageSubBlockTypeNames[];

/**
 * @brief Codes for the different implementations of IndexSubBlock.
 **/
enum IndexSubBlockType {
  kCSBTree = 0,
  kNumIndexSubBlockTypes  // Not an actual IndexSubBlockType, exists for counting purposes.
};

/**
 * @brief Names corresponsing to IndexSubBlockType.
 * @note Defined out-of-line in StorageBlockInfo.cpp
 **/
extern const char *kIndexSubBlockTypeNames[];

enum BloomFilterSubBlockType {
	kDefault = 0
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_STORAGE_BLOCK_INFO_HPP_
