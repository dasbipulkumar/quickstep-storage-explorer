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
#include "storage/StorageManager.hpp"

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageBlockLayout.hpp"
#include "storage/StorageConfig.h"
#include "storage/StorageConstants.hpp"

using std::free;
using std::malloc;
using std::memset;
using std::size_t;
using std::vector;

namespace quickstep {

StorageManager::~StorageManager() {
  for (CompatUnorderedMap<block_id, BlockHandle>::unordered_map::iterator it = blocks_.begin();
       it != blocks_.end();
       ++it) {
    delete (it->second).block;
  }

  for (vector<void*>::iterator it = alloc_chunks_.begin(); it != alloc_chunks_.end(); ++it) {
    free(*it);
  }
}

block_id StorageManager::createBlock(const CatalogRelation &relation,
                                     const StorageBlockLayout *layout) {
  if (layout == NULL) {
    layout = &(relation.getDefaultStorageBlockLayout());
  }

  size_t num_slots = layout->getDescription().num_slots();
  DEBUG_ASSERT(num_slots > 0);
  size_t slot_index = getSlots(num_slots);
  void *new_block_mem = getSlotAddress(slot_index);
  ++block_index_;

  BlockHandle new_block_handle;
  new_block_handle.slot_index_low = slot_index;
  new_block_handle.slot_index_high = slot_index + num_slots;
  new_block_handle.block = new StorageBlock(relation,
                                            block_index_,
                                            *layout,
                                            true,
                                            new_block_mem,
                                            kSlotSizeBytes * num_slots);

  blocks_[block_index_] = new_block_handle;
  return block_index_;
}

bool StorageManager::blockIsLoaded(const block_id block) const {
  if (blocks_.find(block) == blocks_.end()) {
    return false;
  } else {
    return true;
  }
}

void StorageManager::evictBlock(const block_id block) {
  CompatUnorderedMap<block_id, BlockHandle>::unordered_map::iterator block_it = blocks_.find(block);

  if (block_it == blocks_.end()) {
    FATAL_ERROR("Block " << block << " does not exist in memory.");
  }

  delete block_it->second.block;
  for (size_t i = block_it->second.slot_index_low; i < block_it->second.slot_index_high; ++i) {
    free_bitmap_[i] = true;
  }

  blocks_.erase(block_it);
}

StorageBlock* StorageManager::getBlockMutable(const block_id block) const {
  CompatUnorderedMap<block_id, BlockHandle>::unordered_map::const_iterator it = blocks_.find(block);

  if (it == blocks_.end()) {
    FATAL_ERROR("Block " << block << " does not exist in memory.");
  }

  return it->second.block;
}

void* StorageManager::getSlotAddress(const std::size_t slot_index) const {
  return static_cast<char*>(alloc_chunks_[slot_index / kAllocationChunkSizeSlots])
         + kSlotSizeBytes * (slot_index % kAllocationChunkSizeSlots);
}

std::size_t StorageManager::getSlots(const std::size_t num_slots) {
  if (num_slots > kAllocationChunkSizeSlots) {
    FATAL_ERROR("Attempted to allocate more than kAllocationChunkSizeSlots "
                "contiguous slots in StorageManager::getSlots()");
  }

  size_t min_slot;
  bool found = false;
  for (size_t alloc_chunk_num = 0;
       alloc_chunk_num < alloc_chunks_.size();
       ++alloc_chunk_num) {
    min_slot = alloc_chunk_num * kAllocationChunkSizeSlots;
    while (min_slot <= (alloc_chunk_num + 1) * kAllocationChunkSizeSlots - num_slots) {
      found = true;
      for (size_t i = min_slot; i < min_slot + num_slots; ++i) {
        if (!free_bitmap_[i]) {
          min_slot = i + 1;
          found = false;
          break;
        }
      }
      if (found) {
        break;
      }
    }
    if (found) {
      break;
    }
  }

  if (!found) {
    allocChunk();
    min_slot = (alloc_chunks_.size() - 1) * kAllocationChunkSizeSlots;
  }

  for (size_t i = min_slot; i < min_slot + num_slots; ++i) {
    free_bitmap_[i] = false;
  }

#ifdef QUICKSTEP_CLEAR_BLOCK_MEMORY
  memset(getSlotAddress(min_slot), 0, num_slots * kSlotSizeBytes);
#endif
  return min_slot;
}

void StorageManager::allocChunk() {
  void *basemem = malloc(kAllocationChunkSizeSlots * kSlotSizeBytes);
  alloc_chunks_.push_back(basemem);
  free_bitmap_.resize(alloc_chunks_.size() * kAllocationChunkSizeSlots, true);
}

}  // namespace quickstep
