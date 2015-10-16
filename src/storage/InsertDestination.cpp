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

#include "storage/InsertDestination.hpp"

#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "storage/StorageBlock.hpp"
#include "storage/StorageManager.hpp"

namespace quickstep {

InsertDestination::InsertDestination(StorageManager *storage_manager,
                                     CatalogRelation *relation,
                                     const StorageBlockLayout *layout)
    : storage_manager_(storage_manager), relation_(relation) {
  if (layout == NULL) {
    layout_ = &(relation->getDefaultStorageBlockLayout());
  } else {
    layout_ = layout;
  }
}

StorageBlock* InsertDestination::createNewBlock() {
  block_id new_id = storage_manager_->createBlock(*relation_, layout_);
  relation_->addBlock(new_id);
  return storage_manager_->getBlockMutable(new_id);
}

void AlwaysCreateBlockInsertDestination::returnBlock(StorageBlock *block, const bool full) {
  MutexLock lock(mutex_);
  returned_block_ids_.push_back(block->getID());
}

void BlockPoolInsertDestination::addAllBlocksFromRelation() {
  MutexLock lock(mutex_);
  DEBUG_ASSERT(available_block_ids_.empty());
  for (CatalogRelation::const_iterator_blocks it = relation_->begin_blocks();
       it != relation_->end_blocks();
       it++) {
    available_block_ids_.push_back(*it);
  }
}

StorageBlock* BlockPoolInsertDestination::getBlockForInsertion() {
  MutexLock lock(mutex_);
  if (available_block_ids_.empty()) {
    return createNewBlock();
  } else {
    StorageBlock *retval = storage_manager_->getBlockMutable(available_block_ids_.back());
    available_block_ids_.pop_back();
    return retval;
  }
}

void BlockPoolInsertDestination::returnBlock(StorageBlock *block, const bool full) {
  MutexLock lock(mutex_);
  if (full) {
    done_block_ids_.push_back(block->getID());
  } else {
    available_block_ids_.push_back(block->getID());
  }
}

const std::vector<block_id>& BlockPoolInsertDestination::getTouchedBlocksInternal() {
  done_block_ids_.insert(done_block_ids_.end(),
                         available_block_ids_.begin(),
                         available_block_ids_.end());
  available_block_ids_.clear();

  return done_block_ids_;
}

}  // namespace quickstep
