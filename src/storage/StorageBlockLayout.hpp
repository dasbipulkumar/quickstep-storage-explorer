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

#ifndef QUICKSTEP_STORAGE_STORAGE_BLOCK_LAYOUT_HPP_
#define QUICKSTEP_STORAGE_STORAGE_BLOCK_LAYOUT_HPP_

#include <cstddef>
#include <exception>

#include "storage/StorageBlockInfo.hpp"
#include "storage/StorageBlockLayout.pb.h"
#include "utility/Macros.hpp"

namespace quickstep {

class CatalogRelation;

/** \addtogroup Storage
 *  @{
 */


/**
 * @brief A physical layout for StorageBlocks. Describes the size of blocks,
 *        the type of TupleStorageSubBlock to use, any IndexSubBlocks, and the
 *        relative sizes of all sub-blocks.
 **/
class StorageBlockLayout {
 public:
  /**
   * @brief Constructor.
   *
   * @param relation The relation that blocks described by this layout belong
   *        to.
   **/
  explicit StorageBlockLayout(const CatalogRelation &relation)
      : relation_(relation) {
  }

  /**
   * @param Destructor.
   **/
  ~StorageBlockLayout() {
  }

  /**
   * @brief Static method to generate a default layout for a particular
   *        relation.
   * @note The current policy is that a default layout takes up one slot, uses
   *       PackedRowStoreTupleStorageSubBlock, and has no indices.
   *
   * @param relation The relation to generate a default layout for.
   * @return A new StorageBlockLayout for the relation, according to the
   *         default policies.
   **/
  static StorageBlockLayout* GenerateDefaultLayout(const CatalogRelation &relation);

  /**
   * @brief Static method to check whether a StorageBlockLayoutDescription is
   *        fully-formed and all parts are valid.
   *
   * @param relation The relation a layout belongs to.
   * @param description A description of a StorageBlockLayout.
   * @return Whether description is fully-formed and valid.
   **/
  static bool DescriptionIsValid(const CatalogRelation &relation,
                                 const StorageBlockLayoutDescription &description);

  /**
   * @brief Get the relation this layout applies to.
   *
   * @return The relation this layout applies to.
   **/
  const CatalogRelation& getRelation() const {
    return relation_;
  }

  /**
   * @brief Get this layout's internal StorageBlockLayoutDescription.
   *
   * @return A reference to this layout's internal description.
   **/
  const StorageBlockLayoutDescription& getDescription() const {
    return layout_description_;
  }

  /**
   * @brief Get a mutable pointer to this layout's internal
   *        StorageBlockLayoutDescription.
   * @note This method should be used to access the internal description of
   *       this layout to modify it and build up the layout.
   *
   * @return A mutable pointer to this layout's internal description.
   **/
  StorageBlockLayoutDescription* getDescriptionMutable() {
    return &layout_description_;
  }

  /**
   * @brief Finalize the layout and build the StorageBlockHeader.
   * @note This should be called after constructing the StorageBlockLayout and
   *       building it up by accessing getDescriptionMutable(), but before
   *       using getBlockHeaderSize() or copyHeaderTo().
   **/
  void finalize();

  /**
   * @brief Determine the size, in bytes, of the StorageBlockHeader in blocks
   *        with this layout, plus the 4 bytes at the front which store the
   *        header length.
   * @warning finalize() must be called before using this method.
   *
   * @return The size (in bytes) of the StorageBlockHeader for this layout.
   **/
  std::size_t getBlockHeaderSize() const {
    DEBUG_ASSERT(block_header_.IsInitialized());
    return sizeof(int) + block_header_.ByteSize();
  }

  /**
   * @brief Copy a StorageBlockHeader describing this layout to the target
   *        memory location.
   * @note The data copied to dest is an int (the length of the rest of the
   *       header) followed by the binary-serialized form of block_header_.
   *
   * @param dest A memory location to copy the header to (i.e. the start of a
   *        StorageBlock's memory). MUST be at least as large as the size
   *        reported by getBlockHeaderSize().
   **/
  void copyHeaderTo(void *dest) const;

 private:
  const CatalogRelation &relation_;
  StorageBlockLayoutDescription layout_description_;
  StorageBlockHeader block_header_;

  DISALLOW_COPY_AND_ASSIGN(StorageBlockLayout);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_STORAGE_STORAGE_BLOCK_LAYOUT_HPP_
