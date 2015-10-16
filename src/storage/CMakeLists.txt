# This file copyright (c) 2011-2013, the Quickstep authors.
# See file CREDITS.txt for details.
#
# This file is part of Quickstep.
#
# Quickstep is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Quickstep is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Quickstep.  If not, see <http://www.gnu.org/licenses/>.

option(CLEAR_BLOCK_MEMORY "Overwrite StorageBlock memory with zeroes before loading or creating a block in it." OFF)
option(REBUILD_INDEX_ON_UPDATE_OVERFLOW "If an IndexSubBlock runs out of space while re-adding entries during an update() query, try rebuilding it." ON)

if (CLEAR_BLOCK_MEMORY)
  set(QUICKSTEP_CLEAR_BLOCK_MEMORY TRUE)
endif()

if (REBUILD_INDEX_ON_UPDATE_OVERFLOW)
  set(QUICKSTEP_REBUILD_INDEX_ON_UPDATE_OVERFLOW TRUE)
endif()

configure_file (
  "${CMAKE_CURRENT_SOURCE_DIR}/StorageConfig.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/StorageConfig.h"
)

QS_PROTOBUF_GENERATE_CPP(storage_proto_srcs storage_proto_hdrs StorageBlockLayout.proto)
add_custom_target(storage_proto DEPENDS ${storage_proto_hdrs})

add_library(storage
            BasicColumnStoreTupleStorageSubBlock.cpp ColumnStoreUtil.cpp
            CompressedBlockBuilder.cpp
            CompressedColumnStoreTupleStorageSubBlock.cpp
            CompressedPackedRowStoreTupleStorageSubBlock.cpp
            CompressedTupleStorageSubBlock.cpp CSBTreeIndexSubBlock.cpp
            InsertDestination.cpp PackedRowStoreTupleStorageSubBlock.cpp
            StorageBlock.cpp StorageBlockInfo.cpp StorageBlockLayout.cpp
            StorageErrors.cpp StorageManager.cpp TupleStorageSubBlock.cpp
            ${storage_proto_srcs})
