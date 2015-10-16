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

include(CheckCXXSymbolExists)

# strnlen is a POSIX extension, not strictly ANSI C.
CHECK_CXX_SYMBOL_EXISTS(strnlen "cstring" QUICKSTEP_HAVE_STRNLEN)

if (MSVC)
  set(QUICKSTEP_COMPILER_MSVC TRUE)
else()
  set(QUICKSTEP_COMPILER_MSVC FALSE)
endif()

configure_file (
  "${CMAKE_CURRENT_SOURCE_DIR}/TypesConfig.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/TypesConfig.h"
)

add_library(types
            BasicComparisons.cpp CharType.cpp
            Comparison.cpp CompressionDictionary.cpp
            CompressionDictionaryBuilder.cpp DoubleType.cpp FloatType.cpp
            IntType.cpp LongType.cpp Operation.cpp Tuple.cpp Type.cpp
            TypeErrors.cpp TypeInstance.cpp VarCharType.cpp)
add_dependencies(types storage_proto)
