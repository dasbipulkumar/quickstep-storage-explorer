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

include(CheckIncludeFileCXX)

# NOTE(chasseur): We only check for the presence of the include files, because
# checking for actual symbols in a C++ namespace appears to be broken.
check_include_file_cxx(unordered_set QUICKSTEP_HAVE_UNORDERED_SET)
check_include_file_cxx(unordered_map QUICKSTEP_HAVE_UNORDERED_MAP)

include(CheckCXXSourceCompiles)

CHECK_CXX_SOURCE_COMPILES("
  #include <cstdint>

  int main() {
    std::int16_t value = 0;
    return value;
  }
" QUICKSTEP_HAVE_CSTDINT)

CHECK_CXX_SOURCE_COMPILES("
  int main() {
    return __builtin_popcount(1234);
  }
  " QUICKSTEP_HAVE_BUILTIN_POPCOUNT)

CHECK_CXX_SOURCE_COMPILES("
  int main() {
    return __builtin_clz(12340);
  }
  " QUICKSTEP_HAVE_BUILTIN_CLZ)

CHECK_CXX_SOURCE_COMPILES("
  int main() {
    return __builtin_ctz(12340);
  }
  " QUICKSTEP_HAVE_BUILTIN_CTZ)

configure_file (
  "${CMAKE_CURRENT_SOURCE_DIR}/UtilityConfig.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/UtilityConfig.h"
)
