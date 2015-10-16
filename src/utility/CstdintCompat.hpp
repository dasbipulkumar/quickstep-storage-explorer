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

#ifndef QUICKSTEP_UTILITY_CSTDINT_COMPAT_HPP_
#define QUICKSTEP_UTILITY_CSTDINT_COMPAT_HPP_

#include "utility/UtilityConfig.h"

/**
 * Compatibility for systems which have the C99 header stdint.h, but not the
 * C++ version cstdint.
 **/

#ifdef QUICKSTEP_HAVE_CSTDINT
#include <cstdint>  // NOLINT - need to include UtilityConfig.h first
#else
#define __STDC_LIMIT_MACROS
#include <stdint.h>  // NOLINT - need to include UtilityConfig.h first
namespace std {
  using ::int8_t;
  using ::int16_t;
  using ::int32_t;
  using ::int64_t;

  using ::uint8_t;
  using ::uint16_t;
  using ::uint32_t;
  using ::uint64_t;
}  // namespace std
#endif

#endif  // QUICKSTEP_UTILITY_CSTDINT_COMPAT_HPP_
