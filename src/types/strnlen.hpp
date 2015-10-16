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

#ifndef QUICKSTEP_TYPES_STRNLEN_HPP_
#define QUICKSTEP_TYPES_STRNLEN_HPP_

#include <cstddef>
#include <cstring>

#include "types/TypesConfig.h"

namespace quickstep {

/** \addtogroup Types
 *  @{
 */

#ifdef QUICKSTEP_HAVE_STRNLEN
inline std::size_t strnlen(const char *c, const std::size_t maxlen) {
  return ::strnlen(c, maxlen);
}
#else
inline std::size_t strnlen(const char *c, const std::size_t maxlen) {
  const char *loc = static_cast<const char*>(std::memchr(c, '\0', maxlen));
  if (loc == NULL) {
    return maxlen;
  } else {
    return (loc - c);
  }
}
#endif

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_TYPES_STRNLEN_HPP_
