// DVB_SI for Reference Design Kit (RDK)
//
// Copyright 2015 ARRIS Enterprises
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

#ifndef TMULTILINGUALCOMPONENTDESCRIPTOR_H
#define TMULTILINGUALCOMPONENTDESCRIPTOR_H

#include <vector>

#include "TMpegDescriptor.h"
#include "DvbUtils.h"

/**
 * Multilingual component descriptor class
 */
class TMultilingualComponentDescriptor: public TMpegDescriptor
{
private:
  std::vector<const uint8_t*> ItemVector;

public:
  TMultilingualComponentDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    const uint8_t* p = DescriptorDataVector.data();
    if (p) {
      const uint8_t* end = p + DescriptorDataVector.size();
      // component tag
      p += 1;

      while (p + 3 < end) {
        ItemVector.push_back(p);
        // language code
        p += 3;
        // network name length and network name
        p += p[0] + 1;
      }
    }
  }

  uint8_t GetComponentTag()  const
  {
    return DescriptorDataVector[0];
  }

  uint8_t GetCount() const
  {
    return ItemVector.size();
  }

  std::string GetLanguageCode(uint8_t n) const
  {
    return std::string((const char*)ItemVector[n], 3);
  }

  uint8_t GetTextLength(uint8_t n) const
  {
    return *(ItemVector[n] + 3);
  }

  std::string GetText(uint8_t n) const
  {
    return DecodeText(ItemVector[n] + 4, GetTextLength(n));
  }
};

#endif /* TMULTILINGUALCOMPONENTDESCRIPTOR_H */
