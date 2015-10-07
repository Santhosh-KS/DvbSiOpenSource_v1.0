// DVB_SI for Reference Design Kit (RDK)
//
// Copyright (C) 2015 Santhosh KS(Santhosh.KS@arris.com) , Manu Shrot (Manu.Shrot@arris.com),
// Stan Partridge (Stan.Partridge@arris.com) & Dmitry Barablin
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

#ifndef TMULTILINGUALNETWORKNAMEDESCRIPTOR_H
#define TMULTILINGUALNETWORKNAMEDESCRIPTOR_H

#include <vector>

#include "TMpegDescriptor.h"
#include "DvbUtils.h"

class TMultilingualNetworkNameDescriptor: public TMpegDescriptor
{
private:
    /**
     * List of items
     */
    std::vector<const uint8_t*> ItemVector;

public:
  TMultilingualNetworkNameDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    const uint8_t* p = DescriptorDataVector.data();
    if (p) {
      const uint8_t* end = p + DescriptorDataVector.size();
      while (p + 3 < end) {
        ItemVector.push_back(p);
        // language code
        p += 3;
        // network name length and network name
        p += p[0] + 1;
      }
    }
  }
  
  uint8_t GetCount() const
  {
    return ItemVector.size();
  }

  std::string GetLanguageCode(uint8_t n) const
  {
    return std::string((const char*)ItemVector[n], 3);
  }

  uint8_t GetNetworkNameLength(uint8_t n) const
  {
    return *(ItemVector[n] + 3);
  }

  std::string GetNetworkName(uint8_t n) const
  {
    return DecodeText(ItemVector[n] + 4, GetNetworkNameLength(n));
  }
};

#endif /* TMULTILINGUALNETWORKNAMEDESCRIPTOR_H */
