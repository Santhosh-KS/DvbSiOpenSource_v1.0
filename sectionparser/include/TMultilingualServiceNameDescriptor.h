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

#ifndef TMULTILINGUALSERVICENAMEDESCRIPTOR_H
#define TMULTILINGUALSERVICENAMEDESCRIPTOR_H

#include <vector>

#include "TMpegDescriptor.h"
#include "DvbUtils.h"

class TMultilingualServiceNameDescriptor: public TMpegDescriptor
{
private:
  std::vector<const uint8_t*> ItemVector;

public:
  TMultilingualServiceNameDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    const uint8_t* p = DescriptorDataVector.data();
    if (p) {
      const uint8_t* end = p + DescriptorDataVector.size();
      while (p + 4 < end) {
        ItemVector.push_back(p);
        // language code
        p += 3;
        // provider name length and provider name
        p += p[0] + 1;
        // service name length and service name
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

  uint8_t GetServiceProviderNameLength(uint8_t n) const
  {
    return *(ItemVector[n] + 3);
  }

  std::string GetServiceProviderName(uint8_t n) const
  {
    return DecodeText(ItemVector[n] + 4, GetServiceProviderNameLength(n));
  }

  uint8_t GetServiceNameLength(uint8_t n) const
  {
    return *(ItemVector[n] + 4 + GetServiceProviderNameLength(n));
  }

  std::string GetServiceName(uint8_t n) const
  {
    return DecodeText(ItemVector[n] + 5 + GetServiceProviderNameLength(n), GetServiceNameLength(n));
  }
};

#endif /* TMULTILINGUALSERVICENAMEDESCRIPTOR_H */
