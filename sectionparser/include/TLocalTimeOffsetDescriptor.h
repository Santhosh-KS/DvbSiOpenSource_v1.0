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

#ifndef TLOCALTIMEOFFSETDESCRIPTOR_H
#define TLOCALTIMEOFFSETDESCRIPTOR_H

// C system includes
#include <stdint.h>

// C++ system includes
#include <string>

// Other libraries' includes

// Project's includes
#include "TMpegDescriptor.h"

// Syntax of the descriptor from ETSI EN 300 468
//
//local_time_offset_descriptor(){
//    descriptor_tag                     8 uimsbf
//    descriptor_length                  8 uimsbf
//    for(i=0;i<N;i++){
//        country_code                  24 bslbf
//        country_region_id              6 bslbf
//        reserved                       1 bslbf
//        local_time_offset_polarity     1 bslbf
//        local_time_offset             16 bslbf
//        time_of_change                40 bslbf
//        next_time_offset              16 bslbf
//    }
//}

/**
 * Local time offset descriptor class
 */
class TLocalTimeOffsetDescriptor: public TMpegDescriptor
{
public:
  TLocalTimeOffsetDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty  
  }
  virtual ~TLocalTimeOffsetDescriptor()
  {
    // Empty  
  }
  
  inline uint8_t GetCount() const
  {
    return DescriptorDataVector.size()/13;
  }

  inline std::string GetCountryCode(uint8_t n) const
  {
    return std::string((const char*)DescriptorDataVector.data() + (n*13), 3);
  }

  inline uint8_t GetCountryRegionId(uint8_t n) const
  {
    return (DescriptorDataVector[n*13 + 3] >> 2);
  }

  inline bool GetPolarity(uint8_t n) const
  {
    return DescriptorDataVector[n*13 + 3] & 0x01;
  }

  inline uint16_t GetLocalTimeOffset(uint8_t n) const
  {
    return (DescriptorDataVector[n*13 + 4] << 8) | DescriptorDataVector[n*13 + 5];
  }

  inline uint64_t GetTimeOfChange(uint8_t n) const
  {
     return ((uint64_t)DescriptorDataVector[n*13 + 6] << 32) | ((uint64_t)DescriptorDataVector[n*13 + 7] << 24) |
            ((uint64_t)DescriptorDataVector[n*13 + 8] << 16) | ((uint64_t)DescriptorDataVector[n*13 + 9] << 8) |
            (uint64_t)DescriptorDataVector[n*13 + 10];
  }

  inline uint16_t GetNextTimeOffset(uint8_t n) const
  {
    return (DescriptorDataVector[n*13 + 11] << 8) | DescriptorDataVector[n*13 + 12];
  }
};

#endif /* TLOCALTIMEOFFSETDESCRIPTOR_H */
