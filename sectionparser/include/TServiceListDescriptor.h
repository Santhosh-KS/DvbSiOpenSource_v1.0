// DVB_SI for Reference Design Kit (RDK)
//
// Copyright (C) 2015 Santhosh K S (Santhosh.KS@arris.com), Manu Shrot (Manu.Shrot@arris.com), Dmitry Barablin & Stan Partridge (stan.partridge@arris.com)
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

#ifndef TSERVICELISTDESCRIPTOR_H
#define TSERVICELISTDESCRIPTOR_H

// Other libraries' includes

// Project's includes
#include "TMpegDescriptor.h"

// Syntax of the descriptor from ETSI EN 300 468
//
//service_list_descriptor(){
//    descriptor_tag          8 uimsbf
//    descriptor_length       8 uimsbf
//    for (i=0;i<N;i++){
//        service_id         16 uimsbf
//        service_type        8 uimsbf
//    }
//}

/**
 * Service list descriptor class
 */
class TServiceListDescriptor: public TMpegDescriptor
{
public:
  TServiceListDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty
  }

  virtual ~TServiceListDescriptor()
  {
    // Empty
  }

  uint8_t GetCount() const
  {
    return DescriptorDataVector.size()/3;
  }

  uint16_t GetServiceId(uint8_t n) const
  {
    return (DescriptorDataVector[n*3] << 8) | DescriptorDataVector[n*3 + 1];
  }

  uint16_t GetServiceType(uint8_t n) const
  {
    return DescriptorDataVector[n*3 + 2];
  }
};

#endif /* TSERVICELISTDESCRIPTOR_H */
