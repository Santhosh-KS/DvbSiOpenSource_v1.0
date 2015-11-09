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


#ifndef TCONTENTDESCRIPTOR_H
#define TCONTENTDESCRIPTOR_H

#include "TMpegDescriptor.h"

class TContentDescriptor: public TMpegDescriptor
{
public:
  TContentDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty
  }

  virtual ~TContentDescriptor()
  {
    // Empty
  }

  uint8_t GetCount()
  {
    return DescriptorDataVector.size()/2;
  }

  uint8_t GetNibbleLvl1(uint8_t n) const
  {
    return DescriptorDataVector[n*2] >> 4;
  }

  uint8_t GetNibbleLvl2(uint8_t n) const
  {
    return DescriptorDataVector[n*2] & 0xf;
  }

  uint8_t GetUserByte(uint8_t n) const
  {
    return DescriptorDataVector[n*2 + 1];
  }

};

#endif /* TCONTENTDESCRIPTOR_H */
