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

#ifndef SERVICEDESCRIPTOR_H_
#define SERVICEDESCRIPTOR_H_

#include <string>

#include "TMpegDescriptor.h"
#include "DvbUtils.h"

class TServiceDescriptor: public TMpegDescriptor
{
public:
  TServiceDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty
  }

  virtual ~TServiceDescriptor()
  {
    // Empty
  }

  uint8_t GetServiceType()
  {
    return DescriptorDataVector[0];
  }

  uint8_t GetServiceProviderNameLength()
  {
    return DescriptorDataVector[1];
  }

  std::string GetServiceProviderName()
  {
    return DecodeText(DescriptorDataVector.data() + 2, GetServiceProviderNameLength());
  }

  uint8_t GetServiceNameLength()
  {
    return DescriptorDataVector[2 + GetServiceProviderNameLength()];
  }

  std::string GetServiceName()
  {
    return DecodeText(DescriptorDataVector.data() + 3 + GetServiceProviderNameLength(), GetServiceNameLength());
  }

  std::string ToString()
  {
    return std::string("Not implemented");
  }
};

#endif /* SERVICEDESCRIPTOR_H_ */
