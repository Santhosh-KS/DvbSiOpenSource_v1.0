//
// DVB_SI for Reference Design Kit (RDK)
//
// Copyright (C) 2015  
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

#ifndef TNETWORKNAMEDESCRIPTOR_H
#define TNETWORKNAMEDESCRIPTOR_H

#include <string>

#include "TMpegDescriptor.h"
#include "DvbUtils.h"

class TNetworkNameDescriptor: public TMpegDescriptor
{
public:
  TNetworkNameDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty
  }
  
  virtual ~TNetworkNameDescriptor()
  {
    // Empty
  }
 
  std::string GetName()
  {
    return DecodeText(DescriptorDataVector.data(), DescriptorDataVector.size());
  }

  std::string ToString()
  {
    return DecodeText(DescriptorDataVector.data(), DescriptorDataVector.size());
  }
};

#endif /* TNETWORKNAMEDESCRIPTOR_H */
