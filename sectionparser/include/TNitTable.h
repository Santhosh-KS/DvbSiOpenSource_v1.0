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

#ifndef TNITTABLE_H
#define TNITTABLE_H

// C++ system includes
#include <vector>

// Other libraries' includes

// Project's includes
#include "TSiTable.h"
#include "TMpegDescriptor.h"
#include "TTransportStream.h"

/**
 * Network Information Table (NIT) class
 */
class TNitTable: public TSiTable
{
private:
  std::vector<TMpegDescriptor> NetworkDescriptorVector;
  std::vector<TTransportStream> TransportStreamVector;

public:
  TNitTable(uint8_t id, uint16_t extId, uint8_t ver, bool cur)
     : TSiTable(id, extId, ver, cur)
  {
    // Empty 
  }

  uint16_t GetNetworkId() const
  {
    return GetTableExtensionId(); 
  }

  void AddNetworkDescriptor(TMpegDescriptor& descriptor)
  {
    NetworkDescriptorVector.push_back(descriptor);
  }

  void AddNetworkDescriptors(const std::vector<TMpegDescriptor>& descriptors)
  {
    NetworkDescriptorVector.insert(NetworkDescriptorVector.end(), descriptors.begin(), descriptors.end());
  }

  const std::vector<TMpegDescriptor>& GetNetworkDescriptors() const
  {
    return NetworkDescriptorVector;
  }

  void FlushNetworkDescriptors()
  {
    NetworkDescriptorVector.clear();
  }

  void AddTransportStream(TTransportStream& ts)
  {
    TransportStreamVector.push_back(ts);
  }

  const std::vector<TTransportStream>& GetTransportStreams() const
  {
    return TransportStreamVector;
  }

  void FlushTransportStreams()
  {
    TransportStreamVector.clear();
  }
};

#endif // TNITTABLE_H
