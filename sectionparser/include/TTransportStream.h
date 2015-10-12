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

#ifndef TTRANSPORTSTREAM_H
#define TTRANSPORTSTREAM_H

#include <stdint.h>
#include <vector>

class TTransportStream
{
private:
  uint16_t TransportStreamId;
  uint16_t OriginalNetworkId;
  std::vector<TMpegDescriptor> TsDescriptorVector;

public:
  TTransportStream(uint16_t tsId, uint16_t origNetId)
    : TransportStreamId(tsId),
      OriginalNetworkId(origNetId)
  {
    // Empty
  }

  virtual ~TTransportStream()
  {
    // Empty
  }

  uint16_t GetOriginalNetworkId() const
  {
    return OriginalNetworkId;
  }

  void SetOriginalNetworkId(uint16_t originalNetworkId)
  {
    OriginalNetworkId = originalNetworkId;
  }

  uint16_t GetTsId() const
  {
    return TransportStreamId;
  }

  void SetTsId(uint16_t tsId)
  {
    TransportStreamId = tsId;
  }

  const std::vector<TMpegDescriptor>& GetTsDescriptors() const
  {
    return TsDescriptorVector;
  }

  void AddDescriptor(TMpegDescriptor& descriptor)
  {
    TsDescriptorVector.push_back(descriptor);
  }

  void AddDescriptors(const std::vector<TMpegDescriptor>& descriptors)
  {
    TsDescriptorVector.insert(TsDescriptorVector.end(), descriptors.begin(), descriptors.end());
  }

  void FlushDescriptors()
  {
    TsDescriptorVector.clear();
  }
};

#endif /* TRANSPORTSTREAM_H_ */
