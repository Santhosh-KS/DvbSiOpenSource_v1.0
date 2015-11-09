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

#ifndef TBATTABLE_H
#define TBATTABLE_H

// C++ system includes
#include <vector>

// Other libraries' includes

// Project's includes
#include "TSiTable.h"
#include "TMpegDescriptor.h"
#include "TTransportStream.h"

/**
 * Bouquet Association Table (BAT) class
 */
class TBatTable: public TSiTable
{
private:
  std::vector<TMpegDescriptor> BouquetDescriptorsVector;
  std::vector<TTransportStream> TransportStreamVector;

public:
  TBatTable(uint8_t tableId, uint16_t tableExtId, uint8_t versionNum, bool isCurrentNext)
      : TSiTable(tableId, tableExtId, versionNum, isCurrentNext)
  {
    // Empty
  }
   
  virtual ~TBatTable() 
  {
    // Empty
  }

  inline uint16_t GetBouquetId() const
  {
    return GetTableExtensionId();
  }

  inline void AddBouquetDescriptor(const TMpegDescriptor& descriptor)
  {
    BouquetDescriptorsVector.push_back(descriptor);
  }

  inline void AddBouquetDescriptors(const std::vector<TMpegDescriptor>& descriptors)
  {
    BouquetDescriptorsVector.insert(BouquetDescriptorsVector.end(), descriptors.begin(), descriptors.end());
  }

  inline const std::vector<TMpegDescriptor>& GetBouquetDescriptors() const
  {
    return BouquetDescriptorsVector;
  }

  inline void FlushBouquetDescriptors()
  {
    BouquetDescriptorsVector.clear();
  }

  inline void AddTransportStream(const TTransportStream& ts)
  {
    TransportStreamVector.push_back(ts);
  }

  inline const std::vector<TTransportStream>& GetTransportStreams() const
  {
    return TransportStreamVector;
  }

  inline void FlushTransportStreams()
  {
    TransportStreamVector.clear();
  }
};
#endif // TBATTABLE_H
