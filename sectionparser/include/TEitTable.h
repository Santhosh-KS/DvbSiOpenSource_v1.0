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

#ifndef TEITTABLE_H
#define TEITTABLE_H

// C++ system includes
#include <vector>

// Other libraries' includes

// Project's includes
#include "TSiTable.h"
#include "TEitEvent.h"

/**
 * Event Information Table (EIT) class
 */
class TEitTable: public TSiTable
{
private:
    uint16_t TransportId;
    uint16_t NetworkId;
    uint8_t LastTableId;

    std::vector<TEitEvent> EitEventVector;

public:
  TEitTable(uint8_t id, uint16_t extId, uint8_t ver, bool cur)
    : TSiTable(id, extId, ver, cur),
      TransportId(0),
      NetworkId(0),
      LastTableId(0)
  {
    // Empty
  }

  inline void AddEvent(TEitEvent& event)
  {
    EitEventVector.push_back(event);
  }

  inline const std::vector<TEitEvent>& GetEvents() const
  {
    return EitEventVector;
  }

  inline void FlushEvents()
  {
    EitEventVector.clear();
  }

  inline uint8_t GetLastTableId() const
  {
    return LastTableId;
  }

  inline void SetLastTableId(uint8_t lastTableId)
  {
    LastTableId = lastTableId;
  }

  inline uint16_t GetNetworkId() const
  {
    return NetworkId;
  }

  inline void SetNetworkId(uint16_t networkId)
  {
    NetworkId = networkId;
  }

  inline uint16_t GetTsId() const
  {
    return TransportId;
  }

  inline void SetTsId(uint16_t tsId)
  {
    TransportId = tsId;
  }
};

#endif /* TEITTABLE_H */
