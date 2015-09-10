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

#ifndef TSDTTABLE_H
#define TSDTTABLE_H

// C system includes
#include <stdint.h>

// Other libraries' includes

// Project's includes
#include "TSdtService.h"
#include "TSiTable.h"

/**
 * Service Description Table (SDT) class
 */
class TSdtTable: public TSiTable
{
private:
  std::vector<TSdtService> SdtServiceVector;
  uint16_t OriginalNetworkId;

public:
  TSdtTable(uint8_t id, uint16_t extId, uint8_t ver, bool cur)
    : TSiTable(id, extId, ver, cur), OriginalNetworkId(0)
  {
    // Empty
  }

  inline void AddService(const TSdtService& service)
  {
    SdtServiceVector.push_back(service);
  }

  inline const std::vector<TSdtService>& GetServices() const
  {
    return SdtServiceVector;
  }

  //void removeServices()
  inline void FlushServices()
  {
    SdtServiceVector.clear();
  }

  inline uint16_t GetOriginalNetworkId() const
  {
    return OriginalNetworkId;
  }

  inline void SetOriginalNetworkId(uint16_t originalNetworkId)
  {
    OriginalNetworkId = originalNetworkId;
  }
};

#endif // TSDTTABLE_H
