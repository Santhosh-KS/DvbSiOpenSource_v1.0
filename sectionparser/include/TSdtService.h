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

#ifndef TSDT_SERVICE_H
#define TSDT_SERVICE_H

// C system includes
#include <stdint.h>

// C++ system includes
#include <vector>

// Other libraries' includes

// Project's includes
#include "TSiTable.h"
#include "TMpegDescriptor.h"

/**
 * SDT service class
 */
class TSdtService
{
private:
  std::vector<TMpegDescriptor> ServiceDescriptorVector;
  uint16_t ServiceId;
  bool EitScheduleFlag;
  bool EitpfFlag;
  uint8_t RunningStatus;
  bool FreeCaModeFlag;

public:
  TSdtService(uint16_t id, bool eitSched, bool eitPresent, uint8_t runningStatus, bool freeCA)
    : ServiceId(id),
      EitScheduleFlag(eitSched),
      EitpfFlag(eitPresent),
      RunningStatus(runningStatus),
      FreeCaModeFlag(freeCA)
  {
    // Empty
  }

  virtual ~TSdtService()
  {
    // Empty
  }

  inline const std::vector<TMpegDescriptor>& GetServiceDescriptors() const
  {
    return ServiceDescriptorVector;
  }

  inline void AddDescriptor(const TMpegDescriptor& descriptor)
  {
    ServiceDescriptorVector.push_back(descriptor);
  }

  inline void AddDescriptors(const std::vector<TMpegDescriptor>& descriptors)
  {
    ServiceDescriptorVector.insert(ServiceDescriptorVector.end(), descriptors.begin(), descriptors.end());
  }
  
  // void removeDescriptors()
  inline void FlushDescriptors()
  {
    ServiceDescriptorVector.clear();
  }

  inline bool IsEitPfFlagSet() const
  {
    return EitpfFlag;
  }

  inline bool IsEitSchedFlagSet() const
  {
    return EitScheduleFlag;
  }

  inline bool IsScrambled() const
  {
    return FreeCaModeFlag;
  }

  inline TRunningStatus GetRunningStatus() const
  {
    return static_cast<TRunningStatus>(RunningStatus);
  }

  inline uint16_t GetServiceId() const
  {
    return ServiceId;
  }

};

#endif /* TSDT_SERVICE_H */
