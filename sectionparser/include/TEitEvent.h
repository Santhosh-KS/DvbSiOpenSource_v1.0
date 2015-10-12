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

#ifndef EITEVENT_H
#define EITEVENT_H

// C system includes
#include <stdint.h>

// C++ system includes
#include <vector>

// Other libraries' includes

// Project's includes
#include "DvbUtils.h"
#include "TSiTable.h"

/**
 * EIT event class
 */
class TEitEvent
{
private:
  uint16_t EventId;
  uint64_t StartTime;
  uint32_t Duration;
  uint8_t  RunningStatus;
  bool IsCaModeIndicator;
  bool IsNearVideoOnDeman;
  std::vector<TMpegDescriptor> EventDescriptorVector;

public:
  TEitEvent(uint16_t id, uint64_t startTime, uint32_t dur, uint8_t runningStatus, bool freeCa)
    : EventId(id),
      StartTime(startTime),
      Duration(dur),
      RunningStatus(runningStatus),
      IsCaModeIndicator(freeCa)
  {
    IsNearVideoOnDeman = ((StartTime & 0xFFFFF000) == 0xFFFFF000 && RunningStatus == 0x0);
  }

  virtual ~TEitEvent()
  {
    // Empty
  }
  
  const std::vector<TMpegDescriptor>& GetEventDescriptors() const
  {
    return EventDescriptorVector;
  }

  void AddDescriptor(const TMpegDescriptor& descriptor)
  {
    EventDescriptorVector.push_back(descriptor);
  }

  void AddDescriptors(const std::vector<TMpegDescriptor>& descriptors)
  {
    EventDescriptorVector.insert(EventDescriptorVector.end(), descriptors.begin(), descriptors.end());
  }

  void FlushDescriptors()
  {
    EventDescriptorVector.clear();
  }

  uint32_t GetDurationBcd() const
  {
    return Duration;
  }

  uint32_t GetDuration() const
  {
    return BcdByteToDec((Duration >> 16) & 0xFF)*3600 + BcdByteToDec((Duration >> 8) & 0xFF)*60 + BcdByteToDec(Duration & 0xFF);
  }

  uint16_t GetEventId() const
  {
    return EventId;
  }

  bool IsScrambled() const
  {
    return IsCaModeIndicator;
  }

  bool IsNvod() const
  {
    return IsNearVideoOnDeman;
  }

  TRunningStatus GetRunningStatus() const
  {
    return static_cast<TRunningStatus>(RunningStatus);
  }

  time_t GetStartTime() const
  {
    return MjdToDate(StartTime);
  }

  uint64_t GetStartTimeBcd() const
  {
    return StartTime;
  }

};

#endif // EITEVENT_H
