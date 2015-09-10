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

#ifndef TTOTTABLE_H
#define TTOTTABLE_H

// C++ system includes
#include <vector>

// Other libraries' includes

// Project's includes
#include "TSiTable.h"
#include "TMpegDescriptor.h"

#include "DvbUtils.h"

/**
 * Time and Date Table (TDT)/Time Offset Table (TOT) class
 */
class TTotTable: public TSiTable
{
private:
  uint64_t UtcTime;
  std::vector<TMpegDescriptor> DescriptorVector;

public:
  TTotTable(uint8_t id, uint16_t extId, uint8_t ver, bool cur)
    : TSiTable(id, extId, ver, cur),
      UtcTime(0)
  {
    // Empty
  }
 
  virtual ~TTotTable() 
  {
    // Empty
  }
  
  inline void AddDescriptor(const TMpegDescriptor& descriptor)
  {
    DescriptorVector.push_back(descriptor);
  }

  inline void AddDescriptors(const std::vector<TMpegDescriptor>& descriptors)
  {
    DescriptorVector.insert(DescriptorVector.end(), descriptors.begin(), descriptors.end());
  }

  inline std::vector<TMpegDescriptor>& GetDescriptors()
  {
    return DescriptorVector;
  }

  //void removeDescriptors()
  inline void FlushDescriptors()
  {
    DescriptorVector.clear();
  }

  inline uint64_t GetUtcTimeBcd() const
  {
    return UtcTime;
  }

  inline time_t GetUtcTime() const
  {
    return MjdToDate(UtcTime);
  }

  inline void SetUtcTime(uint64_t utcTime)
  {
    UtcTime = utcTime;
  }

};

#endif /* TTOTTABLE_H */
