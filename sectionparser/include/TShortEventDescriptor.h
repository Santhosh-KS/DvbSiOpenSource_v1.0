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

#ifndef TSHORTEVENTDESCRIPTOR_H
#define TSHORTEVENTDESCRIPTOR_H

#include <stdint.h>
#include <string>


#include "DvbUtils.h"
#include "TMpegDescriptor.h"

class TShortEventDescriptor: public TMpegDescriptor
{
public:
  TShortEventDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty
  }

  virtual ~TShortEventDescriptor()
  {
    // Empty
  }

  std::string GetLanguageCode(void) const
  {
    return std::string((const char*)DescriptorDataVector.data(), 3);
  }

  uint8_t GetEventNameLength() const
  {
    return DescriptorDataVector[3];
  }

  std::string GetEventName() const
  {
    return DecodeText(DescriptorDataVector.data() + 4, GetEventNameLength());
  }

  uint8_t GetTextLength() const
  {
    return DescriptorDataVector[4 + GetEventNameLength()];
  }

  std::string GetText() const
  {
    return DecodeText(DescriptorDataVector.data() + 5 + GetEventNameLength(), GetTextLength());
  }

  std::string ToString() const
  {
    return std::string("Not implemented");
  }
};

#endif /* TSHORTEVENTDESCRIPTOR_H */
