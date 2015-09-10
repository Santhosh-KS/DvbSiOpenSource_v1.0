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

#ifndef TPARENTALRATINGDESCRIPTOR_H
#define TPARENTALRATINGDESCRIPTOR_H

#include "TMpegDescriptor.h"

class TParentalRatingDescriptor: public TMpegDescriptor
{
public:
  TParentalRatingDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty
  }
  
  virtual ~TParentalRatingDescriptor()
  {
    // Empty
  }

  uint8_t GetCount() const
  {
    return DescriptorDataVector.size()/4;
  }

  std::string GetCountryCode(uint8_t n) const
  {
    return std::string((const char*)DescriptorDataVector.data() + (n*4), 3);
  }

  uint8_t GetRating(uint8_t n) const
  {
    return DescriptorDataVector[n*4 + 3];
  }
};

#endif /* TPARENTALRATINGDESCRIPTOR_H */
