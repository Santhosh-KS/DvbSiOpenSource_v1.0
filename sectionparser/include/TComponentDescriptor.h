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

#ifndef TCOMPONENTDESCRIPTOR_H
#define TCOMPONENTDESCRIPTOR_H


#include "TMpegDescriptor.h"
#include "DvbUtils.h"

class TComponentDescriptor: public TMpegDescriptor
{
public:
  TComponentDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty 
  }

  virtual ~TComponentDescriptor()
  {
    // Empty 
  }

  inline uint8_t GetStreamContent() const
  {
    return DescriptorDataVector[0] & 0xf;
  }

  inline uint8_t GetComponentType() const
  {
    return DescriptorDataVector[1];
  }

  inline uint8_t GetComponentTag()  const
  {
    return DescriptorDataVector[2];
  }

  inline std::string GetLanguageCode(void) const
  {
    return std::string((const char*)DescriptorDataVector.data() + 3, 3);
  }

  inline std::string GetText(void) const
  {
    return DecodeText(DescriptorDataVector.data() + 6, DescriptorDataVector.size() - 6);
  }
};

#endif /* TCOMPONENTDESCRIPTOR_H */
