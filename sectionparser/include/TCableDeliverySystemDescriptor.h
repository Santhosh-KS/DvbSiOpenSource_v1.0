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

#ifndef TCABLEDELIVERYSYSTEMDESCRIPTOR_H
#define TCABLEDELIVERYSYSTEMDESCRIPTOR_H

// C++ system includes
#include <string>

// Other libraries' includes

// Project's includes
#include "TMpegDescriptor.h"
#include "DvbUtils.h"

/**
 * Cable delivery system descriptor class
 */
class TCableDeliverySystemDescriptor: public TMpegDescriptor
{
public:
  enum TModulation {
    NOT_DEFINED = 0,//!< NOT_DEFINED
    QAM16,          //!< QAM16
    QAM32,          //!< QAM32
    QAM64,          //!< QAM64
    QAM128,         //!< QAM128
    QAM256,         //!< QAM256
  };

  TCableDeliverySystemDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    // Empty
  }

  virtual ~TCableDeliverySystemDescriptor()
  {
    // Empty
  }    
  
  inline uint32_t GetFrequencyBcd()
  {
    return ((DescriptorDataVector[0] << 24) | (DescriptorDataVector[1] << 16) | (DescriptorDataVector[2] << 8) | (DescriptorDataVector[3]));
  }

  inline uint32_t GetFrequency()
  {
    return BcdToDec((DescriptorDataVector[0] << 8) |
           DescriptorDataVector[1]) * 1000000 + BcdToDec((DescriptorDataVector[2] << 8) | 
           DescriptorDataVector[3]) * 100;
  }

  inline TCableDeliverySystemDescriptor::TModulation GetModulation()
  {
    return static_cast<TCableDeliverySystemDescriptor::TModulation>(DescriptorDataVector[6]);
  }

  inline uint32_t GetSymbolRateBcd()
  {
    return ((DescriptorDataVector[7] << 20) | (DescriptorDataVector[8] << 12) | (DescriptorDataVector[9] << 4) | (DescriptorDataVector[10] >> 4));
  }

  inline uint32_t GetSymbolRate()
  {
    return (BcdByteToDec(DescriptorDataVector[7])*100000 + BcdByteToDec(DescriptorDataVector[8])*1000 +
            BcdByteToDec(DescriptorDataVector[9])*10 + BcdByteToDec(DescriptorDataVector[10]>>4))*100;
  }

  inline uint8_t  GetFecInner()
  {
     return (DescriptorDataVector[10] & 0xf);
  }

  inline uint8_t  GetFecOuter()
  {
    return (DescriptorDataVector[5] & 0xf);
  }

  inline std::string ToString()
  {
    return std::string("Not implemented");
  }
};

#endif /* TCABLEDELIVERYSYSTEMDESCRIPTOR_H */
