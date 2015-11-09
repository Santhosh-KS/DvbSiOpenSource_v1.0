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

#ifndef TEXTENDEDEVENTDESCRIPTOR_H
#define TEXTENDEDEVENTDESCRIPTOR_H

#include <string>


#include "TMpegDescriptor.h"
#include "DvbUtils.h"

// Syntax of the descriptor from ETSI EN 300 468
//
//extended_event_descriptor() {
//    descriptor_tag                    8 uimsbf
//    descriptor_length                 8 uimsbf
//    descriptor_number                 4 uimsbf
//    last_descriptor_number            4 uimsbf
//    ISO_639_language_code            24 bslbf
//    length_of_items                   8 uimsbf
//    for ( i=0;i<N;i++){
//        item_description_length       8 uimsbf
//        for (j=0;j<N;j++){
//            item_description_char     8 uimsbf
//        }
//        item_length                   8 uimsbf
//        for (j=0;j<N;j++){
//            item_char                 8 uimsbf
//        }
//    }
//    text_length                       8 uimsbf
//    for (i=0;i<N;i++){
//        text_char                     8 uimsbf
//    }
//}

/**
 * Extended event descriptor class
 */
class TExtendedEventDescriptor: public TMpegDescriptor
{
private:
    /**
     * List of items
     */
    std::vector<const uint8_t*> ItemVector;

public:
  TExtendedEventDescriptor(const TMpegDescriptor& desc)
    : TMpegDescriptor(desc)
  {
    const uint8_t* p = DescriptorDataVector.data();
    if (p) {
      p += 5;
      const uint8_t* end = p + GetLengthOfItems();
      while(p < end) {
        ItemVector.push_back(p);

        // item description and its length
        p += p[0] + 1;

        // item and its length
        p += p[0] + 1;
      }
    }
  }

  inline uint8_t GetNumber() const
  {
    return DescriptorDataVector[0] >> 4;
  }

  inline uint8_t GetLastNumber() const
  {
    return DescriptorDataVector[0] & 0xf;
  }

  inline std::string GetLanguageCode() const
  {
    return std::string((const char*)DescriptorDataVector.data() + 1, 3);
  }

  inline uint8_t GetLengthOfItems() const
  {
    return DescriptorDataVector[4];
  }

  inline uint8_t GetNumberOfItems() const
  {
    return ItemVector.size();
  }

  inline uint8_t GetItemDescriptionLength(uint8_t n) const
  {
    return *(ItemVector[n]);
  }

  inline std::string GetItemDescription(uint8_t n) const
  {
    return DecodeText(ItemVector[n] + 1, *(ItemVector[n]));
  }

  inline uint8_t GetItemLength(uint8_t n) const
  {
    return *(ItemVector[n] + GetItemDescriptionLength(n) + 1);
  }

  inline std::string GetItem(uint8_t n) const
  {
    return DecodeText(ItemVector[n] + GetItemDescriptionLength(n) + 2, GetItemLength(n));
  }

  inline uint8_t GetTextLength() const
  {
    return DescriptorDataVector[5 + GetLengthOfItems()];
  }

  inline std::string GetText() const
  {
    return DecodeText(DescriptorDataVector.data() + 6 + GetLengthOfItems(), GetTextLength());
  }
};

#endif /* TEXTENDEDEVENTDESCRIPTOR_H */
