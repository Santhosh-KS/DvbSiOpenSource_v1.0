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

#ifndef TMPEGDESCRIPTOR_H
#define TMPEGDESCRIPTOR_H

// C system includes
#include <stdint.h>

// C++ system includes
#include <vector>

/**
 * Descriptor tag enumeration
 */
enum class TDescriptorTag : uint8_t
{
  NETWORK_NAME_TAG = 0x40,             //!< NETWORK_NAME
  SERVICE_LIST_TAG = 0x41,             //!< SERVICE_LIST
  STUFFING_TAG     = 0x42,             //!< STUFFING
  SATELLITE_DELIVERY_TAG = 0x43,       //!< SATELLITE_DELIVERY
  CABLE_DELIVERY_TAG = 0x44,           //!< CABLE_DELIVERY
  VBI_DATA_TAG = 0x45,                 //!< VBI_DATA
  VBI_TELETEXT_TAG = 0x46,             //!< VBI_TELETEXT
  BOUQUET_NAME_TAG = 0x47,             //!< BOUQUET_NAME
  SERVICE_TAG = 0x48,                  //!< SERVICE
  COUNTRY_AVAILABILITY_TAG = 0x49,     //!< COUNTRY_AVAILABILITY
  LINKAGE_TAG = 0x4a,                  //!< LINKAGE
  NVOD_REFERENCE_TAG = 0x4b,           //!< NVOD_REFERENCE
  TIME_SHIFTED_SERVICE_TAG = 0x4c,     //!< TIME_SHIFTED_SERVICE
  SHORT_EVENT_TAG = 0x4d,              //!< SHORT_EVENT
  EXTENDED_EVENT_TAG = 0x4e,           //!< EXTENDED_EVENT
  TIME_SHIFTED_EVENT_TAG = 0x4f,       //!< TIME_SHIFTED_EVENT
  COMPONENT_TAG = 0x50,                //!< COMPONENT
  MOSAIC_TAG = 0x51,                   //!< MOSAIC
  STREAM_IDENTIFIER_TAG = 0x52,        //!< STREAM_IDENTIFIER
  CA_IDENTIFIER_TAG = 0x53,            //!< CA_IDENTIFIER
  CONTENT_DESCRIPTOR_TAG = 0x54,       //!< CONTENT_DESCRIPTOR
  PARENTAL_RATING_TAG = 0x55,          //!< PARENTAL_RATING
  TELETEXT_TAG = 0x56,                 //!< TELETEXT
  TELEPHONE_TAG = 0x57,                //!< TELEPHONE
  LOCAL_TIME_OFFSET_TAG = 0x58,        //!< LOCAL_TIME_OFFSET
  SUBTITLING_TAG = 0x59,               //!< SUBTITLING
  TERRESTRIAL_DELIVERY_TAG = 0x5a,     //!< TERRESTRIAL_DELIVERY
  MULTILINGUAL_NETWORK_NAME_TAG = 0x5b,//!< MULTILINGUAL_NETWORK_NAME
  MULTILINGUAL_BOUQUET_NAME_TAG = 0x5c,//!< MULTILINGUAL_BOUQUET_NAME
  MULTILINGUAL_SERVICE_NAME_TAG = 0x5d,//!< MULTILINGUAL_SERVICE_NAME
  MULTILINGUAL_COMPONENT_TAG = 0x5e,   //!< MULTILINGUAL_COMPONENT
  PRIVATE_DATA_SPECIFIER_TAG = 0x5f,   //!< PRIVATE_DATA_SPECIFIER
  SERVICE_MOVE_TAG = 0x60,             //!< SERVICE_MOVE
  SHORT_SMOOTHING_BUFFER_TAG = 0x61,   //!< SHORT_SMOOTHING_BUFFER
  FREQUENCY_LIST_TAG = 0x62,           //!< FREQUENCY_LIST
  PARTIAL_TRANSPORT_STREAM_TAG = 0x63, //!< PARTIAL_TRANSPORT_STREAM
  DATA_BROADCAST_TAG = 0x64,           //!< DATA_BROADCAST
  SCRAMBLING_TAG = 0x65,               //!< SCRAMBLING
  DATA_BROADCAST_ID_TAG = 0x66,        //!< DATA_BROADCAST_ID
  TRANSPORT_STREAM_TAG = 0x67,         //!< TRANSPORT_STREAM
  DSNG_TAG = 0x68,                     //!< DSNG
  PDC_TAG = 0x69,                      //!< PDC
  AC_3_TAG = 0x6a,                     //!< AC_3
  ANCILLARY_DATA_TAG = 0x6b,           //!< ANCILLARY_DATA
  CELL_LIST_TAG = 0x6c,                //!< CELL_LIST
  CELL_FREQUENCY_LINK_TAG = 0x6d,      //!< CELL_FREQUENCY_LINK
  ANNOUNCEMENT_SUPPORT_TAG = 0x6e,     //!< ANNOUNCEMENT_SUPPORT
  APPLICATION_SIGNALLING_TAG = 0x6f,   //!< APPLICATION_SIGNALLING
  ADAPTATION_FIELD_DATA_TAG = 0x70,    //!< ADAPTATION_FIELD_DATA
  SERVICE_IDENTIFIER_TAG = 0x71,       //!< SERVICE_IDENTIFIER
  SERVICE_AVAILABILITY_TAG = 0x72,     //!< SERVICE_AVAILABILITY
  DEFAULT_AUTHORITY_TAG = 0x73,        //!< DEFAULT_AUTHORITY
  RELATED_CONTENT_TAG = 0x74,          //!< RELATED_CONTENT
  TVA_ID_TAG = 0x75,                   //!< TVA_ID
  CONTENT_IDENTIFIER_TAG = 0x76,       //!< CONTENT_IDENTIFIER
  TIME_SLICE_FEC_IDENTIFIER_TAG = 0x77,//!< TIME_SLICE_FEC_IDENTIFIER
  ECM_REPETITION_RATE_TAG = 0x78,      //!< ECM_REPETITION_RATE
  S2_SATELLITE_DELIVERY_TAG = 0x79,    //!< S2_SATELLITE_DELIVERY
  ENHANCED_AC_3_TAG = 0x7a,            //!< ENHANCED_AC_3
  DTS_TAG = 0x7b,                      //!< DTS
  AAC_TAG = 0x7c,                      //!< AAC
  XAIT_LOCATION_TAG = 0x7d,            //!< XAIT_LOCATION
  FTA_CONTENT_MANAGEMENT_TAG = 0x7e,   //!< FTA_CONTENT_MANAGEMENT
  EXTENSION_TAG = 0x7f,                //!< EXTENSION
  USER_DEFINED_START_TAG = 0x80,       //!< USER_DEFINED_START
  LOGICAL_CHANNEL_TAG = 0x9c,          //!< LOGICAL_CHANNEL
  USER_DEFINED_END_TAG = 0xfe,         //!< USER_DEFINED_END
  FORBIDDEN_TAG = 0xFF                 //!< FORBIDDEN
};

/**
 * MPEG descriptor base class
 */
class TMpegDescriptor
{
private:
  TDescriptorTag DescriptorTag;

protected:
  std::vector<uint8_t> DescriptorDataVector;

public:
  TMpegDescriptor(TDescriptorTag tag, uint8_t *data, uint8_t length);

  virtual ~TMpegDescriptor()
  { 
    // Empty
  }

  const std::vector<uint8_t>& GetDescriptorData() const
  {
    return DescriptorDataVector;
  }

  TDescriptorTag GetDescriptorTag() const
  {
    return DescriptorTag;
  }

  void SetDescriptorTag(TDescriptorTag tag)
  {
    DescriptorTag = tag;
  }

  void SetDescriptorData(const std::vector<uint8_t>& data)
  {
    DescriptorDataVector = data;
  }
  static const TMpegDescriptor* FindMpegDescriptor(const std::vector<TMpegDescriptor>& list, TDescriptorTag tag);

  static std::vector<TMpegDescriptor> FindAllMpegDescriptors(const std::vector<TMpegDescriptor>& list, TDescriptorTag tag);

  static std::vector<TMpegDescriptor> ParseMpegDescriptors(uint8_t* p, uint16_t length);

};

#endif // TMPEGDESCRIPTOR_H
