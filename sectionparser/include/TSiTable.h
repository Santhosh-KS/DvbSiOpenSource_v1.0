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

#ifndef TSITABLE_H
#define TSITABLE_H

#include <stdint.h>

//enum class TTableId : uint8_t
enum TTableId {
  TABLE_ID_PAT = 0x00,                  //!< PAT
  TABLE_ID_CAT = 0x01,                  //!< CAT
  TABLE_ID_PMT = 0x02,                  //!< PMT
  TABLE_ID_TSDT = 0x03,                 //!< TSDT
  TABLE_ID_NIT = 0x40,                  //!< NIT
  TABLE_ID_NIT_OTHER = 0x41,            //!< NIT_OTHER
  TABLE_ID_SDT = 0x42,                  //!< SDT
  TABLE_ID_SDT_OTHER = 0x46,            //!< SDT_OTHER
  TABLE_ID_BAT = 0x4a,                  //!< BAT
  TABLE_ID_EIT_PF = 0x4e,               //!< EIT_PF
  TABLE_ID_EIT_PF_OTHER = 0x4f,         //!< EIT_PF_OTHER
  TABLE_ID_EIT_SCHED_START = 0x50,      //!< EIT_SCHED_START
  TABLE_ID_EIT_SCHED_END = 0x5f,        //!< EIT_SCHED_END
  TABLE_ID_EIT_SCHED_OTHER_START = 0x60,//!< EIT_SCHED_OTHER_START
  TABLE_ID_EIT_SCHED_OTHER_END = 0x6f,  //!< EIT_SCHED_OTHER_END
  TABLE_ID_TDT = 0x70,                  //!< TDT
  TABLE_ID_RST = 0x71,                  //!< RST
  TABLE_ID_ST = 0x72,                   //!< ST
  TABLE_ID_TOT = 0x73,                  //!< TOT
  TABLE_ID_DIT = 0x7e,                  //!< DIT
  TABLE_ID_SIT = 0x7f,                  //!< SIT
  TABLE_ID_USER_DEFINED_START = 0x80,   //!< USER_DEFINED_START
  TABLE_ID_USER_DEFINED_END = 0xfe      //!< USER_DEFINED_END
};

//enum class TRunningStatus : uint8_t
enum TRunningStatus {
  STATUS_UNDEFINED = 0,         //!< UNDEFINED
  STATUS_NOT_RUNNING,           //!< NOT_RUNNING
  STATUS_STARTS_IN_FEW_SECONDS, //!< STARTS_IN_FEW_SECONDS
  STATUS_PAUSING,               //!< PAUSING
  STATUS_RUNNING,               //!< RUNNING
  STATUS_SERVICE_OFF_AIR,       //!< SERVICE_OFF_AIR
  STATUS_RESERVED_1,            //!< RESERVED_1
  STATUS_RESERVED_2             //!< RESERVED_2
};

class TSiTable
{
private:
  uint8_t TableId;
  uint16_t TableExtensionId;
  uint8_t VersionNumber;
  bool CurrentNextIndicator;

public:
  TSiTable(uint8_t id, uint16_t extId, uint8_t ver, bool cur)
    : TableId(id),
      TableExtensionId(extId),
      VersionNumber(ver),
      CurrentNextIndicator(cur)
   {
     // Empty
   }

   virtual ~TSiTable()
   {
     // Empty
   }

   inline bool IsCurrentNextIndicator() const
   {
     return CurrentNextIndicator;
   }

   inline uint16_t GetTableExtensionId() const
   {
     return TableExtensionId;
   }

   inline TTableId GetTableId() const
   {
     return static_cast<TTableId>(TableId);
   }

   inline uint8_t GetVersionNumber() const
   {
     return VersionNumber;
   }

};


#endif // TSITABLE_H
