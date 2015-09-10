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

#include "TSectionList.h"

#include <algorithm>
#include <iostream>
#include <sstream>

#include "oswrap.h"

#include "TNitTable.h"
#include "TBatTable.h"
#include "TSdtTable.h"
#include "TEitTable.h"
#include "TTotTable.h"
#include "TUdtTable.h"

using std::list;
using std::string;


TSectionList::TSectionList()
    : IsComplete(false),
      FirstReceivedSectionNumber(0)
{
  // Empty
}

TSectionList::~TSectionList()
{
  // Empty
}

void TSectionList::InitializeSectionList(TSiSection& section)
{
    // Let's InitializeSectionList the list
    SiSectionList.clear();
    SiSectionList.push_front(section);

    // Let's set the new section number
    FirstReceivedSectionNumber = section.SectionNumber;

    // We need to handle EIT sections differently
    if(!IsEit(section.TableId))
    {
        IsComplete = IsSectionListComplete(section);
    }
    else
    {
        // Don't check after receiving the first EIT section
        IsComplete =  false;
    }
}

bool TSectionList::AddSiSection(TSiSection& section)
{
    // empty?
    if(!SiSectionList.empty())
    {
        // Let's check if syntax indicator is on or not
        if(SiSectionList.front().SectionSyntaxIndicator)
        {
            // Table ID extenstion should match
            if(SiSectionList.front().ExtensionTableId != section.ExtensionTableId)
            {
                OS_LOG(DVB_DEBUG,  "<%s> 0x%x.0x%x: ext_id mismatch\n", __FUNCTION__, section.TableId, section.ExtensionTableId);
                // Ext id doesn't match
                return false;
            }

            // Let's check for version and/or last section number mismatch
            if((SiSectionList.front().VersionNumber != section.VersionNumber) ||
               (SiSectionList.front().LastSectionNumber != section.LastSectionNumber))
            {
                OS_LOG(DVB_DEBUG,  "<%s> 0x%x.0x%x: VersionNumber or last section number mismatch\n",
                 __FUNCTION__, section.TableId, section.ExtensionTableId);
                InitializeSectionList(section);
                return true;
            }

            // If we have an already complete version of the table, we can ignore the incoming section
            if(IsComplete && (SiSectionList.front().VersionNumber == section.VersionNumber))
            {
                OS_LOG(DVB_DEBUG,  "<%s> 0x%x.0x%x: ignoring (same version, table already complete)\n",
                 __FUNCTION__, section.TableId, section.ExtensionTableId);
                // Ignore
                return false;
            }
        }
        else // syntax
        {
            OS_LOG(DVB_DEBUG,  "<%s> 0x%x: syntax is false, calling Init()\n", __FUNCTION__, section.TableId);
            InitializeSectionList(section);
            return true;
        }
    }
    else
    {
        OS_LOG(DVB_DEBUG,  "<%s> 0x%x.0x%x: first section received\n", __FUNCTION__, section.TableId, section.ExtensionTableId);
        InitializeSectionList(section);
        return true;
    }

    // Everything checks out. Time to insert the section into the list.
    InsertSectionIntoList(section);

    // Let's update the completeness flag
    IsComplete = IsSectionListComplete(section);

    return true;
}

void TSectionList::InsertSectionIntoList(TSiSection& section)
{
    auto it = SiSectionList.begin();
    auto end = SiSectionList.end();

    // Let's keep the section numbers sorted in the list
    while(it != end)
    {
        if(it->SectionNumber == section.SectionNumber)
        {
            // Do nothing
            return;
        }
        else if(it->SectionNumber > section.SectionNumber)
        {
            SiSectionList.insert(it, section);
            return;
        }

        ++it;
    }

    SiSectionList.push_back(section);
}

TNitTable* TSectionList::BuildNit()
{
    // Let's create the table object
    TNitTable *nit = new TNitTable(SiSectionList.front().TableId, SiSectionList.front().ExtensionTableId,
                                 SiSectionList.front().VersionNumber, SiSectionList.front().CurrentNextIndicator);

    // Time to parse the sections one by one
    for(auto it = SiSectionList.begin(), end = SiSectionList.end(); it != end; ++it)
    {
        // Network descriptors
        uint8_t *p = it->Payload.data();
        if(!p)
        {
            continue;
        }

        uint16_t netDescLength = (((uint16_t)(p[0] & 0x0f) << 8) | p[1]);
        p += 2;

        nit->AddNetworkDescriptors(TMpegDescriptor::ParseMpegDescriptors(p, netDescLength));

        p += netDescLength;

        // Transport streams
        uint16_t tsLoopLength = (((uint16_t)(p[0] & 0x0f) << 8) | p[1]);

        p += 2;
        uint8_t *loopEnd = p + tsLoopLength;

        // ts_id(2 bytes) + orig_net_id(2 bytes) + ts_desc_len(2 bytes)
        while((p + 6) <= loopEnd)
        {
            // TS constructor takes ts_id(2 bytes) and orig_net_id(2 bytes) parameters
            TTransportStream ts(((uint16_t)p[0] << 8) | p[1], ((uint16_t)p[2] << 8) | p[3]);

            uint16_t tsDescLength = ((uint16_t)(p[4] & 0x0f) << 8) | p[5];

            p += 6;
            ts.AddDescriptors(TMpegDescriptor::ParseMpegDescriptors(p, tsDescLength));
            nit->AddTransportStream(ts);

            p += tsDescLength;
        }
    }

    return nit;
}

TBatTable* TSectionList::BuildBat()
{
    // Let's create the table object
    TBatTable *bat = new TBatTable(SiSectionList.front().TableId, SiSectionList.front().ExtensionTableId,
                                 SiSectionList.front().VersionNumber, SiSectionList.front().CurrentNextIndicator);

    // Time to parse the sections one by one
    for(auto it = SiSectionList.begin(), end = SiSectionList.end(); it != end; ++it)
    {
        // Bouquet descriptors
        uint8_t *p = it->Payload.data();
        if(!p)
        {
            continue;
        }

        uint16_t bouquetDescLength = (((uint16_t)(p[0] & 0x0f) << 8) | p[1]);
        p += 2;

        bat->AddBouquetDescriptors(TMpegDescriptor::ParseMpegDescriptors(p, bouquetDescLength));

        p += bouquetDescLength;

        // Transport streams
        uint16_t tsLoopLength = (((uint16_t)(p[0] & 0x0f) << 8) | p[1]);

        p += 2;
        uint8_t *loopEnd = p + tsLoopLength;

        // ts_id(2 bytes) + orig_net_id(2 bytes) + ts_desc_len(2 bytes)
        while((p + 6) <= loopEnd)
        {
            // TS constructor takes ts_id(2 bytes) and orig_net_id(2 bytes) parameters
            TTransportStream ts(((uint16_t)p[0] << 8) | p[1], ((uint16_t)p[2] << 8) | p[3]);

            uint16_t tsDescLength = ((uint16_t)(p[4] & 0x0f) << 8) | p[5];

            p += 6;
            ts.AddDescriptors(TMpegDescriptor::ParseMpegDescriptors(p, tsDescLength));
            bat->AddTransportStream(ts);

            p += tsDescLength;
        }
    }

    return bat;
}

TSdtTable* TSectionList::BuildSdt()
{
    // Let's create the table object
    TSdtTable *sdt = new TSdtTable(SiSectionList.front().TableId, SiSectionList.front().ExtensionTableId,
                                 SiSectionList.front().VersionNumber, SiSectionList.front().CurrentNextIndicator);

    sdt->SetOriginalNetworkId(((uint16_t)(SiSectionList.front().Payload[0]) << 8) | SiSectionList.front().Payload[1]);

    OS_LOG(DVB_DEBUG,  "<%s> SDT: orig_net_id = 0x%x, Payload size = %lu\n",
     __FUNCTION__, sdt->GetOriginalNetworkId(), SiSectionList.front().Payload.size());

    // Time to parse the sections one by one
    for(auto it = SiSectionList.begin(), end = SiSectionList.end(); it != end; ++it)
    {
        uint8_t *p = it->Payload.data();
        if(!p)
        {
            // let's ignore "empty" sections
            continue;
        }

        uint8_t *payloadEnd = p + it->Payload.size();

        // Skip original_network_id bytes and reserved byte
        p += 3;
        while((p + 4) < payloadEnd)
        {
            // Let's extract the data out of the section
            uint16_t serviceId    = ((uint16_t)(p[0]) << 8) | p[1];
            bool eitSchedule      = ((p[2] & 0x2) >> 1);
            bool eitPresent       =  ((p[2]) & 0x1);
            uint8_t runningStatus = (uint8_t)(p[3]) >> 5;
            bool isScrambled      = ((p[3] & 0x10) >> 4);
            uint16_t descLength   = ((uint16_t)(p[3] & 0xf) << 8) | p[4];

            // Let's now make a DvbService object using the extracted data
            TSdtService service(serviceId, eitSchedule, eitPresent, runningStatus, isScrambled);
            OS_LOG(DVB_DEBUG,  "<%s> SDT: service_id = 0x%x, desc len = %d, run_st = %d, isScrambled = %d, eitSchedule = %d\n",
                    __FUNCTION__, serviceId, descLength, runningStatus, isScrambled, eitSchedule);
            p += 5;

            // Boundary check
            if((p + descLength) > payloadEnd)
            {
                break;
            }

            service.AddDescriptors(TMpegDescriptor::ParseMpegDescriptors(p, descLength));
            sdt->AddService(service);

            p += descLength;
        }
    }

    return sdt;
}

TEitTable* TSectionList::BuildEit()
{
    // Let's create the table object
    TEitTable *eit = new TEitTable(SiSectionList.front().TableId, SiSectionList.front().ExtensionTableId,
                                 SiSectionList.front().VersionNumber, SiSectionList.front().CurrentNextIndicator);

    eit->SetTsId(((uint16_t)(SiSectionList.front().Payload[0]) << 8) | SiSectionList.front().Payload[1]);
    eit->SetNetworkId(((uint16_t)(SiSectionList.front().Payload[2]) << 8) | SiSectionList.front().Payload[3]);
    eit->SetLastTableId(SiSectionList.front().Payload[5]);

    // Time to parse the sections one by one
    for(auto it = SiSectionList.begin(), end = SiSectionList.end(); it != end; ++it)
    {
        uint8_t *p = it->Payload.data();
        if(!p)
        {
            // let's ignore "empty" sections
            continue;
        }

        uint8_t *payloadEnd = p + it->Payload.size();

        // Skip ts_id, network_id etc
        p += 6;
        while((p + 12) < payloadEnd)
        {
            // Let's extract the data out of the section
            uint16_t eventId      = ((uint16_t)(p[0]) << 8) | p[1];
            uint64_t startTime    = ((uint64_t)(p[2]) << 32) | ((uint64_t)(p[3]) << 24) |
                                    ((uint64_t)(p[4]) << 16) | ((uint64_t)(p[5]) << 8) |
                                    ((uint64_t)(p[6]));
            uint32_t duration     = ((uint32_t)(p[7]) << 16) | ((uint32_t)(p[8]) << 8) | p[9];
            uint8_t runningStatus = (uint8_t)(p[10]) >> 5;
            bool isScrambled      = ((p[10] & 0x8) == 0x8) ? false : true;
            uint16_t descLength   = ((uint16_t)(p[10] & 0xf) << 8) | p[11];

            OS_LOG(DVB_DEBUG,  "<%s> EIT: event_id = 0x%x, dur = 0x%x, run_st = %d, des_len = %d\n",
                                        __FUNCTION__, eventId, duration, runningStatus, descLength);

            // Let's now make a DvbEvent object using the extracted data
            TEitEvent event(eventId, startTime, duration, runningStatus, isScrambled);

            p += 12;
            event.AddDescriptors(TMpegDescriptor::ParseMpegDescriptors(p, descLength));
            eit->AddEvent(event);

            p += descLength;
        }
    }

    return eit;
}

TTotTable* TSectionList::BuildTot()
{
    // Let's create the table object
    TTotTable *tot = new TTotTable(SiSectionList.front().TableId, SiSectionList.front().ExtensionTableId,
                                 SiSectionList.front().VersionNumber, SiSectionList.front().CurrentNextIndicator);
    uint8_t *p = SiSectionList.front().Payload.data();
    if(p)
    {
        uint8_t *payloadEnd = p + SiSectionList.front().Payload.size();
        if ((p + 5) <= payloadEnd)
        {
            /* 16-bit MJD and 24 bits coded as 6 digits in 4-bit BCD */
            tot->SetUtcTime( ((uint64_t)p[0] << 32) | ((uint64_t)p[1] << 24) | ((uint64_t)p[2] << 16) |
                             ((uint64_t)p[3] << 8) | (uint64_t)p[4] );
            p += 5;
        }

        // Parse descriptors (for TOTs only)
        if(SiSectionList.front().TableId == 0x73)
        {
            uint16_t descLength = ((uint16_t)(p[0] & 0xf) << 8) | p[1];

            p += 2;

            tot->AddDescriptors(TMpegDescriptor::ParseMpegDescriptors(p, descLength));
        }
    }

    return tot;
}

TUdtTable* TSectionList::BuildUdt()
{
    // Let's create the table object
    TUdtTable *udt = new TUdtTable(SiSectionList.front().TableId, SiSectionList.front().ExtensionTableId,
                                 SiSectionList.front().VersionNumber, SiSectionList.front().CurrentNextIndicator);

    // Let's add the sections to the table object
    for(auto it = SiSectionList.begin(), end = SiSectionList.end(); it != end; ++it)
    {
        udt->AddSection(*it);
    }

    return udt;
}

TSiTable* TSectionList::BuildTable()
{
    TSiTable *tbl = NULL;

    // Sanity check
    if(SiSectionList.empty())
    {
        return tbl;
    }

    TTableId tableId = static_cast<TTableId>(SiSectionList.front().TableId);

    // Time to Build an SI table based on the table identifier
    if((tableId == TTableId::TABLE_ID_NIT) || (tableId == TTableId::TABLE_ID_NIT_OTHER))
    {
        tbl = BuildNit();
    }
    else if((tableId == TTableId::TABLE_ID_SDT) || (tableId == TTableId::TABLE_ID_SDT_OTHER))
    {
        tbl = BuildSdt();
    }
    else if(tableId == TTableId::TABLE_ID_BAT)
    {
        tbl = BuildBat();
    }
    else if((tableId >= TTableId::TABLE_ID_EIT_PF) && (tableId <= TTableId::TABLE_ID_EIT_SCHED_OTHER_END))
    {
        tbl = BuildEit();
    }
    else if((tableId == TTableId::TABLE_ID_TDT) || (tableId == TTableId::TABLE_ID_TOT))
    {
        tbl = BuildTot();
    }
    else if((tableId >= TTableId::TABLE_ID_USER_DEFINED_START) && (tableId <= TTableId::TABLE_ID_USER_DEFINED_END))
    {
        tbl = BuildUdt();
    }
    else
    {
        OS_LOG(DVB_ERROR, "<%s> Unknown table id: 0x%x\n", __FUNCTION__, tableId);
    }

    return tbl;
}

bool TSectionList::IsEit(uint8_t id)
{
    TTableId tableId = static_cast<TTableId>(id);

    if((tableId >= TTableId::TABLE_ID_EIT_PF) && (tableId <= TTableId::TABLE_ID_EIT_SCHED_OTHER_END))
    {
        return true;
    }

    return false;
}

string TSectionList::ToString() const
{
    std::stringstream ss;

    if(!SiSectionList.empty())
    {
        ss << "TableId = " << (int) SiSectionList.front().TableId << ", TableExtId = "
           << (int) SiSectionList.front().ExtensionTableId << ", ";
        ss << "Section numbers:";
        std::for_each(SiSectionList.begin(), SiSectionList.end(), [&ss](const TSiSection& sec)
        {
            ss << " " << (int)sec.SectionNumber;
        });
    }
    else
    {
        ss << "Section list is empty";
    }
    return ss.str();
}

bool TSectionList::IsSectionListComplete(TSiSection& lastRcvdSect)
{
    // Sanity check
    if(SiSectionList.empty())
    {
        return false;
    }

    // If syntax indicator is off, then we can say that the table is complete right away
    if(!SiSectionList.front().SectionSyntaxIndicator)
    {
        return true;
    }

    bool complete = false;

    // We need to handle EIT tables differently
    if(IsEit(SiSectionList.front().TableId))
    {
        // Check only if we received the same section second time (full cycle passed)
        // to make sure we got all the sections.
        if(FirstReceivedSectionNumber == lastRcvdSect.SectionNumber)
        {
            uint8_t lastSectionNumber = SiSectionList.front().LastSectionNumber;
            auto it = SiSectionList.begin();
            auto end = SiSectionList.end();
            while(it != end)
            {
                if(it->SectionNumber == lastSectionNumber)
                {
                    complete = true;
                    break;
                }

                // Compare to the segment_last_section_number
                if(it->SectionNumber == it->Payload[4])
                {
                    while((std::next(it) != end) && std::next(it)->SectionNumber < lastSectionNumber)
                    {
                        ++it;
                    }
                }

                ++it;
            }
        }
        else
        {
            complete = false;
        }
    }
    else // non-EIT table
    {
        uint8_t prev = 0;

        // Let's iterate through the section list and see if the order checks out or not
        for(auto it = SiSectionList.begin(), end = SiSectionList.end(); it != end; ++it)
        {
            if(prev != it->SectionNumber)
            {
                complete = false;
                break;
            }

            if(it->LastSectionNumber == it->SectionNumber)
            {
                complete = true;
            }

            prev++;
        }
    }

    return complete;
}
