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

#include "TSectionParser.h"

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <stdlib.h>
#include <algorithm>

#include <utility>

#include "oswrap.h"

//#define DVB_SECTION_OUTPUT
#define DVB_TABLE_DEBUG

#ifdef DVB_TABLE_DEBUG
#include "TNitTable.h"
#include "TSdtTable.h"
#include "TEitTable.h"
#include "TTotTable.h"
#include "TBatTable.h"
#include "TUdtTable.h"
#include "TNetworkNameDescriptor.h"
#include "TCableDeliverySystemDescriptor.h"
#include "TServiceDescriptor.h"
#include "TShortEventDescriptor.h"
#include "TLocalTimeOffsetDescriptor.h"
#include "TServiceListDescriptor.h"
#include "TExtendedEventDescriptor.h"
#include "TLogicalChannelDescriptor.h"
#include "TComponentDescriptor.h"
#include "TMultilingualServiceNameDescriptor.h"
#include "TMultilingualComponentDescriptor.h"
#include "TMultilingualNetworkNameDescriptor.h"
#include "TParentalRatingDescriptor.h"
#include "TContentDescriptor.h"
#else
#include "TSiTable.h"
#include "TSiSection.h"
#endif

using std::map;
using std::pair;

#if 0
/**
 * Callback function to free event data
 *
 * @param eventData event data
 */
void freeEventData(void *eventData)
{
    // Sanity check
    if(!eventData)
    {
        OS_LOG(DVB_ERROR,  "<%s> event data is null\n", __FUNCTION__);
        return;
    }
#ifdef DVB_SECTION_OUTPUT
    OS_LOG(DVB_TRACE3,  "<%s> event data = %p\n", __FUNCTION__, eventData);
    delete eventData;
#else
    // event data is supposed to point to an SiTable object
    TSiTable* tbl = reinterpret_cast<TSiTable*>(eventData);
    OS_LOG(DVB_TRACE3,  "<%s> table_id = 0x%x\n", __FUNCTION__, tbl->GetTableId());

    // clean up
    delete tbl;
    OS_LOG(DVB_TRACE3,  "<%s> tabled freed\n", __FUNCTION__);
    tbl = NULL;
#endif
}
#endif


/*TSectionParser::TSectionParser(void* context, SendEventCallback callback)
  : m_context(context),
    m_sendEventCb(callback)
*/
//TSectionParser::TSectionParser(rmf_osal_eventmanager_handle_t eventMgr): m_eventMgr(eventMgr)
TSectionParser::TSectionParser()
{
}

TSectionParser::~TSectionParser()
{
  ObserverVector.clear();
}

/**
 * Send a table event using the event manager
 * to let the subscribers know about the new table
 *
 * @param eventType event type
 * @param eventData event data
 * @param dataSize data size
*/
#if 0 
void TSectionParser::sendEvent(uint32_t eventType, void *eventData, size_t dataSize)
{
    rmf_Error ret;
    rmf_osal_event_handle_t eventHandle;
    rmf_osal_event_params_t eventParams = {};

    // Let's set the event parameters
    eventParams.data = eventData;
    eventParams.data_extension = dataSize;
    eventParams.free_payload_fn = freeEventData;

    // Time to create the event itself
    ret = rmf_osal_event_create(RMF_OSAL_EVENT_CATEGORY_INB_FILTER, eventType, &eventParams, &eventHandle);
    if (RMF_SUCCESS != ret)
    {
        OS_LOG(DVB_ERROR,  "<%s:%d> Event creation failed\n", __FUNCTION__, __LINE__);
        return;
    }

    // Sending
    ret = rmf_osal_eventmanager_dispatch_event(m_eventMgr, eventHandle);
    if (RMF_SUCCESS != ret)
    {
        OS_LOG(DVB_ERROR,  "<%s:%d> Event dispatch failed\n", __FUNCTION__, __LINE__);
        // clean up
        rmf_osal_event_delete(eventHandle);
    }
}
#endif
/**
 * Check if tables of certain type are supported or not.
 * Note that some optional DVB tables might not be supported.
 *
 * @param tableId table identifier
 * @return true if supported, false otherwise
 */
bool TSectionParser::IsTableSupported(uint8_t id)
{
    TTableId tableId = static_cast<TTableId>(id);

    // NIT, SDT, BAT, EIT, TDT, TOT, UDT
    if( ((tableId >= TTableId::TABLE_ID_NIT) && (tableId <= TTableId::TABLE_ID_SDT)) ||  
         (tableId == TTableId::TABLE_ID_SDT_OTHER) || (tableId == TTableId::TABLE_ID_BAT)  ||
        ((tableId >= TTableId::TABLE_ID_EIT_PF) && (tableId <= TTableId::TABLE_ID_EIT_SCHED_OTHER_END)) ||
         (tableId == TTableId::TABLE_ID_TDT) || (tableId == TTableId::TABLE_ID_TOT) ||
        ((tableId >= TTableId::TABLE_ID_USER_DEFINED_START) && (tableId <= TTableId::TABLE_ID_USER_DEFINED_END)) )
    {
        return true;
    }
    else
    {
        return false;
    }
}

/**
 * Parse SI Section
 *
 * @param data section data
 * @param size data size
 */
void TSectionParser::ParseSiData(uint8_t *data, uint32_t size)
{
    OS_LOG(DVB_TRACE3, 
            "<%s> data: %p, size: 0x%x\n", __FUNCTION__, data, size);

    // Sanity check
    if(!data || size == 0)
    {
        OS_LOG(DVB_ERROR,  "<%s> Invalid parameter passed(%p, 0x%x)\n", __FUNCTION__, data, size);
        return;
    }

    // Check if we support this table or not
    if(!IsTableSupported(data[0]))
    {
        OS_LOG(DVB_WARN,  "<%s> Table id 0x%x is not supported\n", __FUNCTION__, data[0]);
        return;
    }

    TSiSection* section = new TSiSection(data, size);

    OS_LOG(DVB_DEBUG,  "<%s> Handling id = 0x%x, SectionSyntaxIndicator = %d, extId = 0x%x, ver = %d %d/%d\n", __FUNCTION__,
      section->TableId, section->SectionSyntaxIndicator, section->ExtensionTableId,
      section->VersionNumber, section->SectionNumber, section->LastSectionNumber);

    // Find the list in the section map
    pair<uint8_t, uint16_t> key(section->TableId, section->ExtensionTableId);
    TSectionList& secList = m_sectionMap[key];

    // Adding the section to the list
    if(secList.AddSiSection(*section))
    {
        OS_LOG(DVB_TRACE3,  "<%s> Add() returned true\n", __FUNCTION__);
#ifdef DVB_SECTION_OUTPUT
        // Have to make a copy here
        uint8_t *p = new uint8_t[size];
        std::copy(data, data + size, p);

        // Let's publish the event right away
        // TODO: KSS I really don't understand why this sendevent being called from here.
        // Need to eliminate this.
        //sendEvent((uint32_t)section->TableId, p, size);
        // TODO: KSS as to keep things as before need to have the call back mechanism to notify the observers about the SI data.
        NotifyDvbSectionParserObserver((uint32_t)section->TableId, p, size);
#else
        // Let's check if the table became complete
        if(secList.GetCompletenessFlag())
        {
            OS_LOG(DVB_DEBUG,  "<%s> table is complete\n", __FUNCTION__);
            OS_LOG(DVB_DEBUG,  "<%s> SectionList: %s\n", __FUNCTION__, secList.ToString().c_str());

            // Time to build the table
            TSiTable* tbl = secList.BuildTable();
            if(tbl)
            {
// This block parses certain tables and logs the results. Used for debugging purposes only.
#ifdef DVB_TABLE_DEBUG
                if(tbl->GetTableId() == TTableId::TABLE_ID_NIT)
                {
                    OS_LOG(DVB_DEBUG,  "<%s> NIT table received, id: 0x%x, extId: 0x%x\n", __FUNCTION__, tbl->GetTableId(), tbl->GetTableExtensionId());
                    TNitTable* nit = static_cast<TNitTable*>(tbl);
                    const std::vector<TMpegDescriptor>& descriptors = nit->GetNetworkDescriptors();
                    const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(descriptors, TDescriptorTag::NETWORK_NAME_TAG);
                    if(desc)
                    {
                        TNetworkNameDescriptor netName(*desc);
                        OS_LOG(DVB_DEBUG,  "<%s> NIT table, network name: %s\n", __FUNCTION__, netName.GetName().c_str());
                    }
                    else
                    {
                        OS_LOG(DVB_DEBUG,  "<%s> NIT table: network name descriptor not found\n", __FUNCTION__);
                    }

                    const std::vector<TTransportStream>& tsList = nit->GetTransportStreams();

                    // Let's iterate through the list of transport streams in order to extract certain ts descriptors
                    for(auto it = tsList.begin(), end = tsList.end(); it != end; ++it)
                    {
                        const std::vector<TMpegDescriptor>& tsDescriptors = it->GetTsDescriptors();
                        const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(tsDescriptors, TDescriptorTag::CABLE_DELIVERY_TAG);
                        if(desc)
                        {
                            TCableDeliverySystemDescriptor cable(*desc);
                            OS_LOG(DVB_DEBUG,  "<%s> NIT table: freq = 0x%x(%d), mod = 0x%x, symbol_rate = 0x%x(%d)\n",
                              __FUNCTION__, cable.GetFrequencyBcd(), cable.GetFrequency(),
                              cable.GetModulation(), cable.GetSymbolRateBcd(), cable.GetSymbolRate());
                        }
                        else
                        {
                            OS_LOG(DVB_DEBUG,  "<%s> NIT table: cable delivery descriptor not found\n", __FUNCTION__);
                        }
                    }

                    std::vector<TMpegDescriptor> netList = TMpegDescriptor::FindAllMpegDescriptors(descriptors, TDescriptorTag::MULTILINGUAL_NETWORK_NAME_TAG);

                    // Let's dump all the multilingual network name descriptors to the log
                    for(auto ext_it = netList.begin(), ext_end = netList.end(); ext_it != ext_end; ++ext_it)
                    {
                        TMultilingualNetworkNameDescriptor netDesc(*ext_it);
                        for(uint8_t i = 0; i < netDesc.GetCount(); i++)
                        {
                            OS_LOG(DVB_DEBUG,  "<%s> NIT table: multilingual network name[%d] (%s): %s\n",
                                    __FUNCTION__, i, netDesc.GetLanguageCode(i).c_str(), netDesc.GetNetworkName(i).c_str());
                        }
                    }
                }
                else if(tbl->GetTableId() == TTableId::TABLE_ID_SDT || tbl->GetTableId() == TTableId::TABLE_ID_SDT_OTHER)
                {
                    OS_LOG(DVB_DEBUG,  "<%s> SDT table received, id: 0x%x, extId: 0x%x\n", __FUNCTION__, tbl->GetTableId(), tbl->GetTableExtensionId());
                    TSdtTable* sdt = static_cast<TSdtTable*>(tbl);

                    const std::vector<TSdtService>& serviceList = sdt->GetServices();

                    // Let's iterate through the list of services in order to extract certain service descriptors
                    for(auto it = serviceList.begin(), end = serviceList.end(); it != end; ++it)
                    {
                        const std::vector<TMpegDescriptor>& serviceDescriptors = it->GetServiceDescriptors();
                        const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(serviceDescriptors, TDescriptorTag::SERVICE_TAG);
                        if(desc)
                        {
                            TServiceDescriptor servDesc(*desc);
                            OS_LOG(DVB_DEBUG,  "<%s> SDT table: type = 0x%x, provider = %s, name = %s\n",
                                    __FUNCTION__, servDesc.GetServiceType(), servDesc.GetServiceProviderName().c_str(), servDesc.GetServiceName().c_str());
                        }
                        else
                        {
                            OS_LOG(DVB_DEBUG,  "<%s> SDT table: service descriptor not found\n", __FUNCTION__);
                        }

                        std::vector<TMpegDescriptor> nameList = TMpegDescriptor::FindAllMpegDescriptors(serviceDescriptors, TDescriptorTag::MULTILINGUAL_SERVICE_NAME_TAG);
                        for(auto ext_it = nameList.begin(), ext_end = nameList.end(); ext_it != ext_end; ++ext_it)
                        {
                            TMultilingualServiceNameDescriptor nameDesc(*ext_it);

                            // Let's dump the service information in all available languages
                            for(uint8_t i = 0; i < nameDesc.GetCount(); i++)
                            {
                                OS_LOG(DVB_DEBUG,  "<%s> SDT table: multilingual service name[%d] (%s): provider = %s, name = %s\n",
                                        __FUNCTION__, i, nameDesc.GetLanguageCode(i).c_str(), nameDesc.GetServiceProviderName(i).c_str(),  nameDesc.GetServiceName(i).c_str());
                            }
                        }
                    }
                }
                else if((tbl->GetTableId() >= TTableId::TABLE_ID_EIT_PF) && (tbl->GetTableId() <= TTableId::TABLE_ID_EIT_SCHED_OTHER_END))
                {
                    OS_LOG(DVB_DEBUG,  "<%s> EIT table received, id: 0x%x, extId: 0x%x\n", __FUNCTION__, tbl->GetTableId(), tbl->GetTableExtensionId());
                    TEitTable* eit = static_cast<TEitTable*>(tbl);

                    const std::vector<TEitEvent>& eventList = eit->GetEvents();
                    for(auto it = eventList.begin(), end = eventList.end(); it != end; ++it)
                    {
                        OS_LOG(DVB_DEBUG,  "<%s> EIT table: event_id = 0x%x, duration = 0x%x(%d), status = %d\n",
                                __FUNCTION__, it->GetEventId(), it->GetDurationBcd(), it->GetDuration(), it->GetRunningStatus());
                        const std::vector<TMpegDescriptor>& eventDescriptors = it->GetEventDescriptors();
                        std::vector<TMpegDescriptor> shortList = TMpegDescriptor::FindAllMpegDescriptors(eventDescriptors, TDescriptorTag::SHORT_EVENT_TAG);

                        // Let's dump all short event descriptors to the log
                        for(auto ext_it = shortList.begin(), ext_end = shortList.end(); ext_it != ext_end; ++ext_it)
                        {
                            TShortEventDescriptor eventDesc(*ext_it);
                            OS_LOG(DVB_DEBUG,  "<%s> EIT table: lang_code = %s, name = %s, text = %s\n",
                                    __FUNCTION__, eventDesc.GetLanguageCode().c_str(), eventDesc.GetEventName().c_str(), eventDesc.GetText().c_str());
                        }

                        std::vector<TMpegDescriptor> extList = TMpegDescriptor::FindAllMpegDescriptors(eventDescriptors, TDescriptorTag::EXTENDED_EVENT_TAG);

                        // Let's dump all extended event descriptors to the log
                        for(auto ext_it = extList.begin(), ext_end = extList.end(); ext_it != ext_end; ++ext_it)
                        {
                           TExtendedEventDescriptor eventDesc(*ext_it);
                            OS_LOG(DVB_DEBUG,  "<%s> EIT table: %d/%d, lang_code = %s, text = %s\n",
                                    __FUNCTION__, eventDesc.GetNumber(), eventDesc.GetLastNumber(), eventDesc.GetLanguageCode().c_str(), eventDesc.GetText().c_str());
                            for(uint8_t i = 0; i < eventDesc.GetNumberOfItems(); i++)
                            {
                                OS_LOG(DVB_DEBUG,  "<%s> EIT table: item[%d] %s: %s\n", __FUNCTION__, i, eventDesc.GetItemDescription(i).c_str(), eventDesc.GetItem(i).c_str());
                            }
                        }

                        const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(eventDescriptors, TDescriptorTag::PARENTAL_RATING_TAG);
                        if(desc)
                        {
                            TParentalRatingDescriptor prDesc(*desc);

                            // Let's dump the rating value for all available countries
                            for(uint8_t i = 0; i < prDesc.GetCount(); i++)
                            {
                                OS_LOG(DVB_DEBUG,  "<%s> EIT table: PR[%d] country_code = %s, rating = 0x%x\n",
                                        __FUNCTION__, i, prDesc.GetCountryCode(i).c_str(), prDesc.GetRating(i));
                            }
                        }
                        else
                        {
                            OS_LOG(DVB_DEBUG,  "<%s> EIT table: parental rating descriptor not found\n", __FUNCTION__);
                        }

                        desc = TMpegDescriptor::FindMpegDescriptor(eventDescriptors, TDescriptorTag::CONTENT_DESCRIPTOR_TAG);
                        if(desc)
                        {
                            TContentDescriptor contentDesc(*desc);

                            // Let's dump all the available content identifiers to the log
                            for(uint8_t i = 0; i < contentDesc.GetCount(); i++)
                            {
                                OS_LOG(DVB_DEBUG,  "<%s> EIT table: [%d] nibble_lvl_1 = 0x%x, nibble_lvl_2 = 0x%x, user_byte = 0x%x\n",
                                        __FUNCTION__, i, contentDesc.GetNibbleLvl1(i), contentDesc.GetNibbleLvl2(i), contentDesc.GetUserByte(i));
                            }
                        }
                        else
                        {
                            OS_LOG(DVB_DEBUG,  "<%s> EIT table: content descriptor not found\n", __FUNCTION__);
                        }

                        std::vector<TMpegDescriptor> compList = TMpegDescriptor::FindAllMpegDescriptors(eventDescriptors, TDescriptorTag::MULTILINGUAL_COMPONENT_TAG);
                        for(auto ext_it = compList.begin(), ext_end = compList.end(); ext_it != ext_end; ++ext_it)
                        {
                            TMultilingualComponentDescriptor compDesc(*ext_it);

                            // Let's dump the component information in all available languages
                            for(uint8_t i = 0; i < compDesc.GetCount(); i++)
                            {
                                OS_LOG(DVB_DEBUG,  "<%s> EIT table: multilingual component descriptor[%d] tag = 0x%x, (%s): text = %s\n",
                                        __FUNCTION__, i, compDesc.GetComponentTag(), compDesc.GetLanguageCode(i).c_str(), compDesc.GetText(i).c_str());
                            }
                        }
                    }
                }
                else if((tbl->GetTableId() == TTableId::TABLE_ID_TDT) || (tbl->GetTableId() == TTableId::TABLE_ID_TOT))
                {
                    TTotTable* tot = static_cast<TTotTable*>(tbl);
                    OS_LOG(DVB_DEBUG,  "<%s> TDT/TOT table received, id: 0x%x, UTC: %" PRId64"\n", __FUNCTION__, tot->GetTableId(), tot->GetUtcTimeBcd());
                    if(tot->GetTableId() == TTableId::TABLE_ID_TOT)
                    {
                        std::vector<TMpegDescriptor>& timeDescriptors = tot->GetDescriptors();
                        const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(timeDescriptors, TDescriptorTag::LOCAL_TIME_OFFSET_TAG);
                        if(desc)
                        {
                            TLocalTimeOffsetDescriptor offsetDesc(*desc);
                            for(uint8_t i = 0; i < offsetDesc.GetCount(); i++)
                            {
                                OS_LOG(DVB_DEBUG,  "<%s> TOT table: time_offset[%d] code = %s, reg_id = 0x%x, pol = %d, offset = 0x%x, ToC = %" PRId64", next offset = 0x%x\n",
                                        __FUNCTION__, i, offsetDesc.GetCountryCode(i).c_str(), offsetDesc.GetCountryRegionId(i), offsetDesc.GetPolarity(i),
                                        offsetDesc.GetLocalTimeOffset(i), offsetDesc.GetTimeOfChange(i), offsetDesc.GetNextTimeOffset(i));
                            }
                        }
                        else
                        {
                            OS_LOG(DVB_DEBUG,  "<%s> TOT table: local time offset descriptor not found\n", __FUNCTION__);
                        }
                    }
                }
                else if(tbl->GetTableId() == TTableId::TABLE_ID_BAT)
                {
                    TBatTable* bat = static_cast<TBatTable*>(tbl);
                    OS_LOG(DVB_DEBUG,  "<%s> BAT table received, id: 0x%x, bouquet_id: 0x%x\n", __FUNCTION__, bat->GetTableId(), bat->GetBouquetId());

                    const std::vector<TMpegDescriptor>& bouquetDesc = bat->GetBouquetDescriptors();
                    const TMpegDescriptor* d = TMpegDescriptor::FindMpegDescriptor(bouquetDesc, TDescriptorTag::LOGICAL_CHANNEL_TAG);
                    if(d)
                    {
                        TLogicalChannelDescriptor lcnDesc(*d);
                        for(uint8_t i = 0; i < lcnDesc.GetCount(); i++)
                        {
                            OS_LOG(DVB_DEBUG,  "<%s> BAT table: [%d] service_id = 0x%x, visible = %d, lcn = %d\n",
                                    __FUNCTION__, i, lcnDesc.GetServiceId(i), lcnDesc.IsVisible(i), lcnDesc.GetLogicalChannelNumber(i));
                        }
                    }
                    else
                    {
                        OS_LOG(DVB_DEBUG,  "<%s> BAT table: logical channel descriptor not found\n", __FUNCTION__);
                    }


                    const std::vector<TTransportStream>& tsList = bat->GetTransportStreams();

                    // Let's iterate through the list of transport streams in order to extract certain ts descriptors
                    for(auto it = tsList.begin(), end = tsList.end(); it != end; ++it)
                    {
                        const std::vector<TMpegDescriptor>& tsDescriptors = it->GetTsDescriptors();
                        const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(tsDescriptors, TDescriptorTag::SERVICE_LIST_TAG);
                        if(desc)
                        {
                            TServiceListDescriptor serviceList(*desc);

                            // Let's dump all the information about all services to the log
                            for(uint8_t i = 0; i < serviceList.GetCount(); i++)
                            {
                                OS_LOG(DVB_DEBUG,  "<%s> BAT table: service_id = 0x%x, service_type = 0x%x\n",
                                        __FUNCTION__, serviceList.GetServiceId(i), serviceList.GetServiceType(i));
                            }
                        }
                        else
                        {
                            OS_LOG(DVB_DEBUG,  "<%s> BAT table: service list descriptor not found\n", __FUNCTION__);
                        }
                    }

                }
                else if((tbl->GetTableId() >= TTableId::TABLE_ID_USER_DEFINED_START) && (tbl->GetTableId() <= TTableId::TABLE_ID_USER_DEFINED_END))
                {
                    TUdtTable* udt = static_cast<TUdtTable*>(tbl);
                    const std::vector<TSiSection>& list = udt->GetSectionList();
                    OS_LOG(DVB_DEBUG,  "<%s> UDT table received, id: 0x%x, num of sections: %lu\n", __FUNCTION__, udt->GetTableId(), list.size());
                }
#endif // DVB_TABLE_DEBUG

                // Let's publish the event
                //sendEvent((uint32_t)tbl->GetTableId(), tbl, 0);
                NotifyDvbSectionParserObserver((uint32_t)tbl->GetTableId(), tbl, 0);
// TODO: Rework on the sendeventcallback
                //SendEventCallback((uint32_t)tbl->GetTableId(), tbl, 0);
                // Call platform ipc mechinism (callback)
//TODO: KSS
  /*              if(m_context && m_sendEventCb)
                {
                    // Let's publish the event
                    m_sendEventCb(m_context, (uint32_t)tbl->GetTableId(), tbl, 0);
                }*/
            }
        }
        else // isComplete()
        {
            OS_LOG(DVB_DEBUG,  "<%s> table is not complete yet\n", __FUNCTION__);
        }
#endif // DVB_SECTION_OUTPUT
    }
    else // Add()
    {
        OS_LOG(DVB_DEBUG,  "<%s> Add() returned false\n", __FUNCTION__);
    }

    // clean up
    delete section;
}

void TSectionParser::RegisterDvbSectionParserObserver(IDvbSectionParserObserver* observerObject)
{
  ObserverVector.push_back(observerObject);
}

void TSectionParser::RemoveDvbSectionParserObserver(IDvbSectionParserObserver* observerObject)
{
  ObserverVector.erase(std::remove(ObserverVector.begin(), ObserverVector.end(), observerObject), ObserverVector.end());
}

void TSectionParser::NotifyDvbSectionParserObserver(uint32_t eventType, void *eventData, size_t dataSize)
{
  std::vector<IDvbSectionParserObserver*>::const_iterator iter = ObserverVector.begin();
  for (; iter != ObserverVector.end(); ++iter) {
    (*iter)->SendEvent(eventType, eventData, dataSize);
  }

}
