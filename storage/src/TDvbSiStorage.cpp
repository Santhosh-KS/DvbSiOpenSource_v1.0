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

#include "TDvbSiStorage.h"

// C system includes
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>


// C++ system includes
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <string>


#include "oswrap.h"
#include "TDvbDb.h"
//#include <jansson.h>

#include "TNitTable.h"
#include "TSdtTable.h"
#include "TEitTable.h"
#include "TTotTable.h"
#include "TBatTable.h"
#include "TCableDeliverySystemDescriptor.h"
#include "TShortEventDescriptor.h"
#include "TServiceDescriptor.h"
#include "TMultilingualNetworkNameDescriptor.h"
#include "TNetworkNameDescriptor.h"
#include "TLogicalChannelDescriptor.h"
#include "TParentalRatingDescriptor.h"
#include "TContentDescriptor.h"


//using namespace TDvbStorageNamespace;
using std::map;
using std::pair;
using std::vector;
using std::tuple;
using std::shared_ptr;
using std::string;

using namespace TDvbStorageNamespace;

/**
 * Static Initialization
 */
std::mutex TDvbSiStorage::SiStorageMutex;

/** 
 * Default Constructor
 */
TDvbSiStorage::TDvbSiStorage()
  : PreferredNetworkId(0),
    IsFastScan(false),
    HomeTsFrequency(0),
    HomeTsModulationMode(MODULATION_MODE_UNKNOWN),
    HomeTsSymbolRate(0),
    BarkerFrequency(0),
    BarkerModulationMode(MODULATION_MODE_UNKNOWN),
    BarkerSymbolRate(0),
    BackGroundScanInterval(21600),
    BarkerEitTimout(EIT_PAST_8_DAY_SCHED_TIMEOUT)
{
    DvbScanStatus.ScanState = TDvbScanState::SCAN_STOPPED;
#if 0
    if(RMF_SUCCESS != rmf_osal_eventqueue_create ((const uint8_t* )"DvbSiQueue", &m_eventQueueId))
    {
        OS_LOG(DVB_ERROR,   "<%s> - unable to create event queue\n", __FUNCTION__);
        // TODO: Consider throwing an exception here.
    }
#endif

    // TDvbDb::TFileStatus status = DvbDb.OpenDb(string(rmf_osal_envGet("FEATURE.DVB.DB_FILENAME")));
// TODO: Resolve this private fn cal OpenDb() issue;
#if 0
    TDvbDb::TFileStatus status = StorageDb.OpenDb("Fill in the pathname");
    OS_LOG(DVB_INFO,   "<%s> - DB status = 0x%x\n", __FUNCTION__, status);

    if(status != DvbDb::OPENED && status != DvbDb::CREATED)
    {
        OS_LOG(DVB_ERROR,   "<%s> - Unable to open db\n", __FUNCTION__);
        // TODO: Consider throwing an exception here. We can go on without the DB. 
    }
    bool changed = LoadSettings();
    if(changed)
    {
        OS_LOG(DVB_INFO,   "<%s> - DVB scan settings have changed\n", __FUNCTION__);

        // Need to flush the db if it's populated and force the scan
        if(status == DvbDb::OPENED)
        {
            OS_LOG(DVB_INFO,   "<%s> - Flushing db\n", __FUNCTION__);
            DvbDb.DropTables();
            DvbDb.CreateTables();

            // Forcing a fast scan
            status = DvbDb::CREATED;
        }
    }
    else
    {
        OS_LOG(DVB_DEBUG,   "<%s> - DVB scan settings have not changed\n", __FUNCTION__);
    }
#endif
    StartMonitorThread();

    // Let's scan only if we have home TS parameters set
    if(HomeTsFrequency && HomeTsModulationMode && HomeTsSymbolRate)
    {
        // DvbDb::open() calls sanityCheck() that in turn calls contentCheck(),
        // so we don't have to make the sanity and content checks here.
        // Let's skip the fast scan only if status == OPENED
        //if(status == DvbDb::OPENED)
        if(true)
        {
            // Kick off a periodic background scan only
            StartScan(false);
        }
        else
        {
            // Let's do both fast and bkgd scans
            StartScan(true);
        }
    }
    else
    {
        OS_LOG(DVB_ERROR,   "<%s> - home ts parameters not set correctly. not scanning.\n", __FUNCTION__);
    }
}

/**
 * Load environmental runtime settings
 *
 * @return bool true no change in settings; false settings have changed
 */
bool TDvbSiStorage::LoadSettings()
{

    // TODO: Refactor. This function does much more that it is supposed to.
    bool changed = false;
#if 0

    // Let's read all the settings from the DB first
    DvbDb.LoadSettings(); 

    // Preferred nentwork id
    const char* value = rmf_osal_envGet("FEATURE.DVB.PREFERRED_NETWORK_ID");
    const char* dbValue = DvbDb.GetSetting("FEATURE.DVB.PREFERRED_NETWORK_ID");
    if(value && dbValue && strcmp(value, dbValue) != 0)
    {
        changed = true;
        OS_LOG(DVB_DEBUG,   "<%s> Preferred network id changed. Env: nid = %s Db: nid = %s\n", __FUNCTION__, value, dbValue);
    }

    if(value)
    {
        std::stringstream(string(value)) >> PreferredNetworkId;
        OS_LOG(DVB_DEBUG,   "<%s> preferred network id = 0x%x\n", __FUNCTION__, PreferredNetworkId);
    }
    DvbDb.setSetting("FEATURE.DVB.PREFERRED_NETWORK_ID", value);

    // Bouquets
    value = rmf_osal_envGet("FEATURE.DVB.BOUQUET_ID_LIST");
    if(value)
    {
        string str(value);
        std::stringstream ss(str);
        uint32_t id = 0;

        while(ss >> id)
        {
            OS_LOG(DVB_DEBUG,   "<%s> adding bouquet_id 0x%x to the list\n", __FUNCTION__, id);
            HomeBouquetsVector.push_back(id);

            if(ss.peek() == ',')
            {
                ss.ignore();
            }
        }
    }
    DvbDb.SetSetting("FEATURE.DVB.BOUQUET_ID_LIST", value);

    // Home TS frequency
    value = rmf_osal_envGet("FEATURE.DVB.HOME_TS_FREQUENCY");
    dbValue = DvbDb.GetSetting("FEATURE.DVB.HOME_TS_FREQUENCY");
    if(value && dbValue && strcmp(value, dbValue) != 0)
    {
        changed = true;
        OS_LOG(DVB_DEBUG,   "<%s> Home TS frequency changed. Env: freq = %s Db: freq = %s\n", __FUNCTION__, value, dbValue);
    }

    if(value)
    {
        std::stringstream(string(value)) >> HomeTsFrequency;
    }
    DvbDb.SetSetting("FEATURE.DVB.HOME_TS_FREQUENCY", value);

    // Home TS Modulation
    value = rmf_osal_envGet("FEATURE.DVB.HOME_TS_MODULATION");
    if(value)
    {
        uint32_t mod = 0;
        std::stringstream(string(value)) >> mod;
        HomeTsModulationMode = static_cast<TModulationMode>(mod);
    }
    DvbDb.SetSetting("FEATURE.DVB.HOME_TS_MODULATION", value);

    // Home TS symbol rate
    value = rmf_osal_envGet("FEATURE.DVB.HOME_TS_SYMBOL_RATE");
    if(value)
    {
        std::stringstream(string(value)) >> HomeTsSymbolRate;
    }
    DvbDb.SetSetting("FEATURE.DVB.HOME_TS_SYMBOL_RATE", value);

    OS_LOG(DVB_DEBUG,   "<%s> Home TS: freq = %d, mod = %d, sym_rate = %d\n",
            __FUNCTION__, HomeTsFrequency, HomeTsModulationMode, HomeTsSymbolRate);

    // Barker TS frequency
    value = rmf_osal_envGet("FEATURE.DVB.BARKER_TS_FREQUENCY");
    if(value)
    {
        std::stringstream(string(value)) >> BarkerFrequency;
    }
    DvbDb.SetSetting("FEATURE.DVB.BARKER_TS_FREQUENCY", value);

    // Barker TS Modulation
    value = rmf_osal_envGet("FEATURE.DVB.BARKER_TS_MODULATION");
    if(value)
    {
        uint32_t mod = 0;
        std::stringstream(string(value)) >> mod;
        BarkerModulationMode = static_cast<TModulationMode>(mod);
    }
    DvbDb.SetSetting("FEATURE.DVB.BARKER_TS_MODULATION", value);

    // Berker TS symbol rate
    value = rmf_osal_envGet("FEATURE.DVB.BARKER_TS_SYMBOL_RATE");
    if(value)
    {
        std::stringstream(string(value)) >> BarkerSymbolRate;
    }
    DvbDb.SetSetting("FEATURE.DVB.BARKER_TS_SYMBOL_RATE", value);

    // Barker EIT timeout
    value = rmf_osal_envGet("FEATURE.DVB.BARKER_EIT_TIMEOUT");
    if(value)
    {
        std::stringstream(string(value)) >> BarkerEitTimout;
    }
    DvbDb.SetSetting("FEATURE.DVB.BARKER_EIT_TIMEOUT", value);

    OS_LOG(DVB_DEBUG,   "<%s> Barker TS: freq = %d, mod = %d, sym_rate = %d, eit_timeout = %d\n",
            __FUNCTION__, BarkerFrequency, BarkerModulationMode, BarkerSymbolRate, BarkerEitTimout);

    // Smart scan flag
    value = rmf_osal_envGet("FEATURE.DVB.FAST_SCAN_SMART");
    if(value && (strcmp(value, "TRUE") == 0))
    {
        IsFastScan = true;
    }
    DvbDb.SetSetting("FEATURE.DVB.FAST_SCAN_SMART", value);

    // Interval between background scans
    value = rmf_osal_envGet("FEATURE.DVB.BACKGROUND_SCAN_INTERVAL");
    if(value)
    {
        std::stringstream(string(value)) >> BackGroundScanInterval;
    }
    DvbDb.SetSetting("FEATURE.DVB.BACKGROUND_SCAN_INTERVAL", value);

    OS_LOG(DVB_DEBUG,   "<%s> Home TS: smart = %d, bkgd scan interval = %d sec\n",
            __FUNCTION__, IsFastScan, BackGroundScanInterval);

    // DVB profile config file
    value = rmf_osal_envGet("FEATURE.DVB.NETWORK_PROFILE_FILENAME");
    if(value)
    {
        DvbConfigProfileFile = string(value);
    }
    DvbDb.SetSetting("FEATURE.DVB.NETWORK_PROFILE_FILENAME", value);

    OS_LOG(DVB_DEBUG,   "<%s> profile cfg file name %s\n",
            __FUNCTION__, DvbConfigProfileFile.c_str());

    DvbDb.clearSettings();

#endif
    return changed;
}

#if 0
/**
  * Return queue handle for event monitor thread
  *
  * @return rmf_oscal_eventqueue 
  */
rmf_osal_eventqueue_handle_t TDvbSiStorage::GetInputQueue()
{
    return m_eventQueueId;
}
#endif

/**
 * Thread start up function
 */
// TODO: KSS
#if 0
static void EventMonitorThreadFn(void *arg)
{
    TDvbSiStorage *pDvbSiStorage = (TDvbSiStorage*)arg;
    pDvbSiStorage->EventMonitorThread();
}
#endif

/**
 * Start function for monitor thread
 */
void TDvbSiStorage::StartMonitorThread()
{
// TODO:KSS Evaluate the need for thread here. if required use std::thread.
#if 0
    rmf_osal_threadCreate(EventMonitorThreadFn, (void*)this, RMF_OSAL_THREAD_PRIOR_DFLT, RMF_OSAL_THREAD_STACK_SIZE, &m_threadId, "DvbSi_Thread");
#endif 
}


/**
 * Stop function for monitor thread
 */
void TDvbSiStorage::StopMonitorThread()
{
// TODO: KSS
    //rmf_osal_threadDestroy(m_threadId);
}

// Constellation
typedef enum _DVBConstellation
{
    DVB_CONSTELLATION_UNDEFINED,
    DVB_CONSTELLATION_QAM16,
    DVB_CONSTELLATION_QAM32,
    DVB_CONSTELLATION_QAM64,
    DVB_CONSTELLATION_QAM128,
    DVB_CONSTELLATION_QAM256
} DVBConstellation;

/**
 * Return TModulationMode enumeration based on DVBConstellation enumeration
 *
 * @param in DVBConstellation enum
 */
static TModulationMode mapModulationMode(DVBConstellation in)
{
    TModulationMode out;

    switch(in)
    {
        case DVB_CONSTELLATION_QAM16:
            out = MODULATION_MODE_QAM16;
            break;
        case DVB_CONSTELLATION_QAM32:
            out = MODULATION_MODE_QAM32;
            break;
        case DVB_CONSTELLATION_QAM64:
            out = MODULATION_MODE_QAM64;
            break;
        case DVB_CONSTELLATION_QAM128:
            out = MODULATION_MODE_QAM128;
            break;
        case DVB_CONSTELLATION_QAM256:
            out = MODULATION_MODE_QAM256;
            break;
        case DVB_CONSTELLATION_UNDEFINED:
        default:
            out = MODULATION_MODE_UNKNOWN;
            break;
    }

    return out;
}

/**
 * Return a vector of Transport streams
 *
 * @param nId network id
 * @return vector of shared pointers of TTransportStream structures
 */
vector<shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> TDvbSiStorage::GetTsListByNetIdCache(uint16_t nId)
{
    vector<shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> ret;

    std::lock_guard<std::mutex> lock(CacheDataMutex);

    auto it = NitTableMap.begin();
    if(nId != 0)
    {
        it = NitTableMap.find(nId);
    }
    else if(PreferredNetworkId != 0)
    {
        it = NitTableMap.find(PreferredNetworkId);
    }

    if(it == NitTableMap.end())
    {
        return ret;
    }

    const vector<TTransportStream>& tsList = it->second->GetTransportStreams();
    for(auto it = tsList.begin(), end = tsList.end(); it != end; ++it)
    {
        const std::vector<TMpegDescriptor>& tsDescriptors = it->GetTsDescriptors();
        const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(tsDescriptors, TDescriptorTag::CABLE_DELIVERY_TAG);
        if(desc)
        {
            TCableDeliverySystemDescriptor cable(*desc);
            OS_LOG(DVB_DEBUG,   "<%s> NIT table: freq = 0x%x(%d), mod = 0x%x, symbol_rate = 0x%x(%d)\n",
                    __FUNCTION__, cable.GetFrequencyBcd(), cable.GetFrequency(), cable.GetModulation(), cable.GetSymbolRateBcd(), cable.GetSymbolRate());
            std::shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct> ts(new TStorageTransportStreamStruct(cable.GetFrequency(),
   mapModulationMode((DVBConstellation)cable.GetModulation()), cable.GetSymbolRate(), it->GetOriginalNetworkId(), it->GetTsId()));

            OS_LOG(DVB_DEBUG,   "<%s> ts_id =0x%x, frequency: %d, Modulation: %d, symbol_rate: 0x%x\n",
                    __FUNCTION__, ts->TransportStreamId, ts->Frequency,  ts->Modulation, ts->SymbolRate);
            ret.push_back(ts);
        }
        else
        {
            OS_LOG(DVB_ERROR,   "<%s> NIT table: cable delivery descriptor not found\n", __FUNCTION__);
        }
    }

    return ret;
}

/**
 * Return a vector of Transport streams
 *
 * @param nId network id
 * @return vector of shared pointers of TransportStream structures
 */
vector<shared_ptr<TStorageTransportStreamStruct>> TDvbSiStorage::GetTsListByNetId(uint16_t nId)
{
    vector<shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> ret;

    uint16_t networkId;

    if(nId != 0)
    {
        networkId = nId;
    }
    else if(PreferredNetworkId != 0)
    {
        networkId = PreferredNetworkId;
    }
    else
    {
        OS_LOG(DVB_ERROR,   "<%s> Invalid network id: %d, 0x%x\n", __FUNCTION__, nId, nId);
        return ret;
    }

    string cmdStr("SELECT t.original_network_id, t.transport_id, t.frequency, t.Modulation, t.symbol_rate " \
                  "FROM Transport t INNER JOIN Network n "                                                  \
                  "ON t.network_fk = n.network_pk "                                                         \
                  "WHERE n.network_id = ");

    std::stringstream ss;
    ss << networkId;
    cmdStr += ss.str();
    cmdStr += " ORDER BY t.transport_id ASC;";

    vector<vector<string>> results = StorageDb.QueryDb(cmdStr);

    OS_LOG(DVB_DEBUG,   "<%s> nid = %d num rows: %lu\n", __FUNCTION__, networkId, results.size());

    for(vector<vector<string>>::iterator it = results.begin(); it != results.end(); ++it)
    {
        vector<string> row = *it;

        if(row.size() == 5) // 5 columns specified in select statement
        {
            int onId;
            int tsId;
            int frequency;
            int symbolRate;
            int mod;

            std::stringstream(row.at(0)) >> onId;
            std::stringstream(row.at(1)) >> tsId;
            std::stringstream(row.at(2)) >> frequency;
            std::stringstream(row.at(3)) >> mod;
            std::stringstream(row.at(4)) >> symbolRate;

            OS_LOG(DVB_DEBUG,   "<%s> onId: %d tsId: %d frequency: %d mod: %d symbolRate: %d\n", __FUNCTION__, onId, tsId, frequency, mod, symbolRate);
            std::shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct> ts(new TStorageTransportStreamStruct(static_cast<uint32_t>(frequency),
                                                              mapModulationMode((DVBConstellation)mod),
                                                              static_cast<uint32_t>(symbolRate), static_cast<uint16_t>(onId),
                                                              static_cast<uint16_t>(tsId)));
            ret.push_back(ts);
        }
    }

    return ret;
}

/**
 * Return a vector of Service_t structures
 *
 * @param nId network id
 * @param tsId transport stream id
 * @return vector of shared pointers of Service_t structures
 */
vector<shared_ptr<TDvbStorageNamespace::ServiceStruct>> TDvbSiStorage::GetServiceListByTsIdCache(uint16_t nId, uint16_t tsId)
{
    vector<shared_ptr<TDvbStorageNamespace::ServiceStruct>> ret;

    std::lock_guard<std::mutex> lock(CacheDataMutex);

    OS_LOG(DVB_DEBUG,   "<%s> called: nid.tsid = 0x%x.0x%x\n", __FUNCTION__, nId, tsId);

    pair<uint16_t, uint16_t> key(nId, tsId);
    auto it = SdtTableMap.find(key);
    if(it == SdtTableMap.end())
    {
        OS_LOG(DVB_ERROR,   "<%s> No SDT found for nid.tsid = 0x%x.0x%x\n", __FUNCTION__, nId, tsId);
        return ret;
    }

    const std::vector<TSdtService>& serviceList = it->second->GetServices();
    for(auto srv = serviceList.begin(), end = serviceList.end(); srv != end; ++srv)
    {
        const std::vector<TMpegDescriptor>& serviceDescriptors = srv->GetServiceDescriptors();
        const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(serviceDescriptors, TDescriptorTag::SERVICE_TAG);
        if(desc)
        {
            TServiceDescriptor servDesc(*desc);
            OS_LOG(DVB_DEBUG,   "<%s> SDT table: type = 0x%x, provider = %s, name = %s\n",
                    __FUNCTION__, servDesc.GetServiceType(), servDesc.GetServiceProviderName().c_str(), servDesc.GetServiceName().c_str());

            std::shared_ptr<TDvbStorageNamespace::Service> service(new Service(it->second->GetOriginalNetworkId(), it->second->GetTableExtensionId(),
                                                         srv->GetServiceId(), servDesc.GetServiceName()));

            ret.push_back(service);
        }
        else
        {
            OS_LOG(DVB_ERROR,   "<%s> SDT table: service descriptor not found\n", __FUNCTION__);
        }
    }

    return ret;
}

/**
 * Return a vector of ServiceStruct structures
 *
 * @param nId network id
 * @param tsId transport stream id
 * @return vector of shared pointers of ServiceStruct structures
 */
vector<shared_ptr<TDvbStorageNamespace::ServiceStruct>> TDvbSiStorage::GetServiceListByTsId(uint16_t nId, uint16_t tsId)
{
    vector<shared_ptr<TDvbStorageNamespace::ServiceStruct>> ret;

    string cmdStr("SELECT t.original_network_id, t.transport_id, s.service_id, s.service_name FROM Service s " \
                  " INNER JOIN Transport t "                                                                   \
                  " ON s.transport_fk = t.transport_pk "                                                       \
                  "WHERE t.original_network_id = ");

    std::stringstream ss;
    ss << nId;
    cmdStr += ss.str();
    cmdStr += " AND t.transport_id = ";

    ss.str("");
    ss << tsId;
    cmdStr += ss.str();
    cmdStr += " ORDER BY s.service_id ASC;";

    vector<vector<string>> results = StorageDb.QueryDb(cmdStr);

    OS_LOG(DVB_DEBUG,   "<%s> nid.tsid = %d.%d num rows: %lu\n", __FUNCTION__, nId, tsId, results.size());

    for(vector<vector<string>>::iterator it = results.begin(); it != results.end(); ++it)
    {
        vector<string> row = *it;

        if(row.size() == 4)  // number of columns in select statement
        {
            int onId;
            int tsId;
            int serviceId;

            std::stringstream(row.at(0)) >> onId;
            std::stringstream(row.at(1)) >> tsId;
            std::stringstream(row.at(2)) >> serviceId;
            string serviceName = row.at(3);

            OS_LOG(DVB_DEBUG,   "<%s> onId: %d tsId: %d serviceId: %d %s\n", __FUNCTION__, onId, tsId, serviceId, serviceName.c_str());

            std::shared_ptr<TDvbStorageNamespace::Service> service(new Service(static_cast<uint16_t>(onId),
                                                         static_cast<uint16_t>(tsId), static_cast<uint16_t>(serviceId), serviceName));

            ret.push_back(service);
        }
    }

    return ret;
}

/**
 * Return a vector of Events 
 *
 * @param nId network id
 * @param tsId transport stream id
 * @param sId service id
 * @return vector of shared pointers of EventStruct structures
 */
vector<shared_ptr<TDvbStorageNamespace::EventStruct>> TDvbSiStorage::GetEventListByServiceIdCache(uint16_t nId, uint16_t tsId, uint16_t sId)
{
    vector<shared_ptr<TDvbStorageNamespace::EventStruct>> ret;

    std::lock_guard<std::mutex> lock(CacheDataMutex);

    OS_LOG(DVB_DEBUG,   "<%s> called: nid.tsid.sid = 0x%x.0x%x.0x%x\n", __FUNCTION__, nId, tsId, sId);

    tuple<uint16_t, uint16_t, uint16_t, bool> key(nId, tsId, sId, false);
    auto it = TEitTableMap.find(key);
    if(it == TEitTableMap.end())
    {
        OS_LOG(DVB_ERROR,   "<%s> No EIT found for nid.tsid.sid = 0x%x.0x%x.0x%x\n", __FUNCTION__, nId, tsId, sId);
        return ret;
    }

    const std::vector<TEitEvent>& eventList = it->second->GetEvents();
    for(auto e = eventList.begin(), end = eventList.end(); e != end; ++e)
    {
        OS_LOG(DVB_DEBUG,   "<%s> EIT table: event_id = 0x%x, duration = %d, status = %d\n",
                __FUNCTION__, e->GetEventId(), e->GetDuration(), e->GetRunningStatus());

        const std::vector<TMpegDescriptor>& eventDescriptors = e->GetEventDescriptors();
        std::vector<TMpegDescriptor> shortList = TMpegDescriptor::FindAllMpegDescriptors(eventDescriptors, TDescriptorTag::SHORT_EVENT_TAG);

        for(auto ext_it = shortList.begin(), ext_end = shortList.end(); ext_it != ext_end; ++ext_it)
        {
            TShortEventDescriptor eventDesc(*ext_it);
            OS_LOG(DVB_DEBUG,   "<%s> EIT table: lang_code = %s, name = %s, text = %s\n",
                    __FUNCTION__, eventDesc.GetLanguageCode().c_str(), eventDesc.GetEventName().c_str(), eventDesc.GetText().c_str());

            std::shared_ptr<TDvbStorageNamespace::Event> event(new Event);
            event->NetworkId = it->second->GetNetworkId();
            event->TransportStreamId = it->second->GetTsId();
            event->ServiceId = it->second->GetTableExtensionId();
            event->EventId = e->GetEventId();
            event->StartTime = e->GetStartTime();
            event->Duration = e->GetDuration();
            event->EventName = eventDesc.GetEventName();
            event->EventText = eventDesc.GetText();

            OS_LOG(DVB_TRACE1,   "<%s> nid.tsid.sid = 0x%x.0x%x.0x%x, event id: %d, start time: %" PRId64", duration: %d\n",
                    __FUNCTION__, nId, tsId, sId, event->EventId, event->StartTime, event->Duration);
            OS_LOG(DVB_TRACE1,   "<%s> nid.tsid.sid = 0x%x.0x%x.0x%x, event id: %d, event name: %s, text: %s\n",
                    __FUNCTION__, nId, tsId, sId, event->EventId, event->EventName.c_str(), event->EventText.c_str());
            ret.push_back(event);
        }
    }

    return ret;
}

/**
 * Return a vector of Events 
 *
 * @param nId network id
 * @param tsId transport stream id
 * @param sId service id
 * @return vector of shared pointers of EventStruct structures
 */
vector<shared_ptr<TDvbStorageNamespace::EventStruct>> TDvbSiStorage::GetEventListByServiceId(uint16_t nId, uint16_t tsId, uint16_t sId)
{
    vector<shared_ptr<TDvbStorageNamespace::EventStruct>> ret;

    string cmdStr("SELECT e.network_id, e.transport_id, e.service_id, e.event_id, e.start_time, e.duration, " \
                  " ei.title, ei.description FROM EventItem ei "                                              \
                  " INNER JOIN Event e "                                                                      \
                  " ON ei.event_fk = e.event_pk "                                                             \
                  "WHERE e.network_id = ");

    std::stringstream ss;
    ss << nId;
    cmdStr += ss.str();
    cmdStr += " AND e.transport_id = ";

    ss.str("");
    ss << tsId;
    cmdStr += ss.str();
    cmdStr += " AND e.service_id = ";

    ss.str("");
    ss << sId;
    cmdStr += ss.str();
    cmdStr += " ORDER BY e.event_id ASC;";

    vector<vector<string>> results = StorageDb.QueryDb(cmdStr);

    OS_LOG(DVB_DEBUG,   "<%s> nid.tsid.sid = %d.%d.%d num rows: %lu\n", __FUNCTION__, nId, tsId, sId, results.size());

    for(vector<vector<string>>::iterator it = results.begin(); it != results.end(); ++it)
    {
        vector<string> row = *it;

        if(row.size() == 8)  // number of columns in select statement
        {
            int onId;
            int tsId;
            int serviceId;
            int eventId;
            long long int startTime;
            int duration;

            std::stringstream(row.at(0)) >> onId;
            std::stringstream(row.at(1)) >> tsId;
            std::stringstream(row.at(2)) >> serviceId;
            std::stringstream(row.at(3)) >> eventId;
            std::stringstream(row.at(4)) >> startTime;
            std::stringstream(row.at(5)) >> duration;
            string title = row.at(6);
            string description = row.at(7);

            OS_LOG(DVB_TRACE1,   "<%s> nId: %d tsId: %d serviceId: %d eventId: %d startTime: %lld " \
                                   "duration: %d title length: %lu description length: %lu\n", __FUNCTION__, nId, tsId, serviceId, 
                                   eventId, startTime, duration, title.size(), description.size());

            std::shared_ptr<TDvbStorageNamespace::Event> event(new Event);
            event->NetworkId = nId;
            event->TransportStreamId = tsId;
            event->ServiceId = serviceId;
            event->EventId = eventId;
            event->StartTime = startTime;
            event->Duration = duration;
            event->EventName = title;
            event->EventText = description;

            ret.push_back(event);
        }
    }

    return ret;
}

/**
 * Handle Nit table
 *
 * @param nit Nit table
 */
void TDvbSiStorage::HandleNitEvent(const TNitTable& nit)
{
    // TODO: Consider removing one level of handle methods.
    ProcessNitEventCache(nit);

    ProcessNitEventDb(nit);
}

/**
 * Process Nit table for cache storage
 *
 * @param nit Nit table
 */
void TDvbSiStorage::ProcessNitEventCache(const TNitTable& nit)
{
    std::lock_guard<std::mutex> lock(CacheDataMutex);

    auto it = NitTableMap.find(nit.GetNetworkId());
    if(it == NitTableMap.end())
    {
        OS_LOG(DVB_DEBUG,   "<%s> Adding NIT table to the map. Network id: 0x%x\n", __FUNCTION__, nit.GetNetworkId());
        NitTableMap.insert(std::make_pair(nit.GetNetworkId(), std::make_shared<TNitTable>(nit)));
    }
    else
    {
        OS_LOG(DVB_DEBUG,   "<%s> NIT already in cache. Network id: %d version: %d\n", __FUNCTION__,nit.GetNetworkId(),nit.GetVersionNumber());
        if(nit.GetVersionNumber() == it->second->GetVersionNumber())
        {
            OS_LOG(DVB_DEBUG,   "<%s> NIT version matches (%d). Skipping\n", __FUNCTION__, nit.GetVersionNumber());
        }
        else
        {
            OS_LOG(DVB_DEBUG,   "<%s> Current version: 0x%x, new version: 0x%x\n", __FUNCTION__, it->second->GetVersionNumber(), nit.GetVersionNumber());
            it->second = std::make_shared<TNitTable>(nit);
        }
    }

}

/**
 * Process Nit table for database storage
 *
 * @param nit Nit table
 */
void TDvbSiStorage::ProcessNitEventDb(const TNitTable& nit)
{
    int64_t network_fk = -1;
    int8_t nitVersion = -1;

    string cmdStr("SELECT nit_pk, version FROM Nit WHERE network_id = ");

    std::stringstream ss;
    ss << nit.GetNetworkId();
    cmdStr += ss.str();
    cmdStr += ";";

    int64_t nit_fk = StorageDb.FindPrimaryKey(cmdStr, nitVersion);
    if(nit_fk > 0)  // FOUND
    {
        if(nit.GetVersionNumber() == nitVersion)
        {
            OS_LOG(DVB_DEBUG,   "<%s> NIT version matches (%d). Skipping\n", __FUNCTION__, nit.GetVersionNumber());
        }
        else 
        {
            OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, nitVersion, nit.GetVersionNumber());

            StorageDb.DropTables();
            StorageDb.CreateTables();

            return;
        }
    }

    if(nit_fk < 1)  // NOT FOUND
    {
        OS_LOG(DVB_DEBUG,   "<%s> Adding NIT table to database. Network id: %d version: %d\n", __FUNCTION__, nit.GetNetworkId(), nit.GetVersionNumber());

        TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Nit (network_id, version) VALUES (?, ?);"));
        cmd.Bind(1, static_cast<int>(nit.GetNetworkId()));
        cmd.Bind(2, static_cast<int>(nit.GetVersionNumber()));
        cmd.Execute(nit_fk);

        OS_LOG(DVB_DEBUG,   "<%s> nit_fk %ld\n", __FUNCTION__, nit_fk);

        network_fk = ProcessNetwork(nit);
        OS_LOG(DVB_DEBUG,   "<%s> network_fk %ld\n", __FUNCTION__, network_fk);
    }

    if(nit_fk > 0)
    {
        StorageDb.InsertDescriptor(static_cast<const char*>("NitDescriptor"), nit_fk, nit.GetNetworkDescriptors());
    }

    const std::vector<TTransportStream>& tsList = nit.GetTransportStreams();
    for(auto it = tsList.begin(); it != tsList.end(); ++it)
    {
        string cmdStr("SELECT nit_transport_pk FROM NitTransport WHERE original_network_id = ");
        std::stringstream ss;
        ss << it->GetOriginalNetworkId();
        cmdStr += ss.str();
        cmdStr += " AND transport_id = ";

        ss.str("");
        ss << it->GetTsId();
        cmdStr += ss.str();
        cmdStr += ";";

        int64_t nit_transport_fk = StorageDb.FindPrimaryKey(cmdStr);
        if(nit_transport_fk < 1)   // NOT FOUND
        {
            TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO NitTransport (original_network_id, transport_id, nit_fk) VALUES (?, ?, ?);"));
            cmd.Bind(1, static_cast<int>(it->GetOriginalNetworkId()));
            cmd.Bind(2, static_cast<int>(it->GetTsId()));
            cmd.Bind(3, static_cast<int>(nit_fk));
            cmd.Execute(nit_transport_fk);

            OS_LOG(DVB_DEBUG,   "<%s> Insert nit_transport_fk %ld\n", __FUNCTION__, nit_transport_fk);

            if(network_fk > 0)
            {
                ProcessTransport(*it, network_fk);
            }
        }

        if(nit_transport_fk > 0)
        {
            StorageDb.InsertDescriptor(static_cast<const char*>("NitTransportDescriptor"), nit_transport_fk, 
                                           it->GetTsDescriptors());
        }
    }

    StorageDb.PerformUpdate();
}

/**
 * Handle Bat table
 *
 * @param bat Bat table
 */
void TDvbSiStorage::HandleBatEvent(const TBatTable& bat)
{
    ProcessBatEventCache(bat);

    ProcessBatEventDb(bat);
}

/**
 * Process Bat table for cache storage
 *
 * @param bat Bat table
 */
void TDvbSiStorage::ProcessBatEventCache(const TBatTable& bat)
{
    std::lock_guard<std::mutex> lock(CacheDataMutex);

    auto it = TBatTableMap.find(bat.GetBouquetId());
    if(it == TBatTableMap.end())
    {
        OS_LOG(DVB_DEBUG,   "<%s> Adding BAT table to the map. Bouquet id: 0x%x\n", __FUNCTION__, bat.GetBouquetId());
        TBatTableMap.insert(std::make_pair(bat.GetBouquetId(), std::make_shared<TBatTable>(bat)));
    }
    else
    {
        OS_LOG(DVB_DEBUG,   "<%s> BAT already in cache. Bouquet id: %d version: %d\n", __FUNCTION__, bat.GetBouquetId(),bat.GetVersionNumber());
        if(bat.GetVersionNumber() == it->second->GetVersionNumber())
        {
            OS_LOG(DVB_DEBUG,   "<%s> BAT version matches (%d). Skipping\n", __FUNCTION__, bat.GetVersionNumber());
        }
        else
        {
            OS_LOG(DVB_DEBUG,   "<%s> Current version: 0x%x, new version: 0x%x\n", __FUNCTION__, it->second->GetVersionNumber(), bat.GetVersionNumber());
            it->second = std::make_shared<TBatTable>(bat);
        }
    }
}

/**
 * Process Bat table for database storage
 *
 * @param bat Bat table
 */
void TDvbSiStorage::ProcessBatEventDb(const TBatTable& bat)
{
    int64_t network_fk = -1;

    if(PreferredNetworkId != 0)
    {
        string cmdStr("SELECT network_pk FROM Network WHERE network_id = ");

        std::stringstream ss;
        ss << PreferredNetworkId;
        cmdStr += ss.str();
        cmdStr += ";";

        network_fk = StorageDb.FindPrimaryKey(cmdStr);
        if(network_fk < 1)
        {
            OS_LOG(DVB_DEBUG,   "<%s> network_fk %ld\n", __FUNCTION__, network_fk);
            return;
        }  
    }

    string cmdStr("SELECT bat_pk, version FROM Bat WHERE bouquet_id = ");

    std::stringstream ss;
    ss << bat.GetBouquetId();
    cmdStr += ss.str();
    cmdStr += ";";

    int8_t batVersion = -1;
    int64_t bat_fk = StorageDb.FindPrimaryKey(cmdStr, batVersion);
    if(bat_fk > 0)    // FOUND
    {
        if(bat.GetVersionNumber() == batVersion)
        {
            OS_LOG(DVB_DEBUG,   "<%s> BAT version matches (%d). Skipping\n", __FUNCTION__, bat.GetVersionNumber());
        }
        else
        {
            OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, batVersion, bat.GetVersionNumber());

            // Bat version change
            TDvbDb::TTransaction versionChange(StorageDb);

            TDvbDb::TCommand cmd(StorageDb, string("DELETE FROM Bat WHERE bouquet_id = ? AND version != ?;"));
            cmd.Bind(1, static_cast<int>(bat.GetBouquetId()));
            cmd.Bind(2, static_cast<int>(bat.GetVersionNumber()));
            cmd.Execute();

            StorageDb.SqlCommand(string("DELETE FROM BatDescriptor WHERE fkey NOT IN (SELECT DISTINCT bat_pk FROM Bat);"));

            StorageDb.SqlCommand(string("DELETE FROM BatTransport WHERE bat_fk NOT IN (SELECT DISTINCT bat_pk FROM Bat);"));

            StorageDb.SqlCommand(string("DELETE FROM BatTransportDescriptor WHERE fkey NOT IN " \
                                   " (SELECT DISTINCT bat_transport_pk FROM BatTransport);"));

            versionChange.CommitSqlStatements();

            ProcessBouquet(bat);

            bat_fk = StorageDb.FindPrimaryKey(cmdStr, batVersion);
        }
    }

    if(bat_fk < 1)    // NOT FOUND
    {
        OS_LOG(DVB_DEBUG,   "<%s> Adding BAT table to the database. Bouquet id: %d version: %d\n", __FUNCTION__, bat.GetBouquetId(), bat.GetVersionNumber());

        TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Bat (bouquet_id, version) VALUES (?, ?);"));
        cmd.Bind(1, static_cast<int>(bat.GetBouquetId()));
        cmd.Bind(2, static_cast<int>(bat.GetVersionNumber()));
        cmd.Execute(bat_fk);

        OS_LOG(DVB_DEBUG,   "<%s> Insert bat_fk %ld\n", __FUNCTION__, bat_fk);

        ProcessBouquet(bat);
    }

    if(bat_fk > 0)
    {
        StorageDb.InsertDescriptor(static_cast<const char*>("BatDescriptor"), bat_fk, bat.GetBouquetDescriptors());
    }

    const std::vector<TTransportStream>& tsList = bat.GetTransportStreams();
    for(auto it = tsList.begin(); it != tsList.end(); ++it)
    {
        // NOTE: Order dependency of NitTransport entries existing before BatTransport entries.
        string cmdStr("SELECT nit_transport_pk FROM NitTransport WHERE original_network_id = ");
        std::stringstream ss;
        ss << it->GetOriginalNetworkId();
        cmdStr += ss.str();
        cmdStr += " AND transport_id = ";

        ss.str("");
        ss << it->GetTsId();
        cmdStr += ss.str();
        cmdStr += ";";

        int64_t nit_transport_fk = StorageDb.FindPrimaryKey(cmdStr);
        if(nit_transport_fk > 0)
        {
            string cmdStr("SELECT bat_transport_pk FROM BatTransport WHERE original_network_id = ");

            std::stringstream ss;
            ss << it->GetOriginalNetworkId();
            cmdStr += ss.str();
            cmdStr += " AND transport_id = ";

            ss.str("");
            ss << it->GetTsId();
            cmdStr += ss.str();
            cmdStr += ";";

            int64_t bat_transport_fk = StorageDb.FindPrimaryKey(cmdStr);
            if(bat_transport_fk < 1)  // NOT FOUND
            { 
                TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO BatTransport (original_network_id, transport_id, bat_fk, " \
                                                "nit_transport_fk) VALUES (?, ?, ?, ?);"));
                cmd.Bind(1, static_cast<int>(it->GetOriginalNetworkId()));
                cmd.Bind(2, static_cast<int>(it->GetTsId()));
                cmd.Bind(3, static_cast<long long int>(bat_fk));
                cmd.Bind(4, static_cast<long long int>(nit_transport_fk));
                cmd.Execute(bat_transport_fk);

                OS_LOG(DVB_DEBUG,   "<%s> Insert bat_transport_fk %ld\n", __FUNCTION__, bat_transport_fk);
            }

            if(bat_transport_fk > 0)
            {
                StorageDb.InsertDescriptor(static_cast<const char*>("BatTransportDescriptor"), bat_transport_fk, it->GetTsDescriptors());
            }
        }
    }

    StorageDb.PerformUpdate();
}

/**
 * Handle Sdt table
 *
 * @param sdt Sdt table
 */
void TDvbSiStorage::HandleSdtEvent(const TSdtTable& sdt)
{
    ProcessSdtEventCache(sdt);

    ProcessSdtEventDb(sdt);
}

/**
 * Process Sdt table for cache storage
 *
 * @param sdt Sdt table
 */
void TDvbSiStorage::ProcessSdtEventCache(const TSdtTable& sdt)
{
    std::lock_guard<std::mutex> lock(CacheDataMutex);

    pair<uint16_t, uint16_t> key(sdt.GetOriginalNetworkId(), sdt.GetTableExtensionId());
    auto it = SdtTableMap.find(key);
    if(it == SdtTableMap.end())
    {
        OS_LOG(DVB_DEBUG,   "<%s> Adding SDT table to the cache. nid.tsid: 0x%x.0x%x\n", __FUNCTION__, sdt.GetOriginalNetworkId(), sdt.GetTableExtensionId());
        SdtTableMap.insert(std::make_pair(key, std::make_shared<TSdtTable>(sdt)));
    }
    else
    {
        OS_LOG(DVB_DEBUG,   "<%s> SDT already in cache. nid.tsid: %d.%d\n", __FUNCTION__, sdt.GetOriginalNetworkId(), sdt.GetTableExtensionId());
        if(sdt.GetVersionNumber() == it->second->GetVersionNumber())
        {
            OS_LOG(DVB_DEBUG,   "<%s> SDT version matches (0x%x). Skipping\n", __FUNCTION__, sdt.GetVersionNumber());
        }
        else
        {
            OS_LOG(DVB_DEBUG,   "<%s> Current version: 0x%x, new version: 0x%x\n", __FUNCTION__, it->second->GetVersionNumber(), sdt.GetVersionNumber());
            it->second = std::make_shared<TSdtTable>(sdt);
        }
    }
}

/**
 * Process Sdt table for database storage
 *
 * @param sdt Sdt table
 */
void TDvbSiStorage::ProcessSdtEventDb(const TSdtTable& sdt)
{

    string cmdStr("SELECT nit_transport_pk FROM NitTransport WHERE original_network_id = ");

    std::stringstream ss;
    ss << sdt.GetOriginalNetworkId();
    cmdStr += ss.str();
    cmdStr += " AND transport_id = ";

    ss.str("");
    ss << sdt.GetTableExtensionId();
    cmdStr += ss.str();
    cmdStr += ";";

    int64_t nit_transport_fk = StorageDb.FindPrimaryKey(cmdStr);
    if(nit_transport_fk < 1)
    {
        OS_LOG(DVB_DEBUG,   "<%s> nit_transport_fk %ld\n", __FUNCTION__, nit_transport_fk);
        return;
    }

    const std::vector<TSdtService>&  serviceList = sdt.GetServices();
    for(auto it = serviceList.begin(); it != serviceList.end(); ++it)
    {
        const TSdtService& service = (*it);

        string cmdStr("SELECT s.sdt_pk, version FROM Sdt s INNER JOIN NitTransport nt " \
                      " ON s.nit_transport_fk = nt.nit_transport_pk "                   \
                      "WHERE nt.original_network_id = ");

        std::stringstream ss;
        ss << sdt.GetOriginalNetworkId();
        cmdStr += ss.str();
        cmdStr += " AND nt.transport_id = ";

        ss.str("");
        ss << sdt.GetTableExtensionId();
        cmdStr += ss.str();
        cmdStr += " AND s.service_id = ";

        ss.str("");
        ss << service.GetServiceId();
        cmdStr += ss.str();
        cmdStr += ";";

        int8_t sdtVersion = -1; 
        int64_t sdt_fk = StorageDb.FindPrimaryKey(cmdStr, sdtVersion);
        if(sdt_fk > 0)   // FOUND 
        {
            if(sdt.GetVersionNumber() == sdtVersion)
            {
                OS_LOG(DVB_DEBUG,   "<%s> Sdt version matches (%d). Skipping\n", __FUNCTION__, sdt.GetVersionNumber());
            }
            else
            {
                OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, sdtVersion, sdt.GetVersionNumber());

                // Sdt version change
                TDvbDb::TTransaction versionChange(StorageDb);

                {
                    TDvbDb::TCommand cmd(StorageDb, string("DELETE FROM Sdt WHERE service_id = ? AND version != ? AND "  \
                                                    "nit_transport_fk IN (SELECT nt.nit_transport_pk FROM Sdt s " \
                                                    "INNER JOIN NitTransport nt "                                 \
                                                    " ON s.nit_transport_fk = nt.nit_transport_pk "               \
                                                    "WHERE nt.original_network_id = ? AND nt.transport_id = ? "   \
                                                    " AND s.service_id = ?);"));
                    cmd.Bind(1, static_cast<int>(service.GetServiceId()));
                    cmd.Bind(2, static_cast<int>(sdt.GetVersionNumber()));
                    cmd.Bind(3, static_cast<int>(sdt.GetOriginalNetworkId()));
                    cmd.Bind(4, static_cast<int>(sdt.GetTableExtensionId()));
                    cmd.Bind(5, static_cast<int>(service.GetServiceId()));
                    cmd.Execute();
                }

                StorageDb.SqlCommand(string("DELETE FROM SdtDescriptor WHERE fkey NOT IN (SELECT DISTINCT sdt_pk FROM Sdt);"));

                {
                    TDvbDb::TCommand cmd(StorageDb, string("DELETE FROM Eit WHERE network_id = ? AND transport_id = ? AND service_id = ?;"));
                    cmd.Bind(1, static_cast<int>(sdt.GetOriginalNetworkId()));
                    cmd.Bind(2, static_cast<int>(sdt.GetTableExtensionId()));
                    cmd.Bind(3, static_cast<int>(service.GetServiceId()));
                    cmd.Execute();
                }

                StorageDb.SqlCommand(string("DELETE FROM EitDescriptor WHERE fkey NOT IN (SELECT DISTINCT eit_pk FROM Eit);"));

                versionChange.CommitSqlStatements();

                sdt_fk = StorageDb.FindPrimaryKey(cmdStr, sdtVersion);

                ProcessService(sdt);
             }
        }

        if(sdt_fk < 1)  // NOT FOUND
        {
            OS_LOG(DVB_DEBUG,   "<%s> Adding SDT table to database. nid.tsid: %d.%d\n", __FUNCTION__, sdt.GetOriginalNetworkId(), sdt.GetTableExtensionId());

            TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Sdt (service_id, nit_transport_fk, version, schedule, " \
                                            " present_following, scrambled, running) VALUES (?, ?, ?, ?, ?, ?, ?);"));
            cmd.Bind(1, static_cast<int>(service.GetServiceId()));
            cmd.Bind(2, static_cast<int>(nit_transport_fk));
            cmd.Bind(3, static_cast<int>(sdt.GetVersionNumber()));
            cmd.Bind(4, static_cast<int>(service.IsEitSchedFlagSet()));
            cmd.Bind(5, static_cast<int>(service.IsEitPfFlagSet()));
            cmd.Bind(6, static_cast<int>(service.IsScrambled()));
            cmd.Bind(7, static_cast<int>(service.GetRunningStatus()));
            cmd.Execute(sdt_fk);
            
            OS_LOG(DVB_DEBUG,   "<%s> Insert sdt_fk = %ld\n", __FUNCTION__, sdt_fk);

            ProcessService(sdt);
        }

        if(sdt_fk > 0)
        {
            StorageDb.InsertDescriptor(static_cast<const char*>("SdtDescriptor"), sdt_fk, service.GetServiceDescriptors());
        }

    } // for

    StorageDb.PerformUpdate();
}

/**
 * Handle Eit table
 *
 * @param eit Eit table
 */
void TDvbSiStorage::HandleEitEvent(const TEitTable& eit)
{
    ProcessEitEventCache(eit);

    ProcessEitEventDb(eit);
}

/**
 * Process Eit table for cache storage
 *
 * @param eit Eit table
 */
void TDvbSiStorage::ProcessEitEventCache(const TEitTable& eit)
{
    std::lock_guard<std::mutex> lock(CacheDataMutex);

    bool isPf = false;
    TTableId tableId = eit.GetTableId();
    if(tableId == TTableId::TABLE_ID_EIT_PF || tableId == TTableId::TABLE_ID_EIT_PF_OTHER)
    {
        isPf = true;
    }

    tuple<uint16_t, uint16_t, uint16_t, bool> key(eit.GetNetworkId(), eit.GetTsId(), eit.GetTableExtensionId(), isPf);
    auto it = TEitTableMap.find(key);
    if(it == TEitTableMap.end())
    {
        OS_LOG(DVB_DEBUG,   "<%s> Adding EIT table to the cache. nid.tsid.sid: 0x%x.0x%x.0x%x\n",
                __FUNCTION__, eit.GetNetworkId(), eit.GetTsId(), eit.GetTableExtensionId());

        TEitTableMap.insert(std::make_pair(key, std::make_shared<TEitTable>(eit)));
    }
    else
    {
        OS_LOG(DVB_DEBUG,   "<%s> EIT already in cache. nid.tsid.sid: 0x%x.0x%x.0x%x\n", __FUNCTION__, eit.GetNetworkId(), eit.GetTsId(), eit.GetTableExtensionId());
        if(eit.GetVersionNumber() == it->second->GetVersionNumber())
        {
            OS_LOG(DVB_DEBUG,   "<%s> EIT version matches (0x%x). Skipping\n", __FUNCTION__, eit.GetVersionNumber());
        }
        else
        {
            OS_LOG(DVB_DEBUG,   "<%s> Current version: 0x%x, new version: 0x%x\n", __FUNCTION__, it->second->GetVersionNumber(), eit.GetVersionNumber());
            it->second = std::make_shared<TEitTable>(eit);
        }
    }
}

/**
 * Process Eit table for database storage
 *
 * @param eit Eit table
 */
void TDvbSiStorage::ProcessEitEventDb(const TEitTable& eit)
{

    const std::vector<TEitEvent>&  eventList = eit.GetEvents();

    for(auto it = eventList.begin(); it != eventList.end(); ++it)
    {
        const TEitEvent& event = (*it);

        string cmdStr("SELECT eit_pk, version FROM Eit WHERE network_id = ");
        std::stringstream ss;
        ss << eit.GetNetworkId();
        cmdStr += ss.str();
        cmdStr += " AND transport_id = ";

        ss.str("");
        ss << eit.GetTsId();
        cmdStr += ss.str();
        cmdStr += " AND service_id = ";

        ss.str("");
        ss << eit.GetTableExtensionId();
        cmdStr += ss.str();
        cmdStr += " AND event_id = ";

        ss.str("");
        ss << event.GetEventId();
        cmdStr += ss.str();
        cmdStr += ";";

        int8_t eitVersion = -1;
        int64_t eit_fk = StorageDb.FindPrimaryKey(cmdStr, eitVersion);
        if(eit_fk > 0)  // FOUND
        {
            if(eit.GetVersionNumber() == eitVersion)
            {
                OS_LOG(DVB_DEBUG,   "<%s> EIT version matches (%d). Skipping\n", __FUNCTION__, eit.GetVersionNumber());
            }
            else
            {
                OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, eitVersion, eit.GetVersionNumber());
                // Eit version change
                TDvbDb::TTransaction versionChange(StorageDb);

                TDvbDb::TCommand cmd(StorageDb, string("DELETE FROM Eit WHERE network_id = ? AND transport_id = ? AND " \
                                                "event_id = ? AND version != ?;"));
                cmd.Bind(1, static_cast<int>(eit.GetNetworkId()));
                cmd.Bind(2, static_cast<int>(eit.GetTsId()));
                cmd.Bind(3, static_cast<int>(eit.GetTableExtensionId()));
                cmd.Bind(4, static_cast<int>(eit.GetVersionNumber()));
                cmd.Execute();

                StorageDb.SqlCommand(string("DELETE FROM EitDescriptor WHERE fkey NOT IN (SELECT DISTINCT eit_pk FROM Eit);"));

                versionChange.CommitSqlStatements();

                eit_fk = StorageDb.FindPrimaryKey(cmdStr, eitVersion);

            }
        }

        if(eit_fk < 1)  // NOT FOUND
        {
            OS_LOG(DVB_DEBUG,   "<%s> Adding EIT table to the database. nid.tsid.sid: 0x%x.0x%x.0x%x\n", __FUNCTION__, eit.GetNetworkId(), eit.GetTsId(), eit.GetTableExtensionId());

            int present_following = 0;
            TTableId tableId = eit.GetTableId();
            if(tableId == TTableId::TABLE_ID_EIT_PF || tableId == TTableId::TABLE_ID_EIT_PF_OTHER)
            {
                present_following = 1;
            }

            TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Eit (network_id, transport_id, service_id, event_id, "   \
                                            "version, present_following, start_time, duration, scrambled, running) VALUES " \
                                            " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"));
            cmd.Bind(1, static_cast<int>(eit.GetNetworkId()));
            cmd.Bind(2, static_cast<int>(eit.GetTsId()));
            cmd.Bind(3, static_cast<int>(eit.GetTableExtensionId()));
            cmd.Bind(4, static_cast<int>(event.GetEventId()));
            cmd.Bind(5, static_cast<int>(eit.GetVersionNumber()));
            cmd.Bind(6, static_cast<int>(present_following));
            cmd.Bind(7, static_cast<long long int>(event.GetStartTime()));
            cmd.Bind(8, static_cast<int>(event.GetDuration()));
            cmd.Bind(9, static_cast<int>(event.IsScrambled()));
            cmd.Bind(10, static_cast<int>(event.GetRunningStatus()));
            cmd.Execute(eit_fk);

            OS_LOG(DVB_DEBUG,   "<%s> Insert eit_fk %ld\n", __FUNCTION__, eit_fk);
        }

        if(eit_fk > 0)
        {
            StorageDb.InsertDescriptor(static_cast<const char*>("EitDescriptor"), eit_fk, it->GetEventDescriptors());
        }
    } 

    // Process eit data for Event Db Table
    ProcessEvent(eit);
}

/**
 * Process Nit table for parsed database storage
 *
 * @param nit Nit table
 * @return foreign key
 */
int64_t  TDvbSiStorage::ProcessNetwork(const TNitTable& nit)
{
    int64_t network_fk = -1;
    string networkName;
    string iso639languageCode;

    const vector<TMpegDescriptor>& descList = nit.GetNetworkDescriptors();

    for(auto it = descList.cbegin(); it != descList.cend(); ++it)
    {
        const TMpegDescriptor& md = *it;
        if(md.GetDescriptorTag() == TDescriptorTag::NETWORK_NAME_TAG)
        {
            TNetworkNameDescriptor nnd(md);
            networkName = nnd.GetName();
            break;
        }

        if(md.GetDescriptorTag() == TDescriptorTag::MULTILINGUAL_NETWORK_NAME_TAG)
        {
            TMultilingualNetworkNameDescriptor  mnnd(md);
            for(int32_t i=0; i < mnnd.GetCount(); i++)
            {
                iso639languageCode += mnnd.GetLanguageCode(i);
                iso639languageCode += " ";
                networkName += mnnd.GetNetworkName(i);
                networkName += " ";
            }
        }
    }

    TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Network (network_id, version, iso_639_language_code, name) " \
                                    " VALUES (?, ?, ?, ?);"));
    cmd.Bind(1, static_cast<int>(nit.GetNetworkId()));
    cmd.Bind(2, static_cast<int>(nit.GetVersionNumber()));

    if(iso639languageCode.empty())
    {  
        cmd.Bind(3); // Db NULL type
    }
    else
    {
        cmd.Bind(3, iso639languageCode);
    }

    if(networkName.empty())
    {
        cmd.Bind(4);
    }
    else
    {
        cmd.Bind(4, networkName);
    }

    cmd.Execute(network_fk);

    OS_LOG(DVB_DEBUG,   "<%s> Insert network_fk %ld\n", __FUNCTION__, network_fk);

    return network_fk;
}

/**
 * Process Bat table for parsed database storage
 *
 * @param bat Bat table
 * @return foreign key
 */
int64_t  TDvbSiStorage::ProcessBouquet(const TBatTable& bat)
{

    string cmdStr("SELECT bouquet_pk, version FROM Bouquet WHERE bouquet_id = ");

    std::stringstream ss;
    ss << bat.GetBouquetId();
    cmdStr += ss.str();
    cmdStr += ";";

    int8_t bouquetVersion = -1;
    int64_t bouquet_fk = StorageDb.FindPrimaryKey(cmdStr, bouquetVersion);
    if(bouquet_fk > 0)  // FOUND
    {
        if(bat.GetVersionNumber() == bouquetVersion)
        {
            OS_LOG(DVB_DEBUG,   "<%s> BAT version matches (%d). Skipping\n", __FUNCTION__, bat.GetVersionNumber());
        }
        else
        {
            OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, bouquetVersion, bat.GetVersionNumber());

            // Bat version change
            TDvbDb::TTransaction versionChange(StorageDb);

            TDvbDb::TCommand cmd(StorageDb, string("DELETE FROM Bouquet WHERE bouquet_id = ? AND version != ?;"));
            cmd.Bind(1, static_cast<int>(bat.GetVersionNumber()));
            cmd.Bind(2, static_cast<int>(bat.GetVersionNumber())); 
            cmd.Execute();

            versionChange.CommitSqlStatements();

            bouquet_fk = StorageDb.FindPrimaryKey(cmdStr, bouquetVersion);
        }
    }

    if(bouquet_fk < 1)  // NOT FOUND
    {
        string networkName;
        string iso639languageCode;

        // Either a Network Name descriptor or a Multilingual Network descriptor exists. 
        const vector<TMpegDescriptor>& descList = bat.GetBouquetDescriptors();

        for(auto it = descList.cbegin(); it != descList.cend(); ++it)
        {
            const TMpegDescriptor& md = *it;
            if(md.GetDescriptorTag() == TDescriptorTag::NETWORK_NAME_TAG)
            {
                TNetworkNameDescriptor nnd(md);
                networkName += nnd.GetName();
            }

            if(md.GetDescriptorTag() == TDescriptorTag::MULTILINGUAL_NETWORK_NAME_TAG)
            {
                TMultilingualNetworkNameDescriptor  mnnd(md);
                for(int32_t i=0; i < mnnd.GetCount(); i++)
                {
                    iso639languageCode += mnnd.GetLanguageCode(i);
                    iso639languageCode += " ";
                    networkName += mnnd.GetNetworkName(i);
                    networkName += " ";
                }
            }
        }

        OS_LOG(DVB_DEBUG,   "<%s> bouquet_id: %d version: %d networkName: %s iso: %s\n", __FUNCTION__, bat.GetBouquetId(), bat.GetVersionNumber(), networkName.c_str(), iso639languageCode.c_str());

        TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Bouquet (bouquet_id, version, iso_639_language_code, name) " \
                                     " VALUES (?, ?, ?, ?);"));
        cmd.Bind(1, static_cast<int>(bat.GetBouquetId()));
        cmd.Bind(2, static_cast<int>(bat.GetVersionNumber()));

        if(iso639languageCode.empty())
        {  
            cmd.Bind(3);
        }
        else
        {
            cmd.Bind(3, iso639languageCode);
        }

        if(networkName.empty())
        {
            cmd.Bind(4);
        }
        else
        {
            cmd.Bind(4, networkName);
        }

        cmd.Execute(bouquet_fk);

        OS_LOG(DVB_DEBUG,   "<%s> Insert bouquet_fk %ld\n", __FUNCTION__, bouquet_fk);

        // Set bouquet_fk foreign key in Transport.
        if(bouquet_fk > 0)
        {
            const std::vector<TTransportStream>& tsList = bat.GetTransportStreams();
            for(auto it = tsList.begin(); it != tsList.end(); ++it)
            {
                string cmdStr("SELECT transport_pk FROM Transport WHERE original_network_id = ");

                std::stringstream ss;
                ss << it->GetOriginalNetworkId();
                cmdStr += ss.str();
                cmdStr += " AND transport_id = ";

                ss.str("");
                ss << it->GetTsId();
                cmdStr += ss.str();
                cmdStr += ";";

                int64_t transport_fk = StorageDb.FindPrimaryKey(cmdStr);
                if(transport_fk > 0)
                {
                    TDvbDb::TCommand cmd(StorageDb, string("UPDATE Transport SET bouquet_fk = ? WHERE transport_pk = ?;"));
                    cmd.Bind(1, static_cast<long long int>(bouquet_fk));
                    cmd.Bind(2, static_cast<long long int>(transport_fk));
                    cmd.Execute();

                    if(StorageDb.GetNumberOfRowsModified() == 0) // no change schedule an update 
                    {
                        string cmdStr("UPDATE Transport SET bouquet_fk = ");

                        std::stringstream ss;
                        ss << bouquet_fk;
                        cmdStr += ss.str();
                        cmdStr += " WHERE transport_pk = ";

                        ss.str("");
                        ss << transport_fk;
                        cmdStr += ss.str();
                        cmdStr += ";";

                        StorageDb.AddUpdate(cmdStr.c_str());
                    }
                }
            }
        }
    }

    return bouquet_fk;
}

/**
 * Process Transport table for parsed database storage
 *
 * @param ts TransportStream reference
 * @param network_fk foreign key value of Network entry
 * @return foreign key
 */
int64_t  TDvbSiStorage::ProcessTransport(const TTransportStream& ts, int64_t network_fk)
{
    int64_t transport_fk = -1;
    uint8_t modulation = 0;
    uint32_t frequency = 0;
    uint32_t symbolRate = 0;
    uint8_t  fecInner = 0;
    uint8_t  fecOuter = 0;

    const vector<TMpegDescriptor>& tsDesc = ts.GetTsDescriptors();

    for(auto it = tsDesc.cbegin(); it != tsDesc.cend(); ++it)
    {
        const TMpegDescriptor& md = *it;
        if(md.GetDescriptorTag() == TDescriptorTag::CABLE_DELIVERY_TAG)
        {
            TCableDeliverySystemDescriptor cable(md);
            frequency = cable.GetFrequency();
            modulation = static_cast<uint8_t>(cable.GetModulation());
            symbolRate = cable.GetSymbolRate();
            fecInner = cable.GetFecInner();
            fecOuter = cable.GetFecOuter();
        }
    }
    TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Transport (original_network_id, transport_id, network_fk, " \
                                    "frequency, modulation, symbol_rate, fec_outer, fec_inner) VALUES "                \
                                    "(?, ?, ?, ?, ?, ?, ?, ?);"));
    cmd.Bind(1, static_cast<int>(ts.GetOriginalNetworkId()));
    cmd.Bind(2, static_cast<int>(ts.GetTsId()));
    cmd.Bind(3, static_cast<long long int>(network_fk));
    cmd.Bind(4, static_cast<int>(frequency));
    cmd.Bind(5, static_cast<int>(modulation));
    cmd.Bind(6, static_cast<int>(symbolRate));
    cmd.Bind(7, static_cast<int>(fecOuter));
    cmd.Bind(8, static_cast<int>(fecInner));
    cmd.Execute(transport_fk);

    OS_LOG(DVB_DEBUG,   "<%s> Insert transport_fk %ld\n", __FUNCTION__, transport_fk);

    return  transport_fk;
}

/**
 * Process Service table for parsed database storage
 *
 * @param sdt Sdt Table
 * @return foreign key
 */
int64_t  TDvbSiStorage::ProcessService(const TSdtTable& sdt)
{
    int64_t service_fk = -1;

    string cmdStr("SELECT transport_pk FROM Transport WHERE original_network_id = ");

    std::stringstream ss;
    ss << sdt.GetOriginalNetworkId();
    cmdStr += ss.str();
    cmdStr += " AND transport_id = ";

    ss.str("");
    ss << sdt.GetTableExtensionId();
    cmdStr += ss.str();
    cmdStr += ";";

    int64_t transport_fk = StorageDb.FindPrimaryKey(cmdStr);
    if(transport_fk < 1)
    {
        OS_LOG(DVB_DEBUG,   "<%s> transport_fk = %ld\n", __FUNCTION__, transport_fk);
        return service_fk;
    }

    const std::vector<TSdtService>&  serviceList = sdt.GetServices();
    for(auto it = serviceList.begin(); it != serviceList.end(); ++it)
    {
        const TSdtService& service = (*it);
        string cmdStr("SELECT s.service_pk, s.version FROM Service s INNER JOIN Transport t " \
                      " ON s.transport_fk = t.transport_pk "                                  \
                      "WHERE t.original_network_id = ");

        std::stringstream ss;
        ss << sdt.GetOriginalNetworkId();
        cmdStr += ss.str();
        cmdStr += " AND t.transport_id = ";

        ss.str("");
        ss << sdt.GetTableExtensionId();
        cmdStr += ss.str();
        cmdStr += " AND s.service_id = ";

        ss.str("");
        ss << service.GetServiceId();
        cmdStr += ss.str();
        cmdStr += ";";

        int8_t serviceVersion = -1; 
        int64_t service_fk = StorageDb.FindPrimaryKey(cmdStr, serviceVersion);
        if(service_fk > 0)  // FOUND
        {
            if(sdt.GetVersionNumber() == serviceVersion) 
            {
                OS_LOG(DVB_DEBUG,   "<%s> Sdt version matches (%d). Skipping\n", __FUNCTION__, sdt.GetVersionNumber());
            }
            else
            {
                OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, serviceVersion, sdt.GetVersionNumber());

                // Sdt version change
                TDvbDb::TTransaction versionChange(StorageDb);

                {
                    TDvbDb::TCommand cmd(StorageDb, string("DELETE FROM Service WHERE service_id = ? AND version != ? AND " \
                                                    "transport_fk IN (SELECT t.transport_pk FROM Service s "         \
                                                    "INNER JOIN Transport t "                                        \
                                                    " ON s.transport_fk = t.transport_pk "                           \
                                                    "WHERE t.original_network_id = ? AND t.transport_id = ? AND "    \
                                                    " s.service_id = ?);"));
                    cmd.Bind(1, static_cast<int>(service.GetServiceId()));
                    cmd.Bind(2, static_cast<int>(sdt.GetVersionNumber()));
                    cmd.Bind(3, static_cast<int>(sdt.GetOriginalNetworkId()));
                    cmd.Bind(4, static_cast<int>(sdt.GetTableExtensionId()));
                    cmd.Bind(5, static_cast<int>(service.GetServiceId()));
                    cmd.Execute();
                }

                StorageDb.SqlCommand(string("DELETE FROM ServiceComponent WHERE fkey NOT IN (SELECT DISTINCT service_pk FROM Service);"));

                {
                    TDvbDb::TCommand cmd(StorageDb, string("DELETE FROM Event WHERE network_id = ? AND transport_id = ? AND service_id = ?;"));
                    cmd.Bind(1, static_cast<int>(sdt.GetOriginalNetworkId()));
                    cmd.Bind(2, static_cast<int>(sdt.GetTableExtensionId()));
                    cmd.Bind(3, static_cast<int>(service.GetServiceId()));
                    cmd.Execute();
                }

                StorageDb.SqlCommand(string("DELETE FROM EventItem WHERE event_fk NOT IN (SELECT DISTINCT event_pk FROM Event);"));

                StorageDb.SqlCommand(string("DELETE FROM EventComponent WHERE fkey NOT IN (SELECT DISTINCT event_pk FROM Event);"));

                versionChange.CommitSqlStatements();

                service_fk = StorageDb.FindPrimaryKey(cmdStr, serviceVersion);
            }
        }

        if(service_fk < 1)  // NOT found
        {
            uint8_t serviceType = 0;
            uint16_t lcn = 0;
            string serviceName;
            string providerName;

            const vector<TMpegDescriptor>& serveDesc = service.GetServiceDescriptors();

            for(auto it = serveDesc.cbegin(); it != serveDesc.cend(); ++it)
            {
                const TMpegDescriptor& md = *it;
                if(md.GetDescriptorTag() == TDescriptorTag::LOGICAL_CHANNEL_TAG)
                {
                    TLogicalChannelDescriptor lcd(md);
                    lcn = lcd.GetLogicalChannelNumber(0);
                }
                if(md.GetDescriptorTag() == TDescriptorTag::SERVICE_TAG)
                {
                    TServiceDescriptor sd(md);
                    serviceType = sd.GetServiceType();
                    serviceName = sd.GetServiceName();
                    providerName = sd.GetServiceProviderName();
                }
            }

            TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Service (service_id, transport_fk, version, "     \
                                            "service_type, logical_channel_number, running, scrambled, schedule, "   \
                                            "present_following, iso_639_language_code, service_name, provider_name)" \
                                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"));
            cmd.Bind(1, static_cast<int>(service.GetServiceId()));

            if(transport_fk > 0)
            {
                cmd.Bind(2, static_cast<long long int>(transport_fk));
            }
            else
            {
                cmd.Bind(2);
            }
            
            cmd.Bind(3, static_cast<int>(sdt.GetVersionNumber())); 
            cmd.Bind(4, static_cast<int>(serviceType));

            if(lcn > 0)
            { 
                cmd.Bind(5, static_cast<int>(lcn));
            }
            else
            {
                cmd.Bind(5);
            }

            cmd.Bind(6, static_cast<int>(service.GetRunningStatus())); 
            cmd.Bind(7, static_cast<int>(service.IsScrambled()));
            cmd.Bind(8, static_cast<int>(service.IsEitSchedFlagSet()));
            cmd.Bind(9, static_cast<int>(service.IsEitPfFlagSet()));
            cmd.Bind(10);

            if(serviceName.empty())
            {
                cmd.Bind(11);
            }
            else
            {
                cmd.Bind(11, serviceName);
            }

            if(providerName.empty())
            {
                cmd.Bind(12);
            }
            else
            {
                cmd.Bind(12, providerName);
            }

            cmd.Execute(service_fk);

            OS_LOG(DVB_DEBUG,   "<%s> Insert service_fk = %ld\n", __FUNCTION__, service_fk);
        }

        if(service_fk > 0)
        {
            StorageDb.InsertComponent("Service", service_fk, service.GetServiceDescriptors());
        }
    } // for

    return  service_fk;
}

/**
 * Process Event table for parsed database storage
 *
 * @param eit Eit Table
 * @return foreign key
 */
int64_t  TDvbSiStorage::ProcessEvent(const TEitTable& eit)
{
    int64_t event_fk = -1;
    const std::vector<TEitEvent>&  eventList = eit.GetEvents();

    for(auto it = eventList.begin(); it != eventList.end(); ++it)
    {
        const TEitEvent& event = (*it);
        string cmdStr("SELECT event_pk, version FROM Event WHERE network_id = ");

        std::stringstream ss;
        ss << eit.GetNetworkId();
        cmdStr += ss.str();
        cmdStr += " AND transport_id = ";

        ss.str("");
        ss << eit.GetTsId();
        cmdStr += ss.str();
        cmdStr += " AND service_id = ";

        ss.str("");
        ss << eit.GetTableExtensionId();
        cmdStr += ss.str();
        cmdStr += " AND event_id = ";

        ss.str("");
        ss << event.GetEventId();
        cmdStr += ss.str();
        cmdStr += ";";

        int8_t eventVersion = -1;
        event_fk = StorageDb.FindPrimaryKey(cmdStr, eventVersion);
        if(event_fk > 0)  // FOUND
        {
            if(eit.GetVersionNumber() == eventVersion)
            {
                OS_LOG(DVB_DEBUG,   "<%s> EIT version matches (%d). Skipping\n", __FUNCTION__, eit.GetVersionNumber());
            }
            else
            {
                OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, eventVersion, eit.GetVersionNumber());

                // Event version change
                TDvbDb::TTransaction versionChange(StorageDb);

                TDvbDb::TCommand cmd(StorageDb, string("DELETE FROM Event WHERE network_id = ? AND transport_id = ? AND " \
                                                "event_id = ? AND version != ?;"));
                cmd.Bind(1, static_cast<int>(eit.GetNetworkId()));
                cmd.Bind(2, static_cast<int>(eit.GetTsId()));
                cmd.Bind(3, static_cast<int>(eit.GetTableExtensionId()));
                cmd.Bind(4, static_cast<int>(eit.GetVersionNumber()));
                cmd.Execute();

                StorageDb.SqlCommand(string("DELETE FROM EventItem WHERE event_fk NOT IN (SELECT DISTINCT event_pk FROM Event);"));

                StorageDb.SqlCommand(string("DELETE FROM EventComponent WHERE fkey NOT IN (SELECT DISTINCT event_pk FROM Event);"));

                versionChange.CommitSqlStatements();

                event_fk = StorageDb.FindPrimaryKey(cmdStr, eventVersion);
            }
        }

        if(event_fk < 1)  // NOT FOUND
        {
            string parentalRating;
            string content;

            const vector<TMpegDescriptor>& eventDesc = event.GetEventDescriptors();

            for(auto it = eventDesc.cbegin(); it != eventDesc.cend(); ++it)
            {
                const TMpegDescriptor& md = *it;
                if(md.GetDescriptorTag() == TDescriptorTag::CONTENT_DESCRIPTOR_TAG)
                {
                    TContentDescriptor cd(md);
                    for(int i=0; i < cd.GetCount(); i++)
                    {
                        string conStr("(");
                        std::stringstream ss;
                        ss << static_cast<int>(cd.GetNibbleLvl1(i));
                        content += ss.str(); 

                        ss << static_cast<int>(cd.GetNibbleLvl2(i));
                        content += ss.str(); 

                        ss << static_cast<int>(cd.GetUserByte(i));
                        content += ss.str(); 
                        content += ")";
                    }
                }
                if(md.GetDescriptorTag() == TDescriptorTag::PARENTAL_RATING_TAG)
                {
                    TParentalRatingDescriptor prd(md);
                    for(int i=0; i < prd.GetCount(); i++)
                    {
                        parentalRating += "(";
                        parentalRating += prd.GetCountryCode(i);
                        parentalRating += " ";

                        std::stringstream ss;
                        ss << static_cast<int>(prd.GetRating(i));
                        parentalRating += ss.str();
                        parentalRating += ")";
                    }
                }
            }

            int present_following = 0;
            TTableId tableId = eit.GetTableId();
            if(tableId == TTableId::TABLE_ID_EIT_PF || tableId == TTableId::TABLE_ID_EIT_PF_OTHER)
            {
                present_following = 1;
            }

            TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO Event (network_id, transport_id, service_id, event_id, " \
                                            "version, present_following, start_time, duration, scrambled, running, "        \
                                            "parental_rating, content) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"));
            cmd.Bind(1, static_cast<int>(eit.GetNetworkId()));
            cmd.Bind(2, static_cast<int>(eit.GetTsId()));
            cmd.Bind(3, static_cast<int>(eit.GetTableExtensionId()));
            cmd.Bind(4, static_cast<int>(event.GetEventId()));
            cmd.Bind(5, static_cast<int>(eit.GetVersionNumber()));
            cmd.Bind(6, static_cast<int>(present_following));
            cmd.Bind(7, static_cast<long long int>(event.GetStartTime()));
            cmd.Bind(8, static_cast<int>(event.GetDuration()));
            cmd.Bind(9, static_cast<int>(event.IsScrambled()));
            cmd.Bind(10, static_cast<int>(event.GetRunningStatus()));

            if(parentalRating.empty())
            {
                cmd.Bind(11);
            }
            else
            {
                cmd.Bind(11, parentalRating);
            }

            if(content.empty())
            {
                cmd.Bind(12);
            }
            else
            {
                cmd.Bind(12, content);
            }
            cmd.Execute(event_fk);

            OS_LOG(DVB_DEBUG,   "<%s> Insert event_fk = %ld\n", __FUNCTION__, event_fk);

            if(event_fk > 0)
            {
                ProcessEventItem(event.GetEventDescriptors(), event_fk);

                StorageDb.InsertComponent("Event", event_fk, event.GetEventDescriptors());
            }
        }
    }

    return  event_fk;
}

/**
 * Process Event Items for parsed database storage
 *
 * @param descList vector of eit descriptors
 * @param event_fk foreign key to event
 * @return foreign key
 */
int64_t  TDvbSiStorage::ProcessEventItem(const vector<TMpegDescriptor>& descList, int64_t event_fk)
{
    int64_t eventItem_fk = -1;
    string iso_639_language_code;
    string title;
    string description;

    for(auto it = descList.cbegin(); it != descList.cend(); ++it)
    {
        const TMpegDescriptor& md = *it;
        if(md.GetDescriptorTag() == TDescriptorTag::SHORT_EVENT_TAG)
        {
            TShortEventDescriptor sed(md);

            iso_639_language_code = sed.GetLanguageCode();
            title = sed.GetEventName();
            description = sed.GetText();

            TDvbDb::TCommand cmd(StorageDb, string("INSERT OR IGNORE INTO EventItem (event_fk, iso_639_language_code, title, " \
                                            "description) VALUES (?, ?, ?, ?);"));
// TODO: KSS this error we were getting without cmd.Bind(byte, static_cast<int32_t>(event_fk)); 
//src/TDvbSiStorage.cpp:2086:36: error: call of overloaded ‘Bind(int32_t&, int64_t&)’ is ambiguous
//src/TDvbSiStorage.cpp:2086:36: note: candidates are:
//./include/TDvbDb.h:386:13: note: int32_t TDvbDb::TCommand::Bind(int32_t, int)
//./include/TDvbDb.h:392:13: note: int32_t TDvbDb::TCommand::Bind(int32_t, long long int)

            int32_t byte(1);
            cmd.Bind(byte, static_cast<int32_t>(event_fk));
            if(iso_639_language_code.empty())
            {
                cmd.Bind(2);
            }
            else
            {
                cmd.Bind(2, iso_639_language_code);
            }

            if(title.empty())
            {
                cmd.Bind(3);
            }
            else
            {
                cmd.Bind(3, title);
            }

            if(description.empty())
            {
                cmd.Bind(4);
            }
            else
            {
                cmd.Bind(4, description);
            }

            cmd.Execute(eventItem_fk);

            OS_LOG(DVB_DEBUG,   "<%s> Insert eventItem_fk: %ld\n", __FUNCTION__, eventItem_fk);
        }
    }

    return  eventItem_fk; 
}

/**
 * Handle Tot table
 *
 * @param tot Tot table
 */
void TDvbSiStorage::HandleTotEvent(const TTotTable& tot)
{
    if(tot.GetTableId() == TTableId::TABLE_ID_TDT)
    {
        //OS_LOG(DVB_DEBUG,   "  TDT: Time and Date Table\n");

        time_t newTime = tot.GetUtcTime();
        if(newTime > 0)
        {
            OS_LOG(DVB_DEBUG,   "<%s> newTime = %ld\n", __FUNCTION__, newTime);

            struct timeval timeVal;
            timeVal.tv_sec = newTime;
            int ret = settimeofday(&timeVal, NULL);
            if(ret < 0)
            {
                OS_LOG(DVB_ERROR,   "<%s> settimeofday(%ld) failed, %s\n", __FUNCTION__, newTime, strerror(errno));
            }
            StorageDb.UpdateTotStatus(true);
        }
        else
        {
            OS_LOG(DVB_ERROR,   "<%s> MjdToDate() failed: %s\n", __FUNCTION__, strerror(errno));
        }
    }
    else if(tot.GetTableId() == TTableId::TABLE_ID_TDT)
    {
        //OS_LOG(DVB_DEBUG,   "  TOT: Time Offset Table\n");
        //OS_LOG(DVB_DEBUG,   "  TOT: Time Offset Table\n");
    }

    OS_LOG(DVB_DEBUG,   "\tUTC time       : %" PRId64"\n", tot.GetUtcTimeBcd());
}

/**
 * Handle SI tables
 *
 * @param tbl si table
 */
void TDvbSiStorage::HandleTableEvent(const TSiTable& tbl)
{
    TTableId tableId = tbl.GetTableId();

    if((tableId == TTableId::TABLE_ID_NIT) || (tableId == TTableId::TABLE_ID_NIT_OTHER))
    {
        // Only handle NIT tables from preferred Network if specified.
        if(PreferredNetworkId == 0 || PreferredNetworkId == tbl.GetTableExtensionId())
        {
            HandleNitEvent(static_cast<const TNitTable&>(tbl));
        }
    }
    else if((tableId == TTableId::TABLE_ID_SDT) || (tableId == TTableId::TABLE_ID_SDT_OTHER))
    {
        HandleSdtEvent(static_cast<const TSdtTable&>(tbl));
    }
    else if((tableId >= TTableId::TABLE_ID_EIT_PF) && (tableId <= TTableId::TABLE_ID_EIT_SCHED_OTHER_END))
    {
        HandleEitEvent(static_cast<const TEitTable&>(tbl));
    }
    else if((tableId == TTableId::TABLE_ID_TDT) || (tableId == TTableId::TABLE_ID_TOT))
    {
        HandleTotEvent(static_cast<const TTotTable&>(tbl));
    }
    else if(tableId == TTableId::TABLE_ID_BAT)
    {
        HandleBatEvent(static_cast<const TBatTable&>(tbl));
    }
    else 
    {
        OS_LOG(DVB_ERROR,   "<%s> Unknown table id = 0x%x\n", __FUNCTION__, tableId);
    }

}

/**
 * Thread start up function
 */
void TDvbSiStorage::EventMonitorThread()
{
// TODO: KSS
#if 0
    rmf_osal_event_handle_t eventHandle;
    rmf_osal_event_params_t eventParams = {};
    uint32_t eventType;
    rmf_Error err;

    while(true)
    {
        err = rmf_osal_eventqueue_Get_next_event(m_eventQueueId, &eventHandle, NULL, &eventType, &eventParams);
        if(err != RMF_SUCCESS)
        {
            OS_LOG(DVB_ERROR,  
                    "<%s:> - unable to Get event,... terminating thread. err = 0x%x\n", __FUNCTION__, err);
            break;
        }
// DvbSiStorage does not support raw sections as an input
#ifndef DVB_SECTION_OUTPUT
        TSiTable *tbl = static_cast<SiTable *>(eventParams.data);
        OS_LOG(DVB_DEBUG,   "<%s> Received table event, id = 0x%x\n", __FUNCTION__, tbl->GetTableId());
        HandleTableEvent(*tbl);
#else
        OS_LOG(DVB_DEBUG,   "<%s> Received section: size = 0x%x, table_id = 0x%x\n",
                __FUNCTION__, eventParams.data_extension, *((uint8_t*)eventParams.data));
#endif
        rmf_osal_event_delete(eventHandle);
    }
#endif
}

/**
 * Start scanning
 *
 * @param bFast boolean bFast true fast scan; false background scan
 */
bool TDvbSiStorage::StartScan(bool bFast)
{
    OS_LOG(DVB_INFO,   "%s(%d): called\n", __FUNCTION__, bFast);

    std::lock_guard<std::mutex> lock(ScanMutex);

    // Check if the scan is already in progress
    if(DvbScanStatus.ScanState != TDvbScanState::SCAN_STOPPED)
    {
        OS_LOG(DVB_ERROR,   "%s(): scan is already in progress\n", __FUNCTION__);
        return false;
    }

    DvbScanStatus.ScanState = TDvbScanState::SCAN_STARTING;
    ScanThreadObject = std::thread(&TDvbSiStorage::ScanThread, this, bFast);

    return true;
}

/**
 * Stop scanning
 */
void TDvbSiStorage::StopScan()
{
    OS_LOG(DVB_INFO,   "%s(): called\n", __FUNCTION__);

    std::unique_lock<std::mutex> lk(ScanMutex);

    if(DvbScanStatus.ScanState == TDvbScanState::SCAN_STOPPED)
    {
        OS_LOG(DVB_INFO,   "%s(): scan is already stopped\n", __FUNCTION__);
        return;
    }

    while(DvbScanStatus.ScanState != TDvbScanState::SCAN_STOPPED)
    {
        OS_LOG(DVB_INFO,   "%s(): sending stop request\n", __FUNCTION__);
        lk.unlock();
        ThreadScanCondition.notify_one();
        sleep(3);
        lk.lock();
    }

    if(ScanThreadObject.joinable())
    {
        ScanThreadObject.join();
    }

    OS_LOG(DVB_INFO,   "%s(): done\n", __FUNCTION__);
}

/**
 * Scan thread main loop
 *
 * @param bFast boolean true fast scan; false background scan
 */
void TDvbSiStorage::ScanThread(bool bFast)
{
    OS_LOG(DVB_INFO,   "%s(): created(%d)\n", __FUNCTION__, bFast);

    while(true)
    {
        if(bFast)
        {
            DvbScanStatus.ScanState = TDvbScanState::SCAN_IN_PROGRESS_FAST;

            if(!IsFastScanEnabled())
            {
                OS_LOG(DVB_ERROR,   "%s(): IsFastScanEnabled() failed\n", __FUNCTION__);
                DvbScanStatus.ScanState = TDvbScanState::SCAN_FAILED;
            }
            else
            {
                // It's enough to run the fast scan only once
                bFast = false;
                continue;
            }
        }
        else
        {
            DvbScanStatus.ScanState = TDvbScanState::SCAN_IN_PROGRESS_BKGD;
            if(!IsBackgroundScanEnabled())
            {
                OS_LOG(DVB_ERROR,   "%s(): IsBackgroundScanEnabled() failed\n", __FUNCTION__);
                DvbScanStatus.ScanState = TDvbScanState::SCAN_FAILED;
            }
            else
            {
                OS_LOG(DVB_INFO,   "%s(): scan completed successfully\n", __FUNCTION__);
                DvbScanStatus.ScanState = TDvbScanState::SCAN_COMPLETED;
            }
        }

        StorageDb.Audits();

        // Let's check if we need to stop the scan
        {
            std::unique_lock<std::mutex> lk(ScanMutex);
            // TODO: Handle spurious wake-ups
            if(ThreadScanCondition.wait_for(lk, std::chrono::seconds(bFast ? 30 : BackGroundScanInterval)) == std::cv_status::no_timeout)
            {
                OS_LOG(DVB_INFO,   "%s(): stopping\n", __FUNCTION__);
                DvbScanStatus.ScanState = TDvbScanState::SCAN_STOPPED;
                return;
            }
        }
    }

}

/**
 * Scan Home transport stream
 */
bool TDvbSiStorage::ScanHome()
{
    TDvbSiTableStatus status;

    // Let's start over clean slate
    ClearCachedTables();
// TODO: KSS remove tuner dependencies
#if 0    
  rmf_DvbTuner tuner;

    OS_LOG(DVB_INFO,   "%s:%d: tuning to home ts(%d)\n", __FUNCTION__, __LINE__, HomeTsFrequency);
    RMFResult ret = tuner.tune(HomeTsFrequency, HomeTsModulationMode, HomeTsSymbolRate);
    if(ret != RMF_RESULT_SUCCESS)
    {
        OS_LOG(DVB_ERROR,   "%s(): tune(%d) failed with 0x%x\n", __FUNCTION__, HomeTsFrequency, ret);
        // scan failed
        return false;
    }
#endif
    vector<shared_ptr<TSiTable>> tables;

    // NIT
    tables.emplace_back(new TNitTable((uint8_t)TTableId::TABLE_ID_NIT, PreferredNetworkId, 0, true));

    // BAT(s)
    for(auto it = HomeBouquetsVector.begin(), end = HomeBouquetsVector.end(); it != end; ++it)
    {
        OS_LOG(DVB_DEBUG,   "%s:%d: Adding BAT(0x%x) to the list\n", __FUNCTION__, __LINE__, *it);
        tables.emplace_back(new TBatTable((uint8_t)TTableId::TABLE_ID_BAT, *it, 0, true));
    }

    OS_LOG(DVB_INFO,   "%s:%d: Collecting NIT & BAT(s)\n", __FUNCTION__, __LINE__);
    if(CheckCacheTableCollections(tables, NIT_TIMEOUT > BAT_TIMEOUT ? NIT_TIMEOUT : BAT_TIMEOUT))
    {
        OS_LOG(DVB_INFO,   "%s:%d: NIT & BAT(s) found\n", __FUNCTION__, __LINE__);
        status.NitAcquired = true;
        status.BatAcquired = true;
    }
    else
    {
        OS_LOG(DVB_ERROR,   "%s:%d: NIT & BAT(s) not found\n", __FUNCTION__, __LINE__);
        return false;
    }

    tables.clear();

    // Collect SDT & EIT pf(optional)
    vector<shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(PreferredNetworkId);
    for(auto it = tsList.begin(), end = tsList.end(); it != end; ++it)
    {
        if(!IsFastScan)
        {
            if(HomeTsFrequency == (*it)->Frequency)
            {
                OS_LOG(DVB_DEBUG,   "%s:%d: Adding SDT(0x%x.0x%x) actual to the list\n",
                        __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId);
                TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
                sdt->SetOriginalNetworkId((*it)->NetworkId);
                tables.emplace_back(sdt);
                break;
            }
        }
        else
        {
            OS_LOG(DVB_DEBUG,   "%s:%d: Adding SDT(0x%x.0x%x) to the list\n",
                    __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId);
            TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
            sdt->SetOriginalNetworkId((*it)->NetworkId);
            tables.emplace_back(sdt);
        }
    }

    if(CheckCacheTableCollections(tables, IsFastScan ? SDT_OTHER_TIMEOUT : SDT_TIMEOUT))
    {
        OS_LOG(DVB_INFO,   "%s:%d: All SDTs received\n", __FUNCTION__, __LINE__);
        status.SdtAcquired = true;
    }
    else
    {
        OS_LOG(DVB_ERROR,   "%s:%d: All SDTs not received\n", __FUNCTION__, __LINE__);
    }

    DvbScanStatus.TsList.emplace_back(HomeTsFrequency, status);

    return true;
}

/**
 * Scan fast
 */
bool TDvbSiStorage::IsFastScanEnabled()
{
    OS_LOG(DVB_INFO,   "%s: Started\n", __FUNCTION__);

    DvbScanStatus.TsList.clear();

    if(!ScanHome())
    {
        OS_LOG(DVB_ERROR,   "%s: ScanHome() failed\n", __FUNCTION__);
        return false;
    }

//    rmf_DvbTuner tuner;

    vector<shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(PreferredNetworkId);
    for(auto it = tsList.begin(), end = tsList.end(); it != end; ++it)
    {
        TDvbSiTableStatus status;

        // collect SDTa & EITa pf
        TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
        sdt->SetOriginalNetworkId((*it)->NetworkId);

        vector<shared_ptr<TSiTable>> tables;
        tables.emplace_back(sdt);

        vector<shared_ptr<TDvbStorageNamespace::ServiceStruct>> serviceList = GetServiceListByTsIdCache((*it)->NetworkId, (*it)->TransportStreamId);
        for(auto srv = serviceList.begin(), end = serviceList.end(); srv != end; ++srv)
        {
            OS_LOG(DVB_DEBUG,   "%s:%d: Adding EITpf(0x%x.0x%x.0x%x) to the list\n",
                    __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId, (*srv)->ServiceId);

            TEitTable* eit = new TEitTable((uint8_t)TTableId::TABLE_ID_EIT_PF, (*srv)->ServiceId, 0, true);
            eit->SetNetworkId((*it)->NetworkId);
            eit->SetTsId((*it)->TransportStreamId);
            tables.emplace_back(eit);
        }

        if(IsFastScan && CheckCacheTableCollections(tables, 0))
        {
            OS_LOG(DVB_INFO,   "%s:%d: SDTa(0x%x.0x%x) & EITa pf already received. Skipping.\n",
                    __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
            status.SdtAcquired = true;
            status.EitPfAcquired = true;
        }
        else
        {
            OS_LOG(DVB_INFO,   "%s:%d: tune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);
// TODO: KSS
#if 0
            RMFResult ret = tuner.tune((*it)->Frequency, (*it)->modulation, (*it)->symbolRate);
            if(ret != RMF_RESULT_SUCCESS)
            {
                OS_LOG(DVB_ERROR,   "%s(): tune(%d) failed with 0x%x\n", __FUNCTION__, (*it)->Frequency, ret);
            }
#endif
            OS_LOG(DVB_INFO,   "%s:%d: Collecting SDTa & EITs pf\n", __FUNCTION__, __LINE__);
            if(CheckCacheTableCollections(tables, SDT_TIMEOUT > EIT_PF_TIMEOUT ? SDT_TIMEOUT : EIT_PF_TIMEOUT))
            {
                OS_LOG(DVB_INFO,   "%s:%d: SDT(0x%x.0x%x) & EITs pf received\n",
                        __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
                status.SdtAcquired = true;
                status.EitPfAcquired = true;
            }
            else
            {
                OS_LOG(DVB_ERROR,   "%s:%d: SDT(0x%x.0x%x) and/or EITs pf not received\n",
                        __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
            }

            OS_LOG(DVB_INFO,   "%s:%d: untune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);
// TODO: KSS
            //tuner.untune();
        }

        DvbScanStatus.TsList.emplace_back((*it)->Frequency, status);
    }

    OS_LOG(DVB_ERROR,   "%s:%d: Done\n", __FUNCTION__, __LINE__);

    return true;
}

/**
 * Scan background
 */
bool TDvbSiStorage::IsBackgroundScanEnabled()
{
    OS_LOG(DVB_INFO,   "%s: Started\n", __FUNCTION__);

    DvbScanStatus.TsList.clear();

    if(!ScanHome())
    {
        OS_LOG(DVB_ERROR,   "%s: ScanHome() failed\n", __FUNCTION__);
        return false;
    }

// TODO: KSS
//    rmf_DvbTuner tuner;
    vector<shared_ptr<TSiTable>> fullEitSchedule;

    vector<shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(GetPreferredNetworkId());
    for(auto it = tsList.begin(), end = tsList.end(); it != end; ++it)
    {
        TDvbSiTableStatus status;

        OS_LOG(DVB_INFO,   "%s:%d: tune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);
// TODO: KSS
#if 0
        RMFResult ret = tuner.tune((*it)->Frequency, (*it)->modulation, (*it)->SymbolRate);
        if(ret != RMF_RESULT_SUCCESS)
        {
            OS_LOG(DVB_ERROR,   "%s(): tune(%d) failed with 0x%x\n", __FUNCTION__, (*it)->Frequency, ret);
        }
#endif
        // collect SDTa & EITa pf
        // SDTa
        TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
        sdt->SetOriginalNetworkId((*it)->NetworkId);

        vector<shared_ptr<TSiTable>> tables;
        vector<shared_ptr<TSiTable>> eitSchedule;
        tables.emplace_back(sdt);

        OS_LOG(DVB_INFO,   "%s:%d: Collecting SDTa(0x%x.0x%x)\n",
                __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
        if(CheckCacheTableCollections(tables, SDT_TIMEOUT))
        {
            OS_LOG(DVB_INFO,   "%s:%d: SDTa(0x%x.0x%x) received\n",
                    __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
            status.SdtAcquired = true;
        }
        else
        {
            OS_LOG(DVB_ERROR,   "%s:%d: SDTa(0x%x.0x%x) not received\n",
                    __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
        }

        tables.clear();

        //EITa shed & EITa pf
        vector<shared_ptr<TDvbStorageNamespace::ServiceStruct>> serviceList = GetServiceListByTsIdCache((*it)->NetworkId, (*it)->TransportStreamId);
        for(auto srv = serviceList.begin(), end = serviceList.end(); srv != end; ++srv)
        {
            OS_LOG(DVB_DEBUG,   "%s:%d: Adding EITsched(0x%x.0x%x.0x%x) to the list\n",
                                    __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId, (*srv)->ServiceId);

            TEitTable* eitSched = new TEitTable((uint8_t)TTableId::TABLE_ID_EIT_SCHED_START, (*srv)->ServiceId, 0, true);
            eitSched->SetNetworkId((*it)->NetworkId);
            eitSched->SetTsId((*it)->TransportStreamId);
            eitSchedule.emplace_back(eitSched);

            TEitTable* eit = new TEitTable((uint8_t)TTableId::TABLE_ID_EIT_PF, (*srv)->ServiceId, 0, true);
            eit->SetNetworkId((*it)->NetworkId);
            eit->SetTsId((*it)->TransportStreamId);
            tables.emplace_back(eit);
        }

        fullEitSchedule.insert(fullEitSchedule.end(), eitSchedule.begin(), eitSchedule.end());

        OS_LOG(DVB_INFO,   "%s:%d: Collecting EITa pf\n", __FUNCTION__, __LINE__);
        if(CheckCacheTableCollections(tables, EIT_PF_TIMEOUT))
        {
            OS_LOG(DVB_INFO,   "%s:%d: EITa pf received\n",
                    __FUNCTION__, __LINE__);
            status.EitPfAcquired = true;
        }
        else
        {
            OS_LOG(DVB_ERROR,   "%s:%d: EITa pf not received\n",
                    __FUNCTION__, __LINE__);
        }

        if((*it)->Frequency != BarkerFrequency)
        {
            OS_LOG(DVB_INFO,   "%s:%d: Collecting EITa sched\n", __FUNCTION__, __LINE__);
            if(CheckCacheTableCollections(eitSchedule, EIT_8_DAY_SCHED_TIMEOUT))
            {
                OS_LOG(DVB_INFO,   "%s:%d: EITsched received\n", __FUNCTION__, __LINE__);
                status.EitAcquired = true;
            }
            else
            {
                OS_LOG(DVB_ERROR,   "%s:%d: EITsched not received\n", __FUNCTION__, __LINE__);
            }
        }
        else
        {
            OS_LOG(DVB_INFO,   "%s:%d: Not collecting EITa sched on barker(%d)\n",
                    __FUNCTION__, __LINE__, (*it)->Frequency);
        }

        OS_LOG(DVB_INFO,   "%s:%d: untune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);
// TODO: KSS
//        tuner.untune();

        DvbScanStatus.TsList.emplace_back((*it)->Frequency, status);
    }

    // Sitting on barker ts
    if(BarkerFrequency && BarkerModulationMode && BarkerSymbolRate)
    {
        TDvbSiTableStatus status;
        // Clear cached EIT tables
        {
            OS_LOG(DVB_DEBUG,   "%s:%d: Clearing cached EIT tables\n", __FUNCTION__, __LINE__);
            std::lock_guard<std::mutex> lock(CacheDataMutex);
            TEitTableMap.clear();
        }

        OS_LOG(DVB_INFO,   "%s:%d: tune(%d) barker\n", __FUNCTION__, __LINE__, BarkerFrequency);
// TODO: KSS
#if 0
        RMFResult ret = tuner.tune(BarkerFrequency, BarkerModulationMode, BarkerSymbolRate);
        if(ret != RMF_RESULT_SUCCESS)
        {
            OS_LOG(DVB_ERROR,   "%s(): tune(%d) failed with 0x%x\n", __FUNCTION__, BarkerFrequency, ret);
        }
#endif
        OS_LOG(DVB_INFO,   "%s:%d: Collecting EIT sched (barker)\n", __FUNCTION__, __LINE__);
        if(CheckCacheTableCollections(fullEitSchedule, BarkerEitTimout))
        {
            OS_LOG(DVB_INFO,   "%s:%d: EITsched received (barker)\n", __FUNCTION__, __LINE__);
            status.EitAcquired = true;
        }
        else
        {
            OS_LOG(DVB_ERROR,   "%s:%d: EITsched not received (barker)\n", __FUNCTION__, __LINE__);
        }

        OS_LOG(DVB_INFO,   "%s:%d: untune(%d)\n", __FUNCTION__, __LINE__, BarkerFrequency);
// TODO: KSS
//        tuner.untune();

        DvbScanStatus.TsList.emplace_back(BarkerFrequency, status);
    }

    OS_LOG(DVB_INFO,   "%s:%d: Done\n", __FUNCTION__, __LINE__);

    return true;
}

/**
 * Check cache collections
 */
bool TDvbSiStorage::CheckCacheTableCollections(vector<shared_ptr<TSiTable>>& tables, int timeout)
{
    do
    {
        bool found = true;
        for(auto tbl = tables.begin(), end = tables.end(); tbl != end; ++tbl)
        {
            std::lock_guard<std::mutex> lock(CacheDataMutex);
            TTableId tableId = (*tbl)->GetTableId();
            if((tableId == TTableId::TABLE_ID_NIT) || (tableId == TTableId::TABLE_ID_NIT_OTHER))
            {
                OS_LOG(DVB_DEBUG,   "%s:%d: Looking for NIT(0x%x)\n",
                        __FUNCTION__, __LINE__, (*tbl)->GetTableExtensionId());
                auto it = NitTableMap.find((*tbl)->GetTableExtensionId());
                if(it == NitTableMap.end())
                {
                    found = false;
                    break;
                }
                else
                {
                    OS_LOG(DVB_DEBUG,   "%s:%d: NIT(0x%x) found\n",
                            __FUNCTION__, __LINE__, (*tbl)->GetTableExtensionId());
                }
            }
            else if(tableId == TTableId::TABLE_ID_BAT)
            {
                OS_LOG(DVB_DEBUG,   "%s:%d: Looking for BAT(0x%x)\n",
                        __FUNCTION__, __LINE__, (*tbl)->GetTableExtensionId());
                auto it = TBatTableMap.find((*tbl)->GetTableExtensionId());
                if(it == TBatTableMap.end())
                {
                    found = false;
                    break;
                }
                else
                {
                    OS_LOG(DVB_DEBUG,   "%s:%d: BAT(0x%x) found\n",
                            __FUNCTION__, __LINE__, (*tbl)->GetTableExtensionId());
                }
            }
            else if((tableId == TTableId::TABLE_ID_SDT) || (tableId == TTableId::TABLE_ID_SDT_OTHER))
            {
                shared_ptr<TSdtTable> sdt = std::static_pointer_cast<TSdtTable>((*tbl));

                OS_LOG(DVB_DEBUG,   "%s:%d: Looking for SDT(0x%x.0x%x)\n",
                        __FUNCTION__, __LINE__, sdt->GetOriginalNetworkId(), sdt->GetTableExtensionId());

                pair<uint16_t, uint16_t> key(sdt->GetOriginalNetworkId(), sdt->GetTableExtensionId());
                auto it = SdtTableMap.find(key);
                if(it == SdtTableMap.end())
                {
                    found = false;
                    break;
                }
                else
                {
                    OS_LOG(DVB_DEBUG,   "%s:%d: SDT(0x%x.0x%x) found\n",
                            __FUNCTION__, __LINE__, sdt->GetOriginalNetworkId(), sdt->GetTableExtensionId());
                }
            }
            else if((tableId >= TTableId::TABLE_ID_EIT_PF) && (tableId <= TTableId::TABLE_ID_EIT_SCHED_OTHER_END))
            {
                bool isPf = false;
                if(tableId == TTableId::TABLE_ID_EIT_PF || tableId == TTableId::TABLE_ID_EIT_PF_OTHER)
                {
                    isPf = true;
                }

                shared_ptr<TEitTable> eit = std::static_pointer_cast<TEitTable>((*tbl));

                OS_LOG(DVB_DEBUG,   "%s:%d: Looking for EIT(0x%x.0x%x.0x%x) isPF: %d\n",
                        __FUNCTION__, __LINE__, eit->GetNetworkId(), eit->GetTsId(), eit->GetTableExtensionId(), isPf);

                tuple<uint16_t, uint16_t, uint16_t, bool> key(eit->GetNetworkId(), eit->GetTsId(), eit->GetTableExtensionId(), isPf);
                auto it = TEitTableMap.find(key);
                if(it == TEitTableMap.end())
                {
                    found = false;
                    break;
                }
                else
                {
                    OS_LOG(DVB_DEBUG,   "%s:%d: Looking for EIT(0x%x.0x%x.0x%x) isPF: %d found\n",
                            __FUNCTION__, __LINE__, eit->GetNetworkId(), eit->GetTsId(), eit->GetTableExtensionId(), isPf);
                }
            }
        }

        if(found)
        {
            return true;
        }

        // TODO: Consider using ThreadScanCondition.wait_for() here
        sleep(1);
    }
    while(timeout-- > 0);

    return false;
}

/**
 * Clear out cach collections
 */
void TDvbSiStorage::ClearCachedTables()
{
    std::lock_guard<std::mutex> lock(CacheDataMutex);

    OS_LOG(DVB_INFO,   "%s:%d: Clearing cached tables\n", __FUNCTION__, __LINE__);
    NitTableMap.clear();
    SdtTableMap.clear();
    TEitTableMap.clear();
    TBatTableMap.clear();
}

/**
 * Return scanner status
 */
TDvbSiStorage::TDvbScanStatus TDvbSiStorage::GetScanStatus()
{
    OS_LOG(DVB_INFO,   "%s:%d DvbScanStatus.tsList.size() = %lu\n", __FUNCTION__, __LINE__, DvbScanStatus.TsList.size());
    return DvbScanStatus;
}

#if 0 // TODO: KSS remove json dependencies.
/**
 * Return a vector of InbandTableInfoStruct structures
 *
 * @param profile profile name
 * @return vector of shared pointers of InbandTableInfoStruct structures
 */
vector<shared_ptr<TDvbStorageNamespace::InbandTableInfoStruct>> TDvbSiStorage::GetInbandTableInfo(string& profile)
{
    vector<shared_ptr<TDvbStorageNamespace::InbandTableInfoStruct>> ret;

    OS_LOG(DVB_DEBUG,   "%s:%d: called\n", __FUNCTION__, __LINE__);

    json_error_t error;
    json_t* json = json_load_file(DvbConfigProfileFile.c_str(), 0, &error);
    if(!json)
    {
        OS_LOG(DVB_ERROR,   "%s:%d: json_load_file(%s) failed, error = %s\n",
                __FUNCTION__, __LINE__, DvbConfigProfileFile.c_str(), error.text);
        return ret;
    }

    // Search for object with name == profile
    size_t size = json_array_size(json);

    OS_LOG(DVB_DEBUG,   "%s:%d: number of profiles = %d\n", __FUNCTION__, __LINE__, size);

    for(size_t i = 0; i < size; i++)
    {
        json_t* obj = json_array_Get(json, i);
        if(!obj)
        {
            continue;
        }

        json_t* name = json_object_Get(obj, "name");
        if(!name)
        {
            continue;
        }

        OS_LOG(DVB_DEBUG,   "%s:%d: profile = %s\n", __FUNCTION__, __LINE__, json_string_value(name));

        if(profile.compare(json_string_value(name)) == 0)
        {
            OS_LOG(DVB_DEBUG,   "%s:%d: requested profile found\n", __FUNCTION__, __LINE__);

            // Get "tables" object
            json_t* tables = json_object_get(obj, "tables");
            if(tables)
            {
                OS_LOG(DVB_DEBUG,   "%s:%d: tables object found\n", __FUNCTION__, __LINE__);

                // Get "inband" array
                json_t* inband = json_object_get(tables, "inband");
                if(inband)
                {
                    OS_LOG(DVB_DEBUG,   "%s:%d: inband array found\n", __FUNCTION__, __LINE__);

                    // Read table objects
                    size_t num = json_array_size(inband);
                    for(size_t j = 0; j < num; j++)
                    {
                        json_t* t = json_array_get(inband, j);
                        if(!t)
                        {
                            continue;
                        }

                        json_t* v = json_object_get(t, "pid");
                        if(!v)
                        {
                            continue;
                        }
                        uint16_t pid = json_integer_value(v);

                        v = json_object_get(t, "table_id");
                        if(!v)
                        {
                            continue;
                        }
                        uint16_t tableId = json_integer_value(v);

                        v = json_object_get(t, "table_id_ext");
                        if(!v)
                        {
                            continue;
                        }
                        uint16_t extensionId = json_integer_value(v);

                        OS_LOG(DVB_DEBUG,   "%s:%d: table (0x%x, 0x%x, 0x%x)\n", __FUNCTION__, __LINE__, pid, tableId, extensionId);
                        ret.emplace_back(new TDvbStorageNamespace::InbandTableInfoStruct(pid, tableId, extensionId));
                    }
                }
            }

            break;
        }
    }

    json_decref(json);

    return ret;
}

/**
  * Return the DVB profile configuration file
  *
  * @return string configuration file (json)
  */
string TDvbSiStorage::GetProfiles()
{
    string res;
    json_error_t error;

    json_t* json = json_load_file(DvbConfigProfileFile.c_str(), 0, &error);
    if(json)
    {
        res = string(json_dumps(json, JSON_INDENT(2)|JSON_ENSURE_ASCII|JSON_PRESERVE_ORDER));
        json_decref(json);
    }
    else
    {
        OS_LOG(DVB_ERROR,   "%s:%d: json_load_file(%s) failed, error = %s\n",
                __FUNCTION__, __LINE__, DvbConfigProfileFile.c_str(), error.text);
    }

    return res;
}

/**
 * Set the DVB profile configuration file
 *
 * @param profiles configuration file in a string (json)
 * @return true if successfull and false otherwise
 */
bool TDvbSiStorage::SetProfiles(string& profiles)
{
    bool ret = true;

    json_error_t error;
    json_t* json = json_loads(profiles.c_str(), 0, &error);
    if(json)
    {
        if(json_dump_file(json, DvbConfigProfileFile.c_str(), JSON_INDENT(2)|JSON_ENSURE_ASCII|JSON_PRESERVE_ORDER) == -1)
        {
            OS_LOG(DVB_ERROR,   "%s:%d: json_dump_file(%s) failed\n",
                    __FUNCTION__, __LINE__, DvbConfigProfileFile.c_str());
            ret = false;
        }
        json_decref(json);
    }
    else
    {
        OS_LOG(DVB_ERROR,   "%s:%d: json_loads() failed, error = %s\n",
                __FUNCTION__, __LINE__, error.text);
        ret = false;
    }

    return ret;
}

#endif // TODO: KSS
