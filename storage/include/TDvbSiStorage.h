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

#ifndef TDVBSISTORAGE_H
#define TDVBSISTORAGE_H

// C system includes

// C++ system includes
#include <mutex>
#include <map>
#include <utility>
#include <vector>
#include <string>
#include <tuple>
#include <memory>
#include <thread>
#include <condition_variable>

// Other libraries' includes

// Project's includes
//#include "rmf_osal_event.h"
//#include "rmf_osal_thread.h"
//#include "rmf_qamsrc_common.h"
#include "TDvbDb.h"

/**
 * TDvbStorageNamespace namespace
 */
namespace TDvbStorageNamespace
{
  enum TModulationMode {
    MODULATION_MODE_UNKNOWN=0,
    MODULATION_MODE_QPSK,
    MODULATION_MODE_BPSK,
    MODULATION_MODE_OQPSK,
    MODULATION_MODE_VSB8,
    MODULATION_MODE_VSB16,
    MODULATION_MODE_QAM16,
    MODULATION_MODE_QAM32,
    MODULATION_MODE_QAM64,
    MODULATION_MODE_QAM80,
    MODULATION_MODE_QAM96,
    MODULATION_MODE_QAM112,
    MODULATION_MODE_QAM128,
    MODULATION_MODE_QAM160,
    MODULATION_MODE_QAM192,
    MODULATION_MODE_QAM224,
    MODULATION_MODE_QAM256,
    MODULATION_MODE_QAM320,
    MODULATION_MODE_QAM384,
    MODULATION_MODE_QAM448,
    MODULATION_MODE_QAM512,
    MODULATION_MODE_QAM640,
    MODULATION_MODE_QAM768,
    MODULATION_MODE_QAM896,
    MODULATION_MODE_QAM1024,
    MODULATION_MODE_QAM_NTSC // for analog mode
  };

  /**
  * TransportStream structure. Used to represent a Transport Stream from storage collections.
  */
  typedef struct TStorageTransportStream {
    TStorageTransportStream(uint32_t fr, TModulationMode mod,
      uint32_t sym, uint16_t net, uint16_t ts)
      : Frequency(fr),
        Modulation(mod),
        SymbolRate (sym),
        NetworkId(net),
        TransportStreamId(ts)
    {
      // Empty
    }
    uint32_t Frequency;
    TModulationMode Modulation;
    uint32_t SymbolRate;
    uint16_t NetworkId;
    uint16_t TransportStreamId;
  } TStorageTransportStreamStruct;

  /**
  * Service structure. Used to represent a Service from the storage collections.
  */
  typedef struct Service {
    Service(uint16_t net, uint16_t ts, uint16_t id, std::string name)
    : NetworkId(net),
      TransportStreamId(ts),
      ServiceId(id),
      ServiceName(std::move(name))
    {
      // Empty
    }
    uint16_t NetworkId;
    uint16_t TransportStreamId;
    uint16_t ServiceId;
    std::string ServiceName;
  } ServiceStruct;

  /**
  * Event structure. Used to represent an Event from the storage collections.
  */
  typedef struct Event {
    uint16_t NetworkId;
    uint16_t TransportStreamId;
    uint16_t ServiceId;
    uint16_t EventId;
    uint64_t StartTime;
    uint32_t Duration;
    std::string EventName;
    std::string EventText;
  } EventStruct;

  /**
  * Inband table info structure. Used to describe a table from a DVB profile.
  */
  typedef struct InbandTableInfo {
    InbandTableInfo(uint16_t p, uint8_t id, uint16_t extId)
    : Pid(p),
      ExtensionId(extId),
      TableId(id)
    {
      // Empty
    }
    uint16_t Pid;
    uint16_t ExtensionId;
    uint8_t  TableId;
  } InbandTableInfoStruct;
} // Namespace

// Forward declarations
class TSiTable;
class TNitTable;
class TSdtTable;
class TEitTable;
class TTotTable;
class TBatTable;
class TTransportStream;

/**
 * TDvbSiStorage class. Main controller class for collecting and storing DVB SI data.
 */
class TDvbSiStorage
{
private:
  // DvbScan timeout values
  enum {
    NIT_TIMEOUT = 15,
    NIT_OTHER_TIMEOUT = 15,
    SDT_TIMEOUT = 5,
    SDT_OTHER_TIMEOUT = 15,
    BAT_TIMEOUT = 15,
    EIT_PF_TIMEOUT = 5,
    EIT_PF_OTHER_TIMEOUT = 15,
    EIT_8_DAY_SCHED_TIMEOUT = 15,
    EIT_PAST_8_DAY_SCHED_TIMEOUT = 60,
  };

  enum class TDvbScanState {
    SCAN_STOPPED,
    SCAN_STARTING,
    SCAN_IN_PROGRESS_FAST,
    SCAN_IN_PROGRESS_BKGD,
    SCAN_COMPLETED,
    SCAN_FAILED
  };

  struct TDvbSiTableStatus {
    bool NitAcquired;
    bool BatAcquired;
    bool SdtAcquired;
    bool EitPfAcquired;
    bool EitAcquired;
  };

  struct TDvbScanStatus {
    TDvbScanState ScanState;
    std::vector<std::pair<uint32_t, TDvbSiTableStatus>> TsList;
  };

  static std::mutex SiStorageMutex;
  std::mutex CacheDataMutex;
  std::mutex ScanMutex;

#if 0
    /**
     *  Handle to the event queue
     */
    rmf_osal_eventqueue_handle_t m_eventQueueId;

    /**
     *  Thread id
     */
    // TODO: Consider moving to std::thread
    rmf_osal_ThreadId m_threadId;
#endif

  TDvbDb StorageDb;
  std::map<uint16_t, std::shared_ptr<TNitTable>> NitTableMap;
  std::map<std::pair<uint16_t, uint16_t>, std::shared_ptr<TSdtTable>> SdtTableMap;
  std::map<std::tuple<uint16_t, uint16_t, uint16_t, bool>, std::shared_ptr<TEitTable>> TEitTableMap;
  std::map<uint16_t, std::shared_ptr<TBatTable>> TBatTableMap;


  uint16_t PreferredNetworkId;
  bool IsFastScan;
  uint32_t HomeTsFrequency;
  TDvbStorageNamespace::TModulationMode HomeTsModulationMode;
  uint32_t HomeTsSymbolRate;
  std::vector<uint16_t> HomeBouquetsVector;
  uint32_t BarkerFrequency;
  TDvbStorageNamespace::TModulationMode BarkerModulationMode;
  uint32_t BarkerSymbolRate;
  uint32_t BackGroundScanInterval;
  uint32_t BarkerEitTimout;

  std::thread ScanThreadObject;
  TDvbScanStatus DvbScanStatus;
  std::condition_variable ThreadScanCondition;
  std::string DvbConfigProfileFile;

  // Disable Default Copy Constructor
  TDvbSiStorage();
  TDvbSiStorage(const TDvbSiStorage& other)  = delete;
  TDvbSiStorage& operator=(const TDvbSiStorage&)  = delete;

  void HandleTableEvent(const TSiTable& tbl);
  void HandleNitEvent(const TNitTable& nit);
  void HandleSdtEvent(const TSdtTable& sdt);
  void HandleEitEvent(const TEitTable& eit);
  void HandleTotEvent(const TTotTable& tot);
  void HandleBatEvent(const TBatTable& tot);
  void ProcessNitEventCache(const TNitTable& nit);
  void ProcessSdtEventCache(const TSdtTable& sdt);
  void ProcessEitEventCache(const TEitTable& eit);
  void ProcessBatEventCache(const TBatTable& bat);
  void ProcessNitEventDb(const TNitTable& nit);
  void ProcessSdtEventDb(const TSdtTable& sdt);
  void ProcessEitEventDb(const TEitTable& eit);
  void ProcessBatEventDb(const TBatTable& bat);
  // returns foreign key
  int64_t ProcessNetwork(const TNitTable& nit);
  int64_t ProcessBouquet(const TBatTable& bat);
  int64_t ProcessTransport(const TTransportStream& ts, int64_t network_fk);
  int64_t ProcessService(const TSdtTable& sdt);
  int64_t ProcessEvent(const TEitTable& eit);
  int64_t ProcessEventItem(const std::vector<TMpegDescriptor>& descList, int64_t event_fk);
  // TODO: KSS check if this thread is required.
  void StartMonitorThread();
  void StopMonitorThread();

  bool StartScan(bool isFastScanOn);
  void StopScan();

  void ScanThread(bool isFastScanOn);
  bool ScanHome();

  bool IsFastScanEnabled();
  bool IsBackgroundScanEnabled();
  bool CheckCacheTableCollections(std::vector<std::shared_ptr<TSiTable>>& tables, int timeout);
  bool LoadSettings();
  void ClearCachedTables();
public:


  ~TDvbSiStorage()
  {
    // Empty
  }

  inline uint16_t GetPreferredNetworkId() const
  {
    return PreferredNetworkId;
  }

  inline void SetPreferredNetworkId(uint16_t id)
  {
    PreferredNetworkId = id;
  }

  void EventMonitorThread();
  // TODO: KSS Re-evaluate the singleton instance of TDvbSiStorage.
  static TDvbSiStorage* getInstance()
  {
    std::lock_guard<std::mutex> lock(SiStorageMutex);
    static TDvbSiStorage instance;
    return &instance;
  }

#if 0
    /**
     * Return queue handle for event monitor thread
     *
     * @return rmf_oscal_eventqueue 
     */
    rmf_osal_eventqueue_handle_t getInputQueue();
#endif
  std::vector<std::shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> GetTsListByNetId(uint16_t nId = 0);
  std::vector<std::shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> GetTsListByNetIdCache(uint16_t nId = 0);
  std::vector<std::shared_ptr<TDvbStorageNamespace::EventStruct>> GetEventListByServiceId(uint16_t nId, uint16_t tsId, uint16_t sId);
  std::vector<std::shared_ptr<TDvbStorageNamespace::EventStruct>> GetEventListByServiceIdCache(uint16_t nId, uint16_t tsId, uint16_t sId);
  std::vector<std::shared_ptr<TDvbStorageNamespace::ServiceStruct>> GetServiceListByTsId(uint16_t nId, uint16_t tsId);
  std::vector<std::shared_ptr<TDvbStorageNamespace::ServiceStruct>> GetServiceListByTsIdCache(uint16_t nId, uint16_t tsId);

  TDvbScanStatus GetScanStatus();
  std::vector<std::shared_ptr<TDvbStorageNamespace::InbandTableInfoStruct>> GetInbandTableInfo(std::string& profile);

  // return string configuration file (json)
  std::string GetProfiles();

  //  Set the DVB profile configuration file
  //  @param profiles configuration file in a string (json)
  //  @return true if successfull and false otherwise
  bool SetProfiles(std::string& profiles);
};

#endif
