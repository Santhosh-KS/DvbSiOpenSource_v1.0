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
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA.

#ifndef TDVBSISTORAGE_H
#define TDVBSISTORAGE_H

#include "IDvbStorageSubject.h"
#include "IDvbStorageObserver.h"
#include "TBatTable.h"
#include "TDvbDb.h"
#include "TDvbStorageNamespace.h"
#include "TEitTable.h"
#include "TNitTable.h"
#include "TSdtTable.h"
#include "TSiTable.h"
#include "TTransportStream.h"
#include "TTotTable.h"

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

class TDvbSiStorage : IDvbStorageSubject
{
private:
  const uint32_t& HomeTsFrequency;
  const TDvbStorageNamespace::TModulationMode& HomeTsModulationMode;
  const uint32_t& HomeTsSymbolRate;
  const uint16_t& PreferredNetworkId;
  const std::string& DbFilePath;
  uint32_t BackGroundScanInterval; 
  bool IsFastScan;
  bool CurrentTuneStatus;
  uint8_t TunerIndex;
  TDvbDb StorageDb;
  std::mutex CacheDataMutex;
  std::mutex ScanMutex;
  std::condition_variable ThreadScanCondition;
  std::vector<IDvbStorageObserver*> ObserverVector;

  enum TDVBConstellation {
    DVB_CONSTELLATION_UNDEFINED,
    DVB_CONSTELLATION_QAM16,
    DVB_CONSTELLATION_QAM32,
    DVB_CONSTELLATION_QAM64,
    DVB_CONSTELLATION_QAM128,
    DVB_CONSTELLATION_QAM256
  };

  enum TDvbScanState {
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

  TDvbScanStatus DvbScanStatus;
  std::vector<uint16_t> HomeBouquetsVector;
  uint32_t BarkerFrequency;
  uint32_t BarkerSymbolRate;
  uint32_t BarkerEitTimout; 
  TDvbStorageNamespace::TModulationMode BarkerModulationMode;
#if 0
  std::vector<uint16_t> HomeBouquetsVector;
  uint32_t BarkerFrequency;
  TDvbStorageNamespace::TModulationMode BarkerModulationMode;
  uint32_t BarkerSymbolRate;
  uint32_t BackGroundScanInterval; 
  uint32_t BarkerEitTimout; 
#endif
  std::map<uint16_t, std::shared_ptr<TNitTable>> NitTableMap; 
  void HandleNitEvent(const TNitTable& nit);
  void ProcessNitEventCache(const TNitTable& nit);
  void ProcessNitEventDb(const TNitTable& nit);
  int64_t ProcessNetwork(const TNitTable& nit);
  int64_t ProcessTransport(const TTransportStream& ts, int64_t network_fk);
  
  std::map<std::pair<uint16_t, uint16_t>, std::shared_ptr<TSdtTable>> SdtTableMap;
  void HandleSdtEvent(const TSdtTable& sdt);
  void ProcessSdtEventCache(const TSdtTable& sdt);
  void ProcessSdtEventDb(const TSdtTable& sdt);
  int64_t ProcessService(const TSdtTable& sdt);
  void HandleTotEvent(const TTotTable& tot);

  std::map<uint16_t, std::shared_ptr<TBatTable>> BatTableMap;
  void HandleBatEvent(const TBatTable& bat);
  void ProcessBatEventCache(const TBatTable& bat);
  void ProcessBatEventDb(const TBatTable& bat);
  int64_t ProcessBouquet(const TBatTable& bat);

  std::map<std::tuple<uint16_t, uint16_t, uint16_t, bool>, std::shared_ptr<TEitTable>> EitTableMap;
  void HandleEitEvent(const TEitTable& eit);
  void ProcessEitEventCache(const TEitTable& eit);
  void ProcessEitEventDb(const TEitTable& eit);
  int64_t ProcessEvent(const TEitTable& eit);
  int64_t ProcessEventItem(const std::vector<TMpegDescriptor>& descList, int64_t event_fk);

  // Scan thread related functions
  std::thread ScanThreadObject;
  void ScanThread();
  bool IsFastScanEnabled();
  bool IsBackgroundScanEnabled();
  bool ScanHome();
  bool CheckCacheTableCollections(std::vector<std::shared_ptr<TSiTable>>& tables, int timeout);
  std::vector<std::shared_ptr<TDvbStorageNamespace::TStorageTransportStreamStruct>> GetTsListByNetIdCache(uint16_t nId);
  std::vector<std::shared_ptr<TDvbStorageNamespace::ServiceStruct>> GetServiceListByTsIdCache(uint16_t nId, uint16_t tsId);
  TDvbStorageNamespace::TModulationMode MapModulationMode(TDVBConstellation in);
  void ClearCachedTables();
  bool IsTuneDone();
public:
  TDvbSiStorage(const uint32_t& homeTsFreq, const TDvbStorageNamespace::TModulationMode& modulation,
    const uint32_t& homeTsSymbRate, const uint16_t& prefNetworkId, const std::string& dbFile);
  ~TDvbSiStorage();
  TDvbStorageNamespace::TFileStatus CreateDatabase();
  void HandleTableEvent(const TSiTable& tbl);
  void SetBarkerInfo(const uint32_t& barkerFreq, const TDvbStorageNamespace::TModulationMode& barkMod, const uint32_t& barkSymbRate);
  void UpdateScanType(bool isFastScan);
  void UpdateTuneStatus(bool isTuneSuccess);
  static void ScanThreadInit(void *arg);

  // IDvbStorageSubject
  virtual void RegisterDvbStorageObserver(IDvbStorageObserver* observerObject);
  virtual void RemoveDvbStorageObserver(IDvbStorageObserver* observerObject);
  virtual void NotifyDvbStorageTuneObserver(const uint32_t& freq, const TDvbStorageNamespace::TModulationMode& mod,
    const uint32_t& symbol);
  virtual void NotifyDvbStorageUnTuneObserver();
};
#endif // TDVBSISTORAGE_H
