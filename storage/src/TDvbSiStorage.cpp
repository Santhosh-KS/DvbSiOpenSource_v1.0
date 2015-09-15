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

#include "TDvbSiStorage.h"
#include "oswrap.h"

#include "TCableDeliverySystemDescriptor.h"
#include "TContentDescriptor.h"
#include "TLogicalChannelDescriptor.h"
#include "TMpegDescriptor.h"
#include "TMultilingualNetworkNameDescriptor.h"
#include "TNetworkNameDescriptor.h"
#include "TParentalRatingDescriptor.h"
#include "TServiceDescriptor.h"
#include "TShortEventDescriptor.h"
//#include ""

#include <sstream>
#include <utility>
#include <vector>

using namespace TDvbStorageNamespace;

// private function implementations.

void TDvbSiStorage::HandleNitEvent(const TNitTable& nit)
{
  // TODO: Consider removing one level of handle methods.
  ProcessNitEventCache(nit);
  ProcessNitEventDb(nit);
}

void TDvbSiStorage::ProcessNitEventCache(const TNitTable& nit)
{
  std::lock_guard<std::mutex> lock(CacheDataMutex);

  auto it = NitTableMap.find(nit.GetNetworkId());
  if (it == NitTableMap.end()) {
    OS_LOG(DVB_DEBUG,   "<%s> Adding NIT table to the map. Network id: 0x%x\n", __FUNCTION__, nit.GetNetworkId());
    NitTableMap.insert(std::make_pair(nit.GetNetworkId(), std::make_shared<TNitTable>(nit)));
  }
  else {
    OS_LOG(DVB_DEBUG,   "<%s> NIT already in cache. Network id: %d version: %d\n", __FUNCTION__,nit.GetNetworkId(),nit.GetVersionNumber());
    if (nit.GetVersionNumber() == it->second->GetVersionNumber()) {
      OS_LOG(DVB_DEBUG,   "<%s> NIT version matches (%d). Skipping\n", __FUNCTION__, nit.GetVersionNumber());
    }
    else {
      OS_LOG(DVB_DEBUG,   "<%s> Current version: 0x%x, new version: 0x%x\n", __FUNCTION__, it->second->GetVersionNumber(), nit.GetVersionNumber());
      it->second = std::make_shared<TNitTable>(nit);
    }
  }
}

void TDvbSiStorage::ProcessNitEventDb(const TNitTable& nit)
{
  int64_t network_fk(-1);
  int8_t nitVersion(-1);

  std::string cmdStr("SELECT nit_pk, version FROM Nit WHERE network_id = ");
  std::stringstream ss;
  ss << nit.GetNetworkId();
  cmdStr += ss.str();
  cmdStr += ";";

  int64_t nit_fk = StorageDb.FindPrimaryKey(cmdStr, nitVersion);
  // FOUND
  if (nit_fk > 0) {
    if (nit.GetVersionNumber() == nitVersion) {
      OS_LOG(DVB_DEBUG,   "<%s> NIT version matches (%d). Skipping\n", __FUNCTION__, nit.GetVersionNumber());
    }
    else {
      OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, nitVersion, nit.GetVersionNumber());
      StorageDb.DropTables();
      StorageDb.CreateTables();
      return;
    }
  }
  // NOT FOUND
  if (nit_fk < 1) {
    OS_LOG(DVB_DEBUG,   "<%s> Adding NIT table to database. Network id: %d version: %d\n", __FUNCTION__, nit.GetNetworkId(), nit.GetVersionNumber());
    TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Nit (network_id, version) VALUES (?, ?);"));
    cmd.Bind(1, static_cast<int>(nit.GetNetworkId()));
    cmd.Bind(2, static_cast<int>(nit.GetVersionNumber()));
    cmd.Execute(nit_fk);

    OS_LOG(DVB_DEBUG,   "<%s> nit_fk %ld\n", __FUNCTION__, nit_fk);
    network_fk = ProcessNetwork(nit);
    OS_LOG(DVB_DEBUG,   "<%s> network_fk %ld\n", __FUNCTION__, network_fk);
  }

  // TODO: KSS check if we can put it in 1st int_fk > 0 chk.
  if (nit_fk > 0) {
    StorageDb.InsertDescriptor(static_cast<const char*>("NitDescriptor"), nit_fk, nit.GetNetworkDescriptors());
  }

  const std::vector<TTransportStream>& tsList = nit.GetTransportStreams();
  for (auto it = tsList.begin(); it != tsList.end(); ++it) {
    std::string cmdStr("SELECT nit_transport_pk FROM NitTransport WHERE original_network_id = ");
    std::stringstream ss;
    ss << it->GetOriginalNetworkId();
    cmdStr += ss.str();
    cmdStr += " AND transport_id = ";
    ss.str("");
    ss << it->GetTsId();
    cmdStr += ss.str();
    cmdStr += ";";

    int64_t nit_transport_fk = StorageDb.FindPrimaryKey(cmdStr);
    // NOT FOUND
    if (nit_transport_fk < 1) {
      TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO NitTransport (original_network_id, transport_id, nit_fk) VALUES (?, ?, ?);"));
      cmd.Bind(1, static_cast<int>(it->GetOriginalNetworkId()));
      cmd.Bind(2, static_cast<int>(it->GetTsId()));
      cmd.Bind(3, static_cast<int>(nit_fk));
      cmd.Execute(nit_transport_fk);
      OS_LOG(DVB_DEBUG,   "<%s> Insert nit_transport_fk %ld\n", __FUNCTION__, nit_transport_fk);
      if (network_fk > 0) {
        ProcessTransport(*it, network_fk);
      }
    }
    if (nit_transport_fk > 0) {
      StorageDb.InsertDescriptor(static_cast<const char*>("NitTransportDescriptor"), nit_transport_fk, it->GetTsDescriptors());
    }
  }
  StorageDb.PerformUpdate();
}

int64_t TDvbSiStorage::ProcessNetwork(const TNitTable& nit)
{
  int64_t network_fk(-1);
  std::string networkName;
  std::string iso639languageCode;
  const std::vector<TMpegDescriptor>& descList = nit.GetNetworkDescriptors();

  for (auto it = descList.cbegin(); it != descList.cend(); ++it) {
    const TMpegDescriptor& md = *it;
    if (md.GetDescriptorTag() == TDescriptorTag::NETWORK_NAME_TAG) {
      TNetworkNameDescriptor nnd(md);
      networkName = nnd.GetName();
      break;
    }
    if (md.GetDescriptorTag() == TDescriptorTag::MULTILINGUAL_NETWORK_NAME_TAG) {
      TMultilingualNetworkNameDescriptor  mnnd(md);
      for (int32_t i=0; i < mnnd.GetCount(); i++) {
        iso639languageCode += mnnd.GetLanguageCode(i);
        iso639languageCode += " ";
        networkName += mnnd.GetNetworkName(i);
        networkName += " ";
      }
    }
  }
  TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Network (network_id, version, iso_639_language_code, name) " \
    " VALUES (?, ?, ?, ?);"));
  cmd.Bind(1, static_cast<int>(nit.GetNetworkId()));
  cmd.Bind(2, static_cast<int>(nit.GetVersionNumber()));

  if (iso639languageCode.empty()) {  
    cmd.Bind(3); // Db NULL type
  }
  else {
    cmd.Bind(3, iso639languageCode);
  }
  if (networkName.empty()) {
    cmd.Bind(4);
  }
  else {
    cmd.Bind(4, networkName);
  }
  cmd.Execute(network_fk);
  OS_LOG(DVB_DEBUG,   "<%s> Insert network_fk %ld\n", __FUNCTION__, network_fk);
  return network_fk;
}

int64_t  TDvbSiStorage::ProcessTransport(const TTransportStream& ts, int64_t network_fk)
{
  uint8_t modulation(0);
  uint8_t fecInner(0);
  uint8_t fecOuter(0);
  uint32_t frequency(0);
  uint32_t symbolRate(0);
  int64_t transport_fk(-1);
  const std::vector<TMpegDescriptor>& tsDesc = ts.GetTsDescriptors();

  for (auto it = tsDesc.cbegin(); it != tsDesc.cend(); ++it) {
    const TMpegDescriptor& md = *it;
    if (md.GetDescriptorTag() == TDescriptorTag::CABLE_DELIVERY_TAG) {
      TCableDeliverySystemDescriptor cable(md);
      frequency = cable.GetFrequency();
      modulation = static_cast<uint8_t>(cable.GetModulation());
      symbolRate = cable.GetSymbolRate();
      fecInner = cable.GetFecInner();
      fecOuter = cable.GetFecOuter();
    }
  }
  TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Transport (original_network_id, transport_id, network_fk, " \
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

void TDvbSiStorage::HandleSdtEvent(const TSdtTable& sdt)
{
  ProcessSdtEventCache(sdt);
  ProcessSdtEventDb(sdt);
}

void TDvbSiStorage::ProcessSdtEventCache(const TSdtTable& sdt)
{
  std::lock_guard<std::mutex> lock(CacheDataMutex);
  std::pair<uint16_t, uint16_t> key(sdt.GetOriginalNetworkId(), sdt.GetTableExtensionId());
  auto it = SdtTableMap.find(key);

  if (it == SdtTableMap.end()) {
    OS_LOG(DVB_DEBUG,   "<%s> Adding SDT table to the cache. nid.tsid: 0x%x.0x%x\n", __FUNCTION__, sdt.GetOriginalNetworkId(), sdt.GetTableExtensionId());
    SdtTableMap.insert(std::make_pair(key, std::make_shared<TSdtTable>(sdt)));
  }
  else {
    OS_LOG(DVB_DEBUG,   "<%s> SDT already in cache. nid.tsid: %d.%d\n", __FUNCTION__, sdt.GetOriginalNetworkId(), sdt.GetTableExtensionId());
    if (sdt.GetVersionNumber() == it->second->GetVersionNumber()) {
      OS_LOG(DVB_DEBUG,   "<%s> SDT version matches (0x%x). Skipping\n", __FUNCTION__, sdt.GetVersionNumber());
    }
    else {
      OS_LOG(DVB_DEBUG,   "<%s> Current version: 0x%x, new version: 0x%x\n", __FUNCTION__, it->second->GetVersionNumber(), sdt.GetVersionNumber());
      it->second = std::make_shared<TSdtTable>(sdt);
    }
  }
}

void TDvbSiStorage::ProcessSdtEventDb(const TSdtTable& sdt)
{
  std::string cmdStr("SELECT nit_transport_pk FROM NitTransport WHERE original_network_id = ");
  std::stringstream ss;
  ss << sdt.GetOriginalNetworkId();
  cmdStr += ss.str();
  cmdStr += " AND transport_id = ";

  ss.str("");
  ss << sdt.GetTableExtensionId();
  cmdStr += ss.str();
  cmdStr += ";";

  int64_t nit_transport_fk = StorageDb.FindPrimaryKey(cmdStr);
  if (nit_transport_fk < 1) {
    OS_LOG(DVB_DEBUG,   "<%s> nit_transport_fk %ld\n", __FUNCTION__, nit_transport_fk);
    return;
  }

  const std::vector<TSdtService>&  serviceList = sdt.GetServices();
  for (auto it = serviceList.begin(); it != serviceList.end(); ++it) {
    const TSdtService& service = (*it);
    std::string cmdStr("SELECT s.sdt_pk, version FROM Sdt s INNER JOIN NitTransport nt " \
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
    // FOUND 
    if (sdt_fk > 0) {
      if (sdt.GetVersionNumber() == sdtVersion) {
        OS_LOG(DVB_DEBUG,   "<%s> Sdt version matches (%d). Skipping\n", __FUNCTION__, sdt.GetVersionNumber());
      }
      else {
        OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, sdtVersion, sdt.GetVersionNumber());
        // Sdt version change
        TDvbDb::TTransaction versionChange(StorageDb);
// TODO: KSS why {} braces here?
        {
          TDvbDb::TCommand cmd(StorageDb, std::string("DELETE FROM Sdt WHERE service_id = ? AND version != ? AND "  \
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
        StorageDb.SqlCommand(std::string("DELETE FROM SdtDescriptor WHERE fkey NOT IN (SELECT DISTINCT sdt_pk FROM Sdt);"));

// TODO: KSS why {} braces here?
        {
          TDvbDb::TCommand cmd(StorageDb, std::string("DELETE FROM Eit WHERE network_id = ? AND transport_id = ? AND service_id = ?;"));
          cmd.Bind(1, static_cast<int>(sdt.GetOriginalNetworkId()));
          cmd.Bind(2, static_cast<int>(sdt.GetTableExtensionId()));
          cmd.Bind(3, static_cast<int>(service.GetServiceId()));
          cmd.Execute();
        }

        StorageDb.SqlCommand(std::string("DELETE FROM EitDescriptor WHERE fkey NOT IN (SELECT DISTINCT eit_pk FROM Eit);"));
        versionChange.CommitSqlStatements();
        sdt_fk = StorageDb.FindPrimaryKey(cmdStr, sdtVersion);
        ProcessService(sdt);
      }
    }
    // NOT FOUND
    if (sdt_fk < 1) {
      OS_LOG(DVB_DEBUG,   "<%s> Adding SDT table to database. nid.tsid: %d.%d\n", __FUNCTION__, sdt.GetOriginalNetworkId(), sdt.GetTableExtensionId());
      TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Sdt (service_id, nit_transport_fk, version, schedule, " \
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
    if (sdt_fk > 0) {
      StorageDb.InsertDescriptor(static_cast<const char*>("SdtDescriptor"), sdt_fk, service.GetServiceDescriptors());
    }
  } // for
  StorageDb.PerformUpdate();
}

int64_t  TDvbSiStorage::ProcessService(const TSdtTable& sdt)
{
  int64_t service_fk(-1);
  std::string cmdStr("SELECT transport_pk FROM Transport WHERE original_network_id = ");
  std::stringstream ss;
  ss << sdt.GetOriginalNetworkId();
  cmdStr += ss.str();
  cmdStr += " AND transport_id = ";

  ss.str("");
  ss << sdt.GetTableExtensionId();
  cmdStr += ss.str();
  cmdStr += ";";

  int64_t transport_fk = StorageDb.FindPrimaryKey(cmdStr);
  if (transport_fk < 1) {
    OS_LOG(DVB_DEBUG,   "<%s> transport_fk = %ld\n", __FUNCTION__, transport_fk);
    return service_fk;
  }

  const std::vector<TSdtService>&  serviceList = sdt.GetServices();
  for (auto it = serviceList.begin(); it != serviceList.end(); ++it) {
    const TSdtService& service = (*it);
    std::string cmdStr("SELECT s.service_pk, s.version FROM Service s INNER JOIN Transport t " \
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

    int8_t serviceVersion(-1);
    int64_t service_fk = StorageDb.FindPrimaryKey(cmdStr, serviceVersion);
    // FOUND
    if (service_fk > 0) {
      if (sdt.GetVersionNumber() == serviceVersion)  {
        OS_LOG(DVB_DEBUG,   "<%s> Sdt version matches (%d). Skipping\n", __FUNCTION__, sdt.GetVersionNumber());
      }
      else {
        OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, serviceVersion, sdt.GetVersionNumber());
        // Sdt version change
        TDvbDb::TTransaction versionChange(StorageDb);

        {
          TDvbDb::TCommand cmd(StorageDb, std::string("DELETE FROM Service WHERE service_id = ? AND version != ? AND " \
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

        StorageDb.SqlCommand(std::string("DELETE FROM ServiceComponent WHERE fkey NOT IN (SELECT DISTINCT service_pk FROM Service);"));

        {
          TDvbDb::TCommand cmd(StorageDb, std::string("DELETE FROM Event WHERE network_id = ? AND transport_id = ? AND service_id = ?;"));
          cmd.Bind(1, static_cast<int>(sdt.GetOriginalNetworkId()));
          cmd.Bind(2, static_cast<int>(sdt.GetTableExtensionId()));
          cmd.Bind(3, static_cast<int>(service.GetServiceId()));
          cmd.Execute();
        }

        StorageDb.SqlCommand(std::string("DELETE FROM EventItem WHERE event_fk NOT IN (SELECT DISTINCT event_pk FROM Event);"));
        StorageDb.SqlCommand(std::string("DELETE FROM EventComponent WHERE fkey NOT IN (SELECT DISTINCT event_pk FROM Event);"));
        versionChange.CommitSqlStatements();
        service_fk = StorageDb.FindPrimaryKey(cmdStr, serviceVersion);
      }
    }
    // NOT found
    if (service_fk < 1) {
      uint8_t serviceType(0);
      uint16_t lcn(0);
      std::string serviceName;
      std::string providerName;
      const std::vector<TMpegDescriptor>& serveDesc = service.GetServiceDescriptors();

      for (auto it = serveDesc.cbegin(); it != serveDesc.cend(); ++it) {
        const TMpegDescriptor& md = *it;
        if (md.GetDescriptorTag() == TDescriptorTag::LOGICAL_CHANNEL_TAG) {
          TLogicalChannelDescriptor lcd(md);
          lcn = lcd.GetLogicalChannelNumber(0);
        }
        if (md.GetDescriptorTag() == TDescriptorTag::SERVICE_TAG) {
          TServiceDescriptor sd(md);
          serviceType = sd.GetServiceType();
          serviceName = sd.GetServiceName();
          providerName = sd.GetServiceProviderName();
        }
      }

      TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Service (service_id, transport_fk, version, "     \
        "service_type, logical_channel_number, running, scrambled, schedule, "   \
        "present_following, iso_639_language_code, service_name, provider_name)" \
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"));
      cmd.Bind(1, static_cast<int>(service.GetServiceId()));

      if (transport_fk > 0) {
        cmd.Bind(2, static_cast<long long int>(transport_fk));
      }
      else {
        cmd.Bind(2);
      }
            
      cmd.Bind(3, static_cast<int>(sdt.GetVersionNumber())); 
      cmd.Bind(4, static_cast<int>(serviceType));

      if (lcn > 0) { 
        cmd.Bind(5, static_cast<int>(lcn));
      }
      else {
        cmd.Bind(5);
      }

      cmd.Bind(6, static_cast<int>(service.GetRunningStatus())); 
      cmd.Bind(7, static_cast<int>(service.IsScrambled()));
      cmd.Bind(8, static_cast<int>(service.IsEitSchedFlagSet()));
      cmd.Bind(9, static_cast<int>(service.IsEitPfFlagSet()));
      cmd.Bind(10);

      if (serviceName.empty()) {
        cmd.Bind(11);
      }
      else {
        cmd.Bind(11, serviceName);
      }

      if (providerName.empty()) {
        cmd.Bind(12);
      }
      else {
        cmd.Bind(12, providerName);
      }

      cmd.Execute(service_fk);

      OS_LOG(DVB_DEBUG,   "<%s> Insert service_fk = %ld\n", __FUNCTION__, service_fk);
    }

    if (service_fk > 0) {
      StorageDb.InsertComponent("Service", service_fk, service.GetServiceDescriptors());
    }
  } // for
  return  service_fk;
}

void TDvbSiStorage::HandleTotEvent(const TTotTable& tot)
{
  if (tot.GetTableId() == TTableId::TABLE_ID_TDT) {
    //OS_LOG(DVB_DEBUG,   "  TDT: Time and Date Table\n");
    time_t newTime = tot.GetUtcTime();
    if (newTime > 0) {
      OS_LOG(DVB_DEBUG,   "<%s> newTime = %ld\n", __FUNCTION__, newTime);
      struct timeval timeVal;
      timeVal.tv_sec = newTime;
      int ret = settimeofday(&timeVal, NULL);
      if (ret < 0) {
        OS_LOG(DVB_ERROR,   "<%s> settimeofday(%ld) failed, %s\n", __FUNCTION__, newTime, strerror(errno));
      }
      StorageDb.UpdateTotStatus(true);
    }
    else {
      OS_LOG(DVB_ERROR,   "<%s> MjdToDate() failed: %s\n", __FUNCTION__, strerror(errno));
    }
  }
  else if (tot.GetTableId() == TTableId::TABLE_ID_TDT) {
    //OS_LOG(DVB_DEBUG,   "  TOT: Time Offset Table\n");
    //OS_LOG(DVB_DEBUG,   "  TOT: Time Offset Table\n");
  }
  //OS_LOG(DVB_DEBUG,   "\tUTC time       : %" PRId64"\n", tot.GetUtcTimeBcd());
}

void TDvbSiStorage::HandleBatEvent(const TBatTable& bat)
{
  ProcessBatEventCache(bat);
  ProcessBatEventDb(bat);
}

void TDvbSiStorage::ProcessBatEventCache(const TBatTable& bat)
{
  std::lock_guard<std::mutex> lock(CacheDataMutex);
  auto it = BatTableMap.find(bat.GetBouquetId());
  if (it == BatTableMap.end()) {
    OS_LOG(DVB_DEBUG,   "<%s> Adding BAT table to the map. Bouquet id: 0x%x\n", __FUNCTION__, bat.GetBouquetId());
    BatTableMap.insert(std::make_pair(bat.GetBouquetId(), std::make_shared<TBatTable>(bat)));
  }
  else {
    OS_LOG(DVB_DEBUG,   "<%s> BAT already in cache. Bouquet id: %d version: %d\n", __FUNCTION__, bat.GetBouquetId(),bat.GetVersionNumber());
    if (bat.GetVersionNumber() == it->second->GetVersionNumber()) {
      OS_LOG(DVB_DEBUG,   "<%s> BAT version matches (%d). Skipping\n", __FUNCTION__, bat.GetVersionNumber());
    }
    else {
      OS_LOG(DVB_DEBUG,   "<%s> Current version: 0x%x, new version: 0x%x\n", __FUNCTION__, it->second->GetVersionNumber(), bat.GetVersionNumber());
      it->second = std::make_shared<TBatTable>(bat);
    }
  }
}

void TDvbSiStorage::ProcessBatEventDb(const TBatTable& bat)
{
  int64_t network_fk(-1);

  if (PreferredNetworkId != 0) {
    std::string cmdStr("SELECT network_pk FROM Network WHERE network_id = ");

    std::stringstream ss;
    ss << PreferredNetworkId;
    cmdStr += ss.str();
    cmdStr += ";";

    network_fk = StorageDb.FindPrimaryKey(cmdStr);
    if (network_fk < 1) {
      OS_LOG(DVB_DEBUG,   "<%s> network_fk %ld\n", __FUNCTION__, network_fk);
      return;
    }  
  }

  std::string cmdStr("SELECT bat_pk, version FROM Bat WHERE bouquet_id = ");

  std::stringstream ss;
  ss << bat.GetBouquetId();
  cmdStr += ss.str();
  cmdStr += ";";

  int8_t batVersion(-1);
  int64_t bat_fk = StorageDb.FindPrimaryKey(cmdStr, batVersion);
  // FOUND
  if (bat_fk > 0) {
    if (bat.GetVersionNumber() == batVersion) {
      OS_LOG(DVB_DEBUG,   "<%s> BAT version matches (%d). Skipping\n", __FUNCTION__, bat.GetVersionNumber());
    }
    else {
      OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, batVersion, bat.GetVersionNumber());
      // Bat version change
      TDvbDb::TTransaction versionChange(StorageDb);
      TDvbDb::TCommand cmd(StorageDb, std::string("DELETE FROM Bat WHERE bouquet_id = ? AND version != ?;"));
      cmd.Bind(1, static_cast<int>(bat.GetBouquetId()));
      cmd.Bind(2, static_cast<int>(bat.GetVersionNumber()));
      cmd.Execute();

      StorageDb.SqlCommand(std::string("DELETE FROM BatDescriptor WHERE fkey NOT IN (SELECT DISTINCT bat_pk FROM Bat);"));
      StorageDb.SqlCommand(std::string("DELETE FROM BatTransport WHERE bat_fk NOT IN (SELECT DISTINCT bat_pk FROM Bat);"));
      StorageDb.SqlCommand(std::string("DELETE FROM BatTransportDescriptor WHERE fkey NOT IN " \
        " (SELECT DISTINCT bat_transport_pk FROM BatTransport);"));
      versionChange.CommitSqlStatements();
      ProcessBouquet(bat);
      bat_fk = StorageDb.FindPrimaryKey(cmdStr, batVersion);
    }
  }
  // NOT FOUND   
  if (bat_fk < 1) {
    OS_LOG(DVB_DEBUG,   "<%s> Adding BAT table to the database. Bouquet id: %d version: %d\n", __FUNCTION__, bat.GetBouquetId(), bat.GetVersionNumber());
    TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Bat (bouquet_id, version) VALUES (?, ?);"));
    cmd.Bind(1, static_cast<int>(bat.GetBouquetId()));
    cmd.Bind(2, static_cast<int>(bat.GetVersionNumber()));
    cmd.Execute(bat_fk);

    OS_LOG(DVB_DEBUG,   "<%s> Insert bat_fk %ld\n", __FUNCTION__, bat_fk);
    ProcessBouquet(bat);
  }
  if (bat_fk > 0) {
    StorageDb.InsertDescriptor(static_cast<const char*>("BatDescriptor"), bat_fk, bat.GetBouquetDescriptors());
  }

  const std::vector<TTransportStream>& tsList = bat.GetTransportStreams();
  for (auto it = tsList.begin(); it != tsList.end(); ++it) {
    // NOTE: Order dependency of NitTransport entries existing before BatTransport entries.
    std::string cmdStr("SELECT nit_transport_pk FROM NitTransport WHERE original_network_id = ");
    std::stringstream ss;
    ss << it->GetOriginalNetworkId();
    cmdStr += ss.str();
    cmdStr += " AND transport_id = ";

    ss.str("");
    ss << it->GetTsId();
    cmdStr += ss.str();
    cmdStr += ";";

    int64_t nit_transport_fk = StorageDb.FindPrimaryKey(cmdStr);
    if (nit_transport_fk > 0) {
      std::string cmdStr("SELECT bat_transport_pk FROM BatTransport WHERE original_network_id = ");
      std::stringstream ss;
      ss << it->GetOriginalNetworkId();
      cmdStr += ss.str();
      cmdStr += " AND transport_id = ";

      ss.str("");
      ss << it->GetTsId();
      cmdStr += ss.str();
      cmdStr += ";";

      int64_t bat_transport_fk = StorageDb.FindPrimaryKey(cmdStr);
      // NOT FOUND
      if (bat_transport_fk < 1) { 
        TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO BatTransport (original_network_id, transport_id, bat_fk, " \
          "nit_transport_fk) VALUES (?, ?, ?, ?);"));
        cmd.Bind(1, static_cast<int>(it->GetOriginalNetworkId()));
        cmd.Bind(2, static_cast<int>(it->GetTsId()));
        cmd.Bind(3, static_cast<long long int>(bat_fk));
        cmd.Bind(4, static_cast<long long int>(nit_transport_fk));
        cmd.Execute(bat_transport_fk);

        OS_LOG(DVB_DEBUG,   "<%s> Insert bat_transport_fk %ld\n", __FUNCTION__, bat_transport_fk);
      }
      if (bat_transport_fk > 0) {
        StorageDb.InsertDescriptor(static_cast<const char*>("BatTransportDescriptor"), bat_transport_fk, it->GetTsDescriptors());
      }
    }
  }
  StorageDb.PerformUpdate();
}

int64_t TDvbSiStorage::ProcessBouquet(const TBatTable& bat)
{
  std::string cmdStr("SELECT bouquet_pk, version FROM Bouquet WHERE bouquet_id = ");
  std::stringstream ss;
  ss << bat.GetBouquetId();
  cmdStr += ss.str();
  cmdStr += ";";

  int8_t bouquetVersion(-1);
  int64_t bouquet_fk = StorageDb.FindPrimaryKey(cmdStr, bouquetVersion);
  // FOUND
  if (bouquet_fk > 0) {
    if (bat.GetVersionNumber() == bouquetVersion) {
      OS_LOG(DVB_DEBUG,   "<%s> BAT version matches (%d). Skipping\n", __FUNCTION__, bat.GetVersionNumber());
    }
    else {
      OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, bouquetVersion, bat.GetVersionNumber());
      // Bat version change
      TDvbDb::TTransaction versionChange(StorageDb);
      TDvbDb::TCommand cmd(StorageDb, std::string("DELETE FROM Bouquet WHERE bouquet_id = ? AND version != ?;"));
      cmd.Bind(1, static_cast<int>(bat.GetVersionNumber()));
      cmd.Bind(2, static_cast<int>(bat.GetVersionNumber())); 
      cmd.Execute();
      versionChange.CommitSqlStatements();
      bouquet_fk = StorageDb.FindPrimaryKey(cmdStr, bouquetVersion);
    }
  }
  // NOT FOUND
  if (bouquet_fk < 1) {
    std::string networkName;
    std::string iso639languageCode;

    // Either a Network Name descriptor or a Multilingual Network descriptor exists. 
    const std::vector<TMpegDescriptor>& descList = bat.GetBouquetDescriptors();
    for (auto it = descList.cbegin(); it != descList.cend(); ++it) {
      const TMpegDescriptor& md = *it;
      if (md.GetDescriptorTag() == TDescriptorTag::NETWORK_NAME_TAG) {
        TNetworkNameDescriptor nnd(md);
        networkName += nnd.GetName();
      }
      if (md.GetDescriptorTag() == TDescriptorTag::MULTILINGUAL_NETWORK_NAME_TAG) {
        TMultilingualNetworkNameDescriptor  mnnd(md);
        for (int32_t i=0; i < mnnd.GetCount(); i++) {
          iso639languageCode += mnnd.GetLanguageCode(i);
          iso639languageCode += " ";
          networkName += mnnd.GetNetworkName(i);
          networkName += " ";
        }
      }
    }
 
    OS_LOG(DVB_DEBUG,   "<%s> bouquet_id: %d version: %d networkName: %s iso: %s\n",
      __FUNCTION__, bat.GetBouquetId(), bat.GetVersionNumber(),
      networkName.c_str(), iso639languageCode.c_str());
 
    TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Bouquet (bouquet_id, version, iso_639_language_code, name) " \
      " VALUES (?, ?, ?, ?);"));
    cmd.Bind(1, static_cast<int>(bat.GetBouquetId()));
    cmd.Bind(2, static_cast<int>(bat.GetVersionNumber()));
    if (iso639languageCode.empty()) {  
      cmd.Bind(3);
    }
    else {
      cmd.Bind(3, iso639languageCode);
    }
 
    if (networkName.empty()) {
      cmd.Bind(4);
    }
    else {
      cmd.Bind(4, networkName);
    }
    cmd.Execute(bouquet_fk);
    OS_LOG(DVB_DEBUG,   "<%s> Insert bouquet_fk %ld\n", __FUNCTION__, bouquet_fk);
    // Set bouquet_fk foreign key in Transport.
    if (bouquet_fk > 0) {
      const std::vector<TTransportStream>& tsList = bat.GetTransportStreams();
      for (auto it = tsList.begin(); it != tsList.end(); ++it)  {
        std::string cmdStr("SELECT transport_pk FROM Transport WHERE original_network_id = ");
         std::stringstream ss;
        ss << it->GetOriginalNetworkId();
        cmdStr += ss.str();
        cmdStr += " AND transport_id = ";
 
        ss.str("");
        ss << it->GetTsId();
        cmdStr += ss.str();
        cmdStr += ";";
 
        int64_t transport_fk = StorageDb.FindPrimaryKey(cmdStr);
        if (transport_fk > 0) {
          TDvbDb::TCommand cmd(StorageDb, std::string("UPDATE Transport SET bouquet_fk = ? WHERE transport_pk = ?;"));
          cmd.Bind(1, static_cast<long long int>(bouquet_fk));
          cmd.Bind(2, static_cast<long long int>(transport_fk));
          cmd.Execute();
          // no change schedule an update 
          if (StorageDb.GetNumberOfRowsModified() == 0) {
            std::string cmdStr("UPDATE Transport SET bouquet_fk = ");
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

void TDvbSiStorage::HandleEitEvent(const TEitTable& eit)
{
  ProcessEitEventCache(eit);
  ProcessEitEventDb(eit);
}

void TDvbSiStorage::ProcessEitEventCache(const TEitTable& eit)
{
  std::lock_guard<std::mutex> lock(CacheDataMutex);

  bool isPf(false);
  TTableId tableId = eit.GetTableId();
  if (tableId == TTableId::TABLE_ID_EIT_PF || tableId == TTableId::TABLE_ID_EIT_PF_OTHER) {
    isPf = true;
  }

  std::tuple<uint16_t, uint16_t, uint16_t, bool> key(eit.GetNetworkId(), eit.GetTsId(), eit.GetTableExtensionId(), isPf);
  auto it = EitTableMap.find(key);
  if (it == EitTableMap.end()) {
    OS_LOG(DVB_DEBUG,   "<%s> Adding EIT table to the cache. nid.tsid.sid: 0x%x.0x%x.0x%x\n",
      __FUNCTION__, eit.GetNetworkId(), eit.GetTsId(), eit.GetTableExtensionId());

    EitTableMap.insert(std::make_pair(key, std::make_shared<TEitTable>(eit)));
  }
  else {
    OS_LOG(DVB_DEBUG, "<%s> EIT already in cache. nid.tsid.sid: 0x%x.0x%x.0x%x\n", __FUNCTION__,
      eit.GetNetworkId(), eit.GetTsId(), eit.GetTableExtensionId());
    if (eit.GetVersionNumber() == it->second->GetVersionNumber()) {
      OS_LOG(DVB_DEBUG,   "<%s> EIT version matches (0x%x). Skipping\n", __FUNCTION__, eit.GetVersionNumber());
    }
    else {
      OS_LOG(DVB_DEBUG,   "<%s> Current version: 0x%x, new version: 0x%x\n", __FUNCTION__, it->second->GetVersionNumber(), eit.GetVersionNumber());
      it->second = std::make_shared<TEitTable>(eit);
    }
  }
}

void TDvbSiStorage::ProcessEitEventDb(const TEitTable& eit)
{
  const std::vector<TEitEvent>&  eventList = eit.GetEvents();
  for (auto it = eventList.begin(); it != eventList.end(); ++it) {
    const TEitEvent& event = (*it);
    std::string cmdStr("SELECT eit_pk, version FROM Eit WHERE network_id = ");
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

    int8_t eitVersion(-1);
    int64_t eit_fk = StorageDb.FindPrimaryKey(cmdStr, eitVersion);
    // FOUND
    if (eit_fk > 0) {
      if (eit.GetVersionNumber() == eitVersion) {
        OS_LOG(DVB_DEBUG,   "<%s> EIT version matches (%d). Skipping\n", __FUNCTION__, eit.GetVersionNumber());
      }
      else {
        OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, eitVersion, eit.GetVersionNumber());
        // Eit version change
        TDvbDb::TTransaction versionChange(StorageDb);
        TDvbDb::TCommand cmd(StorageDb, std::string("DELETE FROM Eit WHERE network_id = ? AND transport_id = ? AND " \
          "event_id = ? AND version != ?;"));
        cmd.Bind(1, static_cast<int>(eit.GetNetworkId()));
        cmd.Bind(2, static_cast<int>(eit.GetTsId()));
        cmd.Bind(3, static_cast<int>(eit.GetTableExtensionId()));
        cmd.Bind(4, static_cast<int>(eit.GetVersionNumber()));
        cmd.Execute();

        StorageDb.SqlCommand(std::string("DELETE FROM EitDescriptor WHERE fkey NOT IN (SELECT DISTINCT eit_pk FROM Eit);"));
        versionChange.CommitSqlStatements();
        eit_fk = StorageDb.FindPrimaryKey(cmdStr, eitVersion);
      }
    }
    // NOT FOUND
    if (eit_fk < 1) {
      OS_LOG(DVB_DEBUG,   "<%s> Adding EIT table to the database. nid.tsid.sid: 0x%x.0x%x.0x%x\n",
         __FUNCTION__, eit.GetNetworkId(), eit.GetTsId(), eit.GetTableExtensionId());
      int present_following(0);
      TTableId tableId = eit.GetTableId();
      if (tableId == TTableId::TABLE_ID_EIT_PF || tableId == TTableId::TABLE_ID_EIT_PF_OTHER) {
        present_following = 1;
      }
      TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Eit (network_id, transport_id, service_id, event_id, "   \
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
    if (eit_fk > 0) {
      StorageDb.InsertDescriptor(static_cast<const char*>("EitDescriptor"), eit_fk, it->GetEventDescriptors());
    }
  } 
  // Process eit data for Event Db Table
  ProcessEvent(eit);
}

int64_t  TDvbSiStorage::ProcessEvent(const TEitTable& eit)
{
  int64_t event_fk(-1);
  const std::vector<TEitEvent>&  eventList = eit.GetEvents();

  for (auto it = eventList.begin(); it != eventList.end(); ++it) {
    const TEitEvent& event = (*it);
    std::string cmdStr("SELECT event_pk, version FROM Event WHERE network_id = ");

    std::stringstream ss;
    ss << eit.GetNetworkId();
    cmdStr += ss.str();
    cmdStr += " AND transport_id = ";
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

    int8_t eventVersion(-1);
    event_fk = StorageDb.FindPrimaryKey(cmdStr, eventVersion);
    // FOUND
    if (event_fk > 0) {
      if (eit.GetVersionNumber() == eventVersion) {
        OS_LOG(DVB_DEBUG,   "<%s> EIT version matches (%d). Skipping\n", __FUNCTION__, eit.GetVersionNumber());
      }
      else {
        OS_LOG(DVB_DEBUG,   "<%s> Current version: %d, new version: %d\n", __FUNCTION__, eventVersion, eit.GetVersionNumber());
        // Event version change
        TDvbDb::TTransaction versionChange(StorageDb);

        TDvbDb::TCommand cmd(StorageDb, std::string("DELETE FROM Event WHERE network_id = ? AND transport_id = ? AND " \
          "event_id = ? AND version != ?;"));
        cmd.Bind(1, static_cast<int>(eit.GetNetworkId()));
        cmd.Bind(2, static_cast<int>(eit.GetTsId()));
        cmd.Bind(3, static_cast<int>(eit.GetTableExtensionId()));
        cmd.Bind(4, static_cast<int>(eit.GetVersionNumber()));
        cmd.Execute();

        StorageDb.SqlCommand(std::string("DELETE FROM EventItem WHERE event_fk NOT IN (SELECT DISTINCT event_pk FROM Event);"));
        StorageDb.SqlCommand(std::string("DELETE FROM EventComponent WHERE fkey NOT IN (SELECT DISTINCT event_pk FROM Event);"));
        versionChange.CommitSqlStatements();
        event_fk = StorageDb.FindPrimaryKey(cmdStr, eventVersion);
      }
    }
    // NOT FOUND
    if (event_fk < 1) {
      std::string parentalRating;
      std::string content;
      const std::vector<TMpegDescriptor>& eventDesc = event.GetEventDescriptors();
      for (auto it = eventDesc.cbegin(); it != eventDesc.cend(); ++it) {
        const TMpegDescriptor& md = *it;
        if (md.GetDescriptorTag() == TDescriptorTag::CONTENT_DESCRIPTOR_TAG) {
          TContentDescriptor cd(md);
          for (int i=0; i < cd.GetCount(); i++) {
            std::string conStr("(");
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
        if (md.GetDescriptorTag() == TDescriptorTag::PARENTAL_RATING_TAG) {
          TParentalRatingDescriptor prd(md);
          for (int i=0; i < prd.GetCount(); i++) {
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
      if (tableId == TTableId::TABLE_ID_EIT_PF || tableId == TTableId::TABLE_ID_EIT_PF_OTHER) {
        present_following = 1;
      }

      TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO Event (network_id, transport_id, service_id, event_id, " \
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

      if (parentalRating.empty()) {
        cmd.Bind(11);
      }
      else {
        cmd.Bind(11, parentalRating);
      }

      if (content.empty()) {
        cmd.Bind(12);
      }
      else {
        cmd.Bind(12, content);
      }
      cmd.Execute(event_fk);
      OS_LOG(DVB_DEBUG,   "<%s> Insert event_fk = %ld\n", __FUNCTION__, event_fk);
      if (event_fk > 0) {
        ProcessEventItem(event.GetEventDescriptors(), event_fk);
        StorageDb.InsertComponent("Event", event_fk, event.GetEventDescriptors());
      }
    }
  }
  return  event_fk;
}

int64_t  TDvbSiStorage::ProcessEventItem(const std::vector<TMpegDescriptor>& descList, int64_t event_fk)
{
  int64_t eventItem_fk(-1);
  std::string iso_639_language_code;
  std::string title;
  std::string description;

  for (auto it = descList.cbegin(); it != descList.cend(); ++it) {
    const TMpegDescriptor& md = *it;
    if (md.GetDescriptorTag() == TDescriptorTag::SHORT_EVENT_TAG) {
      TShortEventDescriptor sed(md);
      iso_639_language_code = sed.GetLanguageCode();
      title = sed.GetEventName();
      description = sed.GetText();

      TDvbDb::TCommand cmd(StorageDb, std::string("INSERT OR IGNORE INTO EventItem (event_fk, iso_639_language_code, title, " \
        "description) VALUES (?, ?, ?, ?);"));
// TODO: KSS this error we were getting without cmd.Bind(byte, static_cast<int32_t>(event_fk)); 
//src/TDvbSiStorage.cpp:2086:36: error: call of overloaded ‘Bind(int32_t&, int64_t&)’ is ambiguous
//src/TDvbSiStorage.cpp:2086:36: note: candidates are:
//./include/TDvbDb.h:386:13: note: int32_t TDvbDb::TCommand::Bind(int32_t, int)
//./include/TDvbDb.h:392:13: note: int32_t TDvbDb::TCommand::Bind(int32_t, long long int)

      int32_t byte(1);
      cmd.Bind(byte, static_cast<int32_t>(event_fk));
      if (iso_639_language_code.empty()) {
        cmd.Bind(2);
      }
      else {
        cmd.Bind(2, iso_639_language_code);
      }
      if (title.empty()) {
        cmd.Bind(3);
      }
      else {
        cmd.Bind(3, title);
      }
      if (description.empty()) {
        cmd.Bind(4);
      }
      else {
        cmd.Bind(4, description);
      }
      cmd.Execute(eventItem_fk);
      OS_LOG(DVB_DEBUG,   "<%s> Insert eventItem_fk: %ld\n", __FUNCTION__, eventItem_fk);
    }
  }
  return  eventItem_fk; 
}

// Scan thread related methods
void TDvbSiStorage::ScanThread(bool bFast)
{
  OS_LOG(DVB_INFO,   "%s(): created(%d)\n", __FUNCTION__, bFast);
  while (true) {
    if (bFast) {
      DvbScanStatus.ScanState = TDvbScanState::SCAN_IN_PROGRESS_FAST;
      if (!IsFastScanEnabled()) {
        OS_LOG(DVB_ERROR,   "%s(): IsFastScanEnabled() failed\n", __FUNCTION__);
        DvbScanStatus.ScanState = TDvbScanState::SCAN_FAILED;
      }
      else {
        // It's enough to run the fast scan only once
        bFast = false;
        continue;
      }
    }
    else {
      DvbScanStatus.ScanState = TDvbScanState::SCAN_IN_PROGRESS_BKGD;
      if (!IsBackgroundScanEnabled()) {
        OS_LOG(DVB_ERROR,   "%s(): IsBackgroundScanEnabled() failed\n", __FUNCTION__);
        DvbScanStatus.ScanState = TDvbScanState::SCAN_FAILED;
      }
      else {
        OS_LOG(DVB_INFO,   "%s(): scan completed successfully\n", __FUNCTION__);
        DvbScanStatus.ScanState = TDvbScanState::SCAN_COMPLETED;
      }
    }
    StorageDb.Audits();
    // Let's check if we need to stop the scan
    {
       std::unique_lock<std::mutex> lk(ScanMutex);
       // TODO: Handle spurious wake-ups
       if (ThreadScanCondition.wait_for(lk, std::chrono::seconds(bFast ? 30 : BackGroundScanInterval)) == std::cv_status::no_timeout) {
         OS_LOG(DVB_INFO,   "%s(): stopping\n", __FUNCTION__);
         DvbScanStatus.ScanState = TDvbScanState::SCAN_STOPPED;
// TODO: KSS Terminate this thread properly. Else it becomes a zombie and haunts you.
         return;
       }
     }
   }
}

bool TDvbSiStorage::IsFastScanEnabled()
{
  OS_LOG(DVB_INFO, "%s: Started\n", __FUNCTION__);
  DvbScanStatus.TsList.clear();
  if (!ScanHome()) {
    OS_LOG(DVB_ERROR,   "%s: ScanHome() failed\n", __FUNCTION__);
    return false;
  }
//    rmf_DvbTuner tuner;

  std::vector<std::shared_ptr<TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(PreferredNetworkId);
  for (auto it = tsList.begin(), end = tsList.end(); it != end; ++it) {
    TDvbSiTableStatus status;
    // collect SDTa & EITa pf
    TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
    sdt->SetOriginalNetworkId((*it)->NetworkId);
    std::vector<std::shared_ptr<TSiTable>> tables;
    tables.emplace_back(sdt);
    std::vector<std::shared_ptr<ServiceStruct>> serviceList = GetServiceListByTsIdCache((*it)->NetworkId, (*it)->TransportStreamId);
    for (auto srv = serviceList.begin(), end = serviceList.end(); srv != end; ++srv) {
      OS_LOG(DVB_DEBUG,   "%s:%d: Adding EITpf(0x%x.0x%x.0x%x) to the list\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId, (*srv)->ServiceId);

      TEitTable* eit = new TEitTable((uint8_t)TTableId::TABLE_ID_EIT_PF, (*srv)->ServiceId, 0, true);
      eit->SetNetworkId((*it)->NetworkId);
      eit->SetTsId((*it)->TransportStreamId);
      tables.emplace_back(eit);
    }
    if (IsFastScan && CheckCacheTableCollections(tables, 0)) {
      OS_LOG(DVB_INFO, "%s:%d: SDTa(0x%x.0x%x) & EITa pf already received. Skipping.\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
      status.SdtAcquired = true;
      status.EitPfAcquired = true;
    }
    else {
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
      if (CheckCacheTableCollections(tables, SDT_TIMEOUT > EIT_PF_TIMEOUT ? SDT_TIMEOUT : EIT_PF_TIMEOUT)) {
        OS_LOG(DVB_INFO,   "%s:%d: SDT(0x%x.0x%x) & EITs pf received\n",
          __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
        status.SdtAcquired = true;
        status.EitPfAcquired = true;
      }
      else {
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

bool TDvbSiStorage::IsBackgroundScanEnabled()
{
  OS_LOG(DVB_INFO,   "%s: Started\n", __FUNCTION__);
  DvbScanStatus.TsList.clear();

  if (!ScanHome()) {
    OS_LOG(DVB_ERROR,   "%s: ScanHome() failed\n", __FUNCTION__);
    return false;
  }
// TODO: KSS
//    rmf_DvbTuner tuner;
  std::vector<std::shared_ptr<TSiTable>> fullEitSchedule;
  std::vector<std::shared_ptr<TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(PreferredNetworkId);
  for (auto it = tsList.begin(), end = tsList.end(); it != end; ++it) {
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

    std::vector<std::shared_ptr<TSiTable>> tables;
    std::vector<std::shared_ptr<TSiTable>> eitSchedule;
    tables.emplace_back(sdt);

    OS_LOG(DVB_INFO,   "%s:%d: Collecting SDTa(0x%x.0x%x)\n",
      __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
    if (CheckCacheTableCollections(tables, SDT_TIMEOUT)) {
      OS_LOG(DVB_INFO,   "%s:%d: SDTa(0x%x.0x%x) received\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
      status.SdtAcquired = true;
    }
    else {
      OS_LOG(DVB_ERROR,   "%s:%d: SDTa(0x%x.0x%x) not received\n",
       __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
     }
     tables.clear();

     //EITa shed & EITa pf
     std::vector<std::shared_ptr<ServiceStruct>> serviceList = GetServiceListByTsIdCache((*it)->NetworkId, (*it)->TransportStreamId);
     for (auto srv = serviceList.begin(), end = serviceList.end(); srv != end; ++srv) {
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
     if (CheckCacheTableCollections(tables, EIT_PF_TIMEOUT)) {
       OS_LOG(DVB_INFO,   "%s:%d: EITa pf received\n",
         __FUNCTION__, __LINE__);
       status.EitPfAcquired = true;
     }
     else {
       OS_LOG(DVB_ERROR,   "%s:%d: EITa pf not received\n",
         __FUNCTION__, __LINE__);
     }
     if ((*it)->Frequency != BarkerFrequency) {
       OS_LOG(DVB_INFO,   "%s:%d: Collecting EITa sched\n", __FUNCTION__, __LINE__);
       if (CheckCacheTableCollections(eitSchedule, EIT_8_DAY_SCHED_TIMEOUT)) {
         OS_LOG(DVB_INFO,   "%s:%d: EITsched received\n", __FUNCTION__, __LINE__);
         status.EitAcquired = true;
       }
       else {
         OS_LOG(DVB_ERROR,   "%s:%d: EITsched not received\n", __FUNCTION__, __LINE__);
       }
     }
     else {
       OS_LOG(DVB_INFO,   "%s:%d: Not collecting EITa sched on barker(%d)\n",
         __FUNCTION__, __LINE__, (*it)->Frequency);
     }
     OS_LOG(DVB_INFO,   "%s:%d: untune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);
// TODO: KSS
//        tuner.untune();

      DvbScanStatus.TsList.emplace_back((*it)->Frequency, status);
  }

  // Sitting on barker ts
  if (BarkerFrequency && BarkerModulationMode && BarkerSymbolRate) {
    TDvbSiTableStatus status;
    // Clear cached EIT tables
    {
      OS_LOG(DVB_DEBUG,   "%s:%d: Clearing cached EIT tables\n", __FUNCTION__, __LINE__);
      std::lock_guard<std::mutex> lock(CacheDataMutex);
      EitTableMap.clear();
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
      if (CheckCacheTableCollections(fullEitSchedule, BarkerEitTimout)) {
        OS_LOG(DVB_INFO,   "%s:%d: EITsched received (barker)\n", __FUNCTION__, __LINE__);
        status.EitAcquired = true;
      }
      else {
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

std::vector<std::shared_ptr<TStorageTransportStreamStruct>> TDvbSiStorage::GetTsListByNetIdCache(uint16_t nId)
{
  std::vector<std::shared_ptr<TStorageTransportStreamStruct>> ret;
  std::lock_guard<std::mutex> lock(CacheDataMutex);

  auto it = NitTableMap.begin();
  if (nId != 0) {
    it = NitTableMap.find(nId);
  }
  else if (PreferredNetworkId != 0) {
    it = NitTableMap.find(PreferredNetworkId);
  }
  if (it == NitTableMap.end()) {
    return ret;
  }
  const std::vector<TTransportStream>& tsList = it->second->GetTransportStreams();
  for (auto it = tsList.begin(), end = tsList.end(); it != end; ++it) {
    const std::vector<TMpegDescriptor>& tsDescriptors = it->GetTsDescriptors();
    const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(tsDescriptors, TDescriptorTag::CABLE_DELIVERY_TAG);
    if (desc) {
      TCableDeliverySystemDescriptor cable(*desc);
      OS_LOG(DVB_DEBUG,   "<%s> NIT table: freq = 0x%x(%d), mod = 0x%x, symbol_rate = 0x%x(%d)\n",
        __FUNCTION__, cable.GetFrequencyBcd(), cable.GetFrequency(), cable.GetModulation(), cable.GetSymbolRateBcd(), cable.GetSymbolRate());
      std::shared_ptr<TStorageTransportStreamStruct> ts(new TStorageTransportStreamStruct(cable.GetFrequency(),
        MapModulationMode((TDVBConstellation)cable.GetModulation()), cable.GetSymbolRate(), it->GetOriginalNetworkId(), it->GetTsId()));

      OS_LOG(DVB_DEBUG,   "<%s> ts_id =0x%x, frequency: %d, Modulation: %d, symbol_rate: 0x%x\n",
        __FUNCTION__, ts->TransportStreamId, ts->Frequency,  ts->Modulation, ts->SymbolRate);
      ret.push_back(ts);
    }
    else {
      OS_LOG(DVB_ERROR,   "<%s> NIT table: cable delivery descriptor not found\n", __FUNCTION__);
    }
  }
  return ret;
}

bool TDvbSiStorage::CheckCacheTableCollections(std::vector<std::shared_ptr<TSiTable>>& tables, int timeout)
{
  do {
    bool found(true); 
    for (auto tbl = tables.begin(), end = tables.end(); tbl != end; ++tbl) {
      std::lock_guard<std::mutex> lock(CacheDataMutex);
      TTableId tableId = (*tbl)->GetTableId();
      if ((tableId == TTableId::TABLE_ID_NIT) || (tableId == TTableId::TABLE_ID_NIT_OTHER)) {
        OS_LOG(DVB_DEBUG,   "%s:%d: Looking for NIT(0x%x)\n",
          __FUNCTION__, __LINE__, (*tbl)->GetTableExtensionId());
        auto it = NitTableMap.find((*tbl)->GetTableExtensionId());
        if (it == NitTableMap.end()) {
          found = false;
          break;
        }
        else {
          OS_LOG(DVB_DEBUG,   "%s:%d: NIT(0x%x) found\n",
            __FUNCTION__, __LINE__, (*tbl)->GetTableExtensionId());
        }
      }
      else if (tableId == TTableId::TABLE_ID_BAT) {
        OS_LOG(DVB_DEBUG,   "%s:%d: Looking for BAT(0x%x)\n",
          __FUNCTION__, __LINE__, (*tbl)->GetTableExtensionId());
        auto it = BatTableMap.find((*tbl)->GetTableExtensionId());
        if (it == BatTableMap.end()) {
          found = false;
          break;
        }
        else {
          OS_LOG(DVB_DEBUG,   "%s:%d: BAT(0x%x) found\n",
            __FUNCTION__, __LINE__, (*tbl)->GetTableExtensionId());
        }
      }
      else if ((tableId == TTableId::TABLE_ID_SDT) || (tableId == TTableId::TABLE_ID_SDT_OTHER)) {
        std::shared_ptr<TSdtTable> sdt = std::static_pointer_cast<TSdtTable>((*tbl));
        OS_LOG(DVB_DEBUG,   "%s:%d: Looking for SDT(0x%x.0x%x)\n",
          __FUNCTION__, __LINE__, sdt->GetOriginalNetworkId(), sdt->GetTableExtensionId());

        std::pair<uint16_t, uint16_t> key(sdt->GetOriginalNetworkId(), sdt->GetTableExtensionId());
        auto it = SdtTableMap.find(key);
        if (it == SdtTableMap.end()) {
          found = false;
          break;
        }
        else {
          OS_LOG(DVB_DEBUG,   "%s:%d: SDT(0x%x.0x%x) found\n",
            __FUNCTION__, __LINE__, sdt->GetOriginalNetworkId(), sdt->GetTableExtensionId());
        }
      }
      else if ((tableId >= TTableId::TABLE_ID_EIT_PF) && (tableId <= TTableId::TABLE_ID_EIT_SCHED_OTHER_END)) {
        bool isPf = false;
        if (tableId == TTableId::TABLE_ID_EIT_PF || tableId == TTableId::TABLE_ID_EIT_PF_OTHER) {
          isPf = true;
        }
        std::shared_ptr<TEitTable> eit = std::static_pointer_cast<TEitTable>((*tbl));
        OS_LOG(DVB_DEBUG,   "%s:%d: Looking for EIT(0x%x.0x%x.0x%x) isPF: %d\n",
          __FUNCTION__, __LINE__, eit->GetNetworkId(), eit->GetTsId(), eit->GetTableExtensionId(), isPf);
        std::tuple<uint16_t, uint16_t, uint16_t, bool> key(eit->GetNetworkId(), eit->GetTsId(), eit->GetTableExtensionId(), isPf);
        auto it = EitTableMap.find(key);
        if (it == EitTableMap.end()) {
          found = false;
          break;
        }
        else {
          OS_LOG(DVB_DEBUG,   "%s:%d: Looking for EIT(0x%x.0x%x.0x%x) isPF: %d found\n",
          __FUNCTION__, __LINE__, eit->GetNetworkId(), eit->GetTsId(), eit->GetTableExtensionId(), isPf);
        }
      }
    }
    if (found) {
      return true;
    }
    // TODO: Consider using ThreadScanCondition.wait_for() here
   // TODO: KSS validate if sleep() is required here.
    sleep(1);
  } while(timeout-- > 0);
  return false;
}

std::vector<std::shared_ptr<ServiceStruct>> TDvbSiStorage::GetServiceListByTsIdCache(uint16_t nId, uint16_t tsId)
{
  std::vector<std::shared_ptr<ServiceStruct>> ret;
  std::lock_guard<std::mutex> lock(CacheDataMutex);
  OS_LOG(DVB_DEBUG,   "<%s> called: nid.tsid = 0x%x.0x%x\n", __FUNCTION__, nId, tsId);
  std::pair<uint16_t, uint16_t> key(nId, tsId);
  auto it = SdtTableMap.find(key);
  if (it == SdtTableMap.end()) {
    OS_LOG(DVB_ERROR,   "<%s> No SDT found for nid.tsid = 0x%x.0x%x\n", __FUNCTION__, nId, tsId);
    return ret;
  }
  const std::vector<TSdtService>& serviceList = it->second->GetServices();
  for (auto srv = serviceList.begin(), end = serviceList.end(); srv != end; ++srv) {
    const std::vector<TMpegDescriptor>& serviceDescriptors = srv->GetServiceDescriptors();
    const TMpegDescriptor* desc = TMpegDescriptor::FindMpegDescriptor(serviceDescriptors, TDescriptorTag::SERVICE_TAG);
    if (desc) {
      TServiceDescriptor servDesc(*desc);
      OS_LOG(DVB_DEBUG,   "<%s> SDT table: type = 0x%x, provider = %s, name = %s\n",
        __FUNCTION__, servDesc.GetServiceType(), servDesc.GetServiceProviderName().c_str(), servDesc.GetServiceName().c_str());
      std::shared_ptr<Service> service(new Service(it->second->GetOriginalNetworkId(), it->second->GetTableExtensionId(),
        srv->GetServiceId(), servDesc.GetServiceName()));
      ret.push_back(service);
    }
    else {
      OS_LOG(DVB_ERROR,   "<%s> SDT table: service descriptor not found\n", __FUNCTION__);
    }
  }
  return ret;
}

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
  std::vector<std::shared_ptr<TSiTable>> tables;
  // NIT
  tables.emplace_back(new TNitTable((uint8_t)TTableId::TABLE_ID_NIT, PreferredNetworkId, 0, true));
  // BAT(s)
  for (auto it = HomeBouquetsVector.begin(), end = HomeBouquetsVector.end(); it != end; ++it) {
    OS_LOG(DVB_DEBUG,   "%s:%d: Adding BAT(0x%x) to the list\n", __FUNCTION__, __LINE__, *it);
    tables.emplace_back(new TBatTable((uint8_t)TTableId::TABLE_ID_BAT, *it, 0, true));
  }
  OS_LOG(DVB_INFO,   "%s:%d: Collecting NIT & BAT(s)\n", __FUNCTION__, __LINE__);
  if (CheckCacheTableCollections(tables, NIT_TIMEOUT > BAT_TIMEOUT ? NIT_TIMEOUT : BAT_TIMEOUT)) {
    OS_LOG(DVB_INFO,   "%s:%d: NIT & BAT(s) found\n", __FUNCTION__, __LINE__);
    status.NitAcquired = true;
    status.BatAcquired = true;
  }
  else {
    OS_LOG(DVB_ERROR,   "%s:%d: NIT & BAT(s) not found\n", __FUNCTION__, __LINE__);
    return false;
  }
  tables.clear();
  // Collect SDT & EIT pf(optional)
  std::vector<std::shared_ptr<TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(PreferredNetworkId);
  for (auto it = tsList.begin(), end = tsList.end(); it != end; ++it) {
    if (!IsFastScan) {
      if (HomeTsFrequency == (*it)->Frequency) {
        OS_LOG(DVB_DEBUG,   "%s:%d: Adding SDT(0x%x.0x%x) actual to the list\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId);
        TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
        sdt->SetOriginalNetworkId((*it)->NetworkId);
        tables.emplace_back(sdt);
        break;
      }
    }
    else {
      OS_LOG(DVB_DEBUG,   "%s:%d: Adding SDT(0x%x.0x%x) to the list\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId);
      TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
      sdt->SetOriginalNetworkId((*it)->NetworkId);
      tables.emplace_back(sdt);
    }
  }
  if (CheckCacheTableCollections(tables, IsFastScan ? SDT_OTHER_TIMEOUT : SDT_TIMEOUT)) {
    OS_LOG(DVB_INFO,   "%s:%d: All SDTs received\n", __FUNCTION__, __LINE__);
    status.SdtAcquired = true;
  }
  else {
    OS_LOG(DVB_ERROR,   "%s:%d: All SDTs not received\n", __FUNCTION__, __LINE__);
  }
  DvbScanStatus.TsList.emplace_back(HomeTsFrequency, status);
  return true;
}

TModulationMode TDvbSiStorage::MapModulationMode(TDVBConstellation in)
{
  TModulationMode out;
  switch (in) {
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

void TDvbSiStorage::ClearCachedTables()
{
  std::lock_guard<std::mutex> lock(CacheDataMutex);
  OS_LOG(DVB_INFO,   "%s:%d: Clearing cached tables\n", __FUNCTION__, __LINE__);
  NitTableMap.clear();
  SdtTableMap.clear();
  EitTableMap.clear();
  BatTableMap.clear();
}

// public function implementations.

TDvbSiStorage::TDvbSiStorage(const uint32_t& freq, const TModulationMode& modulation, const uint32_t& symblRate,
  const uint16_t& prefNetworkId, const std::string& dbFile)
  : HomeTsFrequency(freq),
    HomeTsModulationMode(modulation),
    HomeTsSymbolRate(symblRate),
    PreferredNetworkId(prefNetworkId),
    DbFilePath(dbFile),
    BackGroundScanInterval(21600),
    IsFastScan(false),
    TunerIndex(0)
{
  // Empty
}

TDvbSiStorage::~TDvbSiStorage()
{
  // Empty
}

TFileStatus TDvbSiStorage::CreateDatabase()
{
  TFileStatus status(FILE_STATUS_ERROR);
  status = StorageDb.CreateDbFile(DbFilePath);
  if (status == TFileStatus::FILE_STATUS_OPENED) {
    // flush old entires if any.
    StorageDb.DropTables();
    StorageDb.CreateTables();
    status = FILE_STATUS_CREATED;
  }
}

void TDvbSiStorage::HandleTableEvent(const TSiTable& tbl)
{
  TTableId tableId = tbl.GetTableId();
  switch (tableId) {
  case TTableId::TABLE_ID_NIT:
  case TTableId::TABLE_ID_NIT_OTHER:
    // Only handle NIT tables from preferred Network if specified.
    if (PreferredNetworkId == 0 || PreferredNetworkId == tbl.GetTableExtensionId()) {
      HandleNitEvent(static_cast<const TNitTable&>(tbl));
    }
    break;
  case TTableId::TABLE_ID_SDT:
  case TTableId::TABLE_ID_SDT_OTHER:
    HandleSdtEvent(static_cast<const TSdtTable&>(tbl));
    break;
  case TTableId::TABLE_ID_TDT:
  case TTableId::TABLE_ID_TOT:
    HandleTotEvent(static_cast<const TTotTable&>(tbl));
    break;
  case TTableId::TABLE_ID_BAT:
    HandleBatEvent(static_cast<const TBatTable&>(tbl));
    break;
  default:
    if ((tableId >= TTableId::TABLE_ID_EIT_PF) && (tableId <= TTableId::TABLE_ID_EIT_SCHED_OTHER_END)) {
      HandleEitEvent(static_cast<const TEitTable&>(tbl));
    }
    else {
      OS_LOG(DVB_ERROR,   "<%s> Unknown table id = 0x%x\n", __FUNCTION__, tableId);
    }
  }
}

void TDvbSiStorage::SetBarkerInfo(const uint32_t& barkerFreq, const TModulationMode& barkMod, const uint32_t& barkSymbRate)
{
  BarkerFrequency = barkerFreq;
  BarkerSymbolRate = barkSymbRate;
  BarkerModulationMode = barkMod;
}
#if 0
void TDvbSiStorage::StopScanThread()
{
  OS_LOG(DVB_INFO, "%s(): called\n", __FUNCTION__);
  std::unique_lock<std::mutex> lk(ScanMutex);
  if (DvbScanStatus.ScanState == TDvbScanState::SCAN_STOPPED) {
    OS_LOG(DVB_INFO,   "%s(): scan is already stopped\n", __FUNCTION__);
    return;
  }
  while (DvbScanStatus.ScanState != TDvbScanState::SCAN_STOPPED) {
    OS_LOG(DVB_INFO,   "%s(): sending stop request\n", __FUNCTION__);
    lk.unlock();
    ThreadScanCondition.notify_one();
    sleep(3);
    lk.lock();
  }
  if (ScanThreadObject.joinable()) {
    ScanThreadObject.join();
  }
  OS_LOG(DVB_INFO,   "%s(): done\n", __FUNCTION__);
}
#endif

// IDvbStorageSubject

void TDvbSiStorage::RegisterDvbStorageObserver(IDvbStorageObserver* observerObject)
{
  ObserverVector.push_back(observerObject);
}

void TDvbSiStorage::RemoveDvbStorageObserver(IDvbStorageObserver* observerObject)
{
  ObserverVector.erase(std::remove(ObserverVector.begin(), ObserverVector.end(), observerObject), ObserverVector.end());
}

void TDvbSiStorage::NotifyDvbStorageTuneObserver(const uint32_t& freq, const TModulationMode& mod,
    const uint32_t& symbol,const uint8_t& tuneIndex)
{
  std::vector<IDvbStorageObserver*>::const_iterator iter = ObserverVector.begin();
  for (; iter != ObserverVector.end(); ++iter) {
    (*iter)->Tune(freq, mod, symbol, tuneIndex);
  }
}

void TDvbSiStorage::NotifyDvbStorageUnTuneObserver()
{
  std::vector<IDvbStorageObserver*>::const_iterator iter = ObserverVector.begin();
  for (; iter != ObserverVector.end(); ++iter) {
    (*iter)->UnTune();
  }
}

void TDvbSiStorage::DoBackGroundScan()
{
  OS_LOG(DVB_INFO,   "%s: BackGround scan Started\n", __FUNCTION__);
  DvbScanStatus.TsList.clear();
  std::vector<std::shared_ptr<TSiTable>> fullEitSchedule;
  std::vector<std::shared_ptr<TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(PreferredNetworkId);
  for (auto it = tsList.begin(), end = tsList.end(); it != end; ++it) {
    TDvbSiTableStatus status;
    OS_LOG(DVB_INFO,   "%s:%d: tune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);

    // collect SDTa & EITa pf
    // SDTa
    TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
    sdt->SetOriginalNetworkId((*it)->NetworkId);

    std::vector<std::shared_ptr<TSiTable>> tables;
    std::vector<std::shared_ptr<TSiTable>> eitSchedule;
    tables.emplace_back(sdt);

    OS_LOG(DVB_INFO,   "%s:%d: Collecting SDTa(0x%x.0x%x)\n",
      __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
    if (CheckCacheTableCollections(tables, SDT_TIMEOUT)) {
      OS_LOG(DVB_INFO,   "%s:%d: SDTa(0x%x.0x%x) received\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
      status.SdtAcquired = true;
    }
    else {
      OS_LOG(DVB_ERROR,   "%s:%d: SDTa(0x%x.0x%x) not received\n",
       __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
     }
     tables.clear();

     //EITa shed & EITa pf
     std::vector<std::shared_ptr<ServiceStruct>> serviceList = GetServiceListByTsIdCache((*it)->NetworkId, (*it)->TransportStreamId);
     for (auto srv = serviceList.begin(), end = serviceList.end(); srv != end; ++srv) {
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
     if (CheckCacheTableCollections(tables, EIT_PF_TIMEOUT)) {
       OS_LOG(DVB_INFO,   "%s:%d: EITa pf received\n",
         __FUNCTION__, __LINE__);
       status.EitPfAcquired = true;
     }
     else {
       OS_LOG(DVB_ERROR,   "%s:%d: EITa pf not received\n",
         __FUNCTION__, __LINE__);
     }
     if ((*it)->Frequency != BarkerFrequency) {
       OS_LOG(DVB_INFO,   "%s:%d: Collecting EITa sched\n", __FUNCTION__, __LINE__);
       if (CheckCacheTableCollections(eitSchedule, EIT_8_DAY_SCHED_TIMEOUT)) {
         OS_LOG(DVB_INFO,   "%s:%d: EITsched received\n", __FUNCTION__, __LINE__);
         status.EitAcquired = true;
       }
       else {
         OS_LOG(DVB_ERROR,   "%s:%d: EITsched not received\n", __FUNCTION__, __LINE__);
       }
     }
     else {
       OS_LOG(DVB_INFO,   "%s:%d: Not collecting EITa sched on barker(%d)\n",
         __FUNCTION__, __LINE__, (*it)->Frequency);
     }
     OS_LOG(DVB_INFO,   "%s:%d: untune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);
// TODO: KSS
     NotifyDvbStorageUnTuneObserver();
//        tuner.untune();

      DvbScanStatus.TsList.emplace_back((*it)->Frequency, status);
  }

  // Sitting on barker ts
  if (BarkerFrequency && BarkerModulationMode && BarkerSymbolRate) {
    TDvbSiTableStatus status;
    // Clear cached EIT tables
    {
      OS_LOG(DVB_DEBUG,   "%s:%d: Clearing cached EIT tables\n", __FUNCTION__, __LINE__);
      std::lock_guard<std::mutex> lock(CacheDataMutex);
      EitTableMap.clear();
    }
    OS_LOG(DVB_INFO,   "%s:%d: tune(%d) barker\n", __FUNCTION__, __LINE__, BarkerFrequency);
// TODO: KSS
     NotifyDvbStorageTuneObserver(BarkerFrequency, BarkerModulationMode, BarkerSymbolRate, ++TunerIndex);
#if 0
        RMFResult ret = tuner.tune(BarkerFrequency, BarkerModulationMode, BarkerSymbolRate);
        if(ret != RMF_RESULT_SUCCESS)
        {
            OS_LOG(DVB_ERROR,   "%s(): tune(%d) failed with 0x%x\n", __FUNCTION__, BarkerFrequency, ret);
        }
#endif
    OS_LOG(DVB_INFO,   "%s:%d: Collecting EIT sched (barker)\n", __FUNCTION__, __LINE__);
    if (CheckCacheTableCollections(fullEitSchedule, BarkerEitTimout)) {
      OS_LOG(DVB_INFO,   "%s:%d: EITsched received (barker)\n", __FUNCTION__, __LINE__);
      status.EitAcquired = true;
    }
    else {
      OS_LOG(DVB_ERROR,   "%s:%d: EITsched not received (barker)\n", __FUNCTION__, __LINE__);
    }

    OS_LOG(DVB_INFO,   "%s:%d: untune(%d)\n", __FUNCTION__, __LINE__, BarkerFrequency);
// TODO: KSS
    NotifyDvbStorageUnTuneObserver();
//    tuner.untune();
    DvbScanStatus.TsList.emplace_back(BarkerFrequency, status);
  }
  OS_LOG(DVB_INFO,   "%s:%d: Done\n", __FUNCTION__, __LINE__);
}

void TDvbSiStorage::DoFastScan()
{
  OS_LOG(DVB_INFO, "%s:FastScan Started\n", __FUNCTION__);
  DvbScanStatus.TsList.clear();
  // NOTE: Home Freq scan should have been already initiated as part of Initialiation in client code.
  std::vector<std::shared_ptr<TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(PreferredNetworkId);
  for (auto it = tsList.begin(), end = tsList.end(); it != end; ++it) {
    TDvbSiTableStatus status;
    // collect SDTa & EITa pf
    TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
    sdt->SetOriginalNetworkId((*it)->NetworkId);
    std::vector<std::shared_ptr<TSiTable>> tables;
    tables.emplace_back(sdt);
    std::vector<std::shared_ptr<ServiceStruct>> serviceList = GetServiceListByTsIdCache((*it)->NetworkId, (*it)->TransportStreamId);
    for (auto srv = serviceList.begin(), end = serviceList.end(); srv != end; ++srv) {
      OS_LOG(DVB_DEBUG, "%s:%d: Adding EITpf(0x%x.0x%x.0x%x) to the list\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId, (*srv)->ServiceId);

      TEitTable* eit = new TEitTable((uint8_t)TTableId::TABLE_ID_EIT_PF, (*srv)->ServiceId, 0, true);
      eit->SetNetworkId((*it)->NetworkId);
      eit->SetTsId((*it)->TransportStreamId);
      tables.emplace_back(eit);
    }
    if (IsFastScan && CheckCacheTableCollections(tables, 0)) {
      OS_LOG(DVB_INFO, "%s:%d: SDTa(0x%x.0x%x) & EITa pf already received. Skipping.\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
      status.SdtAcquired = true;
      status.EitPfAcquired = true;
    }
    else {
      OS_LOG(DVB_INFO, "%s:%d: tune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);
// TODO: KSS
      NotifyDvbStorageTuneObserver((*it)->Frequency, (*it)->Modulation, (*it)->SymbolRate, ++TunerIndex);
#if 0
            RMFResult ret = tuner.tune((*it)->Frequency, (*it)->modulation, (*it)->symbolRate);
            if(ret != RMF_RESULT_SUCCESS)
            {
                OS_LOG(DVB_ERROR,   "%s(): tune(%d) failed with 0x%x\n", __FUNCTION__, (*it)->Frequency, ret);
            }
#endif
      OS_LOG(DVB_INFO, "%s:%d: Collecting SDTa & EITs pf\n", __FUNCTION__, __LINE__);
      if (CheckCacheTableCollections(tables, SDT_TIMEOUT > EIT_PF_TIMEOUT ? SDT_TIMEOUT : EIT_PF_TIMEOUT)) {
        OS_LOG(DVB_INFO, "%s:%d: SDT(0x%x.0x%x) & EITs pf received\n",
          __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
        status.SdtAcquired = true;
        status.EitPfAcquired = true;
      }
      else {
        OS_LOG(DVB_ERROR,   "%s:%d: SDT(0x%x.0x%x) and/or EITs pf not received\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, sdt->GetTableExtensionId());
      }
// TODO: KSS
      NotifyDvbStorageUnTuneObserver();
//      OS_LOG(DVB_INFO,   "%s:%d: untune(%d)\n", __FUNCTION__, __LINE__, (*it)->Frequency);
        //tuner.untune();
    }
    DvbScanStatus.TsList.emplace_back((*it)->Frequency, status);
  }
  OS_LOG(DVB_ERROR,   "%s:%d: Done\n", __FUNCTION__, __LINE__);
}

void TDvbSiStorage::DoHomeScanDataProcess()
{
  TDvbSiTableStatus status;
  // Let's start over clean slate
  ClearCachedTables();
  std::vector<std::shared_ptr<TSiTable>> tables;
  // NIT
  tables.emplace_back(new TNitTable((uint8_t)TTableId::TABLE_ID_NIT, PreferredNetworkId, 0, true));
  // BAT(s)
  for (auto it = HomeBouquetsVector.begin(), end = HomeBouquetsVector.end(); it != end; ++it) {
    OS_LOG(DVB_DEBUG,   "%s:%d: Adding BAT(0x%x) to the list\n", __FUNCTION__, __LINE__, *it);
    tables.emplace_back(new TBatTable((uint8_t)TTableId::TABLE_ID_BAT, *it, 0, true));
  }
  OS_LOG(DVB_INFO,   "%s:%d: Collecting NIT & BAT(s)\n", __FUNCTION__, __LINE__);
  if (CheckCacheTableCollections(tables, NIT_TIMEOUT > BAT_TIMEOUT ? NIT_TIMEOUT : BAT_TIMEOUT)) {
    OS_LOG(DVB_INFO,   "%s:%d: NIT & BAT(s) found\n", __FUNCTION__, __LINE__);
    status.NitAcquired = true;
    status.BatAcquired = true;
  }
  else {
    OS_LOG(DVB_ERROR,   "%s:%d: NIT & BAT(s) not found\n", __FUNCTION__, __LINE__);
    //return false;
    return ;
  }
  tables.clear();
  // Collect SDT & EIT pf(optional)
  std::vector<std::shared_ptr<TStorageTransportStreamStruct>> tsList = GetTsListByNetIdCache(PreferredNetworkId);
  for (auto it = tsList.begin(), end = tsList.end(); it != end; ++it) {
    if (!IsFastScan) {
      if (HomeTsFrequency == (*it)->Frequency) {
        OS_LOG(DVB_DEBUG,   "%s:%d: Adding SDT(0x%x.0x%x) actual to the list\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId);
        TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
        sdt->SetOriginalNetworkId((*it)->NetworkId);
        tables.emplace_back(sdt);
        break;
      }
    }
    else {
      OS_LOG(DVB_DEBUG,   "%s:%d: Adding SDT(0x%x.0x%x) to the list\n",
        __FUNCTION__, __LINE__, (*it)->NetworkId, (*it)->TransportStreamId);
      TSdtTable* sdt = new TSdtTable((uint8_t)TTableId::TABLE_ID_SDT, (*it)->TransportStreamId, 0, true);
      sdt->SetOriginalNetworkId((*it)->NetworkId);
      tables.emplace_back(sdt);
    }
  }
  if (CheckCacheTableCollections(tables, IsFastScan ? SDT_OTHER_TIMEOUT : SDT_TIMEOUT)) {
    OS_LOG(DVB_INFO,   "%s:%d: All SDTs received\n", __FUNCTION__, __LINE__);
    status.SdtAcquired = true;
  }
  else {
    OS_LOG(DVB_ERROR,   "%s:%d: All SDTs not received\n", __FUNCTION__, __LINE__);
  }
  DvbScanStatus.TsList.emplace_back(HomeTsFrequency, status);
}
