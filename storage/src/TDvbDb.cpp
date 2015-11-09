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

#include "TDvbDb.h"

#include <stdlib.h>
#include <sys/time.h>

#include <cstdio>

#include <oswrap.h>

#include "TMpegDescriptor.h"
#include "TComponentDescriptor.h"

using namespace std;
using namespace sqlite3pp;
using namespace TDvbStorageNamespace;


const StringVector TDvbDb::DatabaseTables =
{
  "EitDescriptor",
  "Eit",
  "SdtDescriptor",
  "Sdt",
  "BatDescriptor",
  "BatTransportDescriptor",
  "BatTransport", 
  "Bat",
  "NitDescriptor",
  "NitTransportDescriptor",
  "NitTransport",
  "Nit",
  "EventItem",
  "EventComponent",
  "Event",
  "ServiceComponent",
  "Service",
  "Transport",
  "Bouquet",
  "Network"
};

const StringVector TDvbDb::DatabaseSchema =
{
  "CREATE TABLE IF NOT EXISTS Nit ("          \
  "nit_pk INTEGER PRIMARY KEY NOT NULL, "     \
  "network_id INTEGER  NOT NULL, "            \
  "version INTEGER NOT NULL);",
  "CREATE UNIQUE INDEX IF NOT EXISTS Nit_index ON Nit (" \
  "network_id);",

  "CREATE TABLE IF NOT EXISTS NitDescriptor ( " \
  "fkey INTEGER NOT NULL,"                      \
  "descriptor_id INTEGER NOT NULL,"             \
  "descriptor BLOB NOT NULL,"                   \
  "FOREIGN KEY (fkey) REFERENCES Nit (nit_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS NitDescriptor_index ON NitDescriptor (" \
  "fkey,"                                                                    \
  "descriptor_id,"                                                           \
  "descriptor);",

  "CREATE TABLE IF NOT EXISTS NitTransport ("      \
  "nit_transport_pk INTEGER PRIMARY KEY NOT NULL," \
  "original_network_id INTEGER NOT NULL,"          \
  "transport_id INTEGER NOT NULL,"                 \
  "nit_fk INTEGER NOT NULL,"                       \
  "FOREIGN KEY (nit_fk) REFERENCES Nit (network_id));",

  "CREATE UNIQUE INDEX IF NOT EXISTS NitTransport_index ON NitTransport (" \
  "original_network_id,"                                                   \
  "transport_id,"                                                          \
  "nit_fk);",
  "CREATE TABLE IF NOT EXISTS NitTransportDescriptor ( "  \
  "fkey INTEGER NOT NULL,"                                \
  "descriptor_id INTEGER NOT NULL,"                       \
  "descriptor BLOB NOT NULL,"                             \
  "FOREIGN KEY (fkey) REFERENCES NitTransport (nit_transport_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS NitTransportDescriptor_index ON NitTransportDescriptor (" \
  "fkey,"                                                                                      \
  "descriptor_id,"                                                                             \
  "descriptor);",

  "CREATE TABLE IF NOT EXISTS Sdt ("             \
  "sdt_pk INTEGER PRIMARY KEY NOT NULL,"         \
  "service_id INTEGER NOT NULL,"                 \
  "nit_transport_fk INTEGER,"                    \
  "version INTEGER NOT NULL,"                    \
  "schedule INTEGER NOT NULL,"                   \
  "present_following INTEGER NOT NULL,"          \
  "scrambled INTEGER NOT NULL,"                  \
  "running INTEGER NOT NULL, "                   \
  "FOREIGN KEY (nit_transport_fk) "              \
  "REFERENCES NitTransport (nit_transport_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS Sdt_index ON Sdt (" \
  "service_id,"                                          \
  "nit_transport_fk);",

  "CREATE TABLE IF NOT EXISTS SdtDescriptor ( " \
  "fkey INTEGER NOT NULL,"                      \
  "descriptor_id INTEGER NOT NULL,"             \
  "descriptor BLOB NOT NULL,"                   \
  "FOREIGN KEY (fkey) REFERENCES Sdt (sdt_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS SdtDescriptor_index ON SdtDescriptor (" \
  "fkey,"                                                                    \
  "descriptor_id,"                                                           \
  "descriptor);",

  "CREATE TABLE IF NOT EXISTS Eit ("             \
  "eit_pk INTEGER PRIMARY KEY NOT NULL,"         \
  "network_id INTEGER NOT NULL,"                 \
  "transport_id INTEGER NOT NULL,"               \
  "service_id INTEGER NOT NULL,"                 \
  "event_id INTEGER NOT NULL,"                   \
  "version INTEGER NOT NULL,"                    \
  "present_following INTEGER NOT NULL,"          \
  "start_time INTEGER NOT NULL,"                 \
  "duration INTEGER NOT NULL,"                   \
  "scrambled INTEGER NOT NULL,"                  \
  "running INTEGER NOT NULL);"                   \

  "CREATE UNIQUE INDEX IF NOT EXISTS Eit_index ON Eit (" \
  "network_id,"                                          \
  "transport_id,"                                        \
  "service_id,"                                          \
  "event_id);",

  "CREATE TABLE IF NOT EXISTS EitDescriptor ("   \
  "fkey INTEGER NOT NULL,"                       \
  "descriptor_id INTEGER NOT NULL,"              \
  "descriptor BLOB NOT NULL,"                    \
  "FOREIGN KEY (fkey) REFERENCES Eit (eit_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS EitDescriptor_index ON EitDescriptor (" \
  "fkey,"                                                                    \
  "descriptor_id,"                                                           \
  "descriptor);",

  "CREATE TABLE IF NOT EXISTS Bat ("             \
  "bat_pk INTEGER PRIMARY KEY NOT NULL,"         \
  "bouquet_id INTEGER NOT NULL,"                 \
  "version INTEGER NOT NULL);",

  "CREATE UNIQUE INDEX IF NOT EXISTS Bat_index ON Bat (" \
  "bouquet_id);",

  "CREATE TABLE IF NOT EXISTS BatDescriptor ( "  \
  "fkey INTEGER NOT NULL,"                       \
  "descriptor_id INTEGER NOT NULL,"              \
  "descriptor BLOB NOT NULL,"                    \
  "FOREIGN KEY (fkey) REFERENCES Bat (bat_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS BatDescriptor_index ON BatDescriptor (" \
  "fkey,"                                                                    \
  "descriptor_id,"                                                           \
  "descriptor);",

  "CREATE TABLE IF NOT EXISTS BatTransport ("      \
  "bat_transport_pk INTEGER PRIMARY KEY NOT NULL," \
  "original_network_id INTEGER NOT NULL,"          \
  "transport_id INTEGER NOT NULL,"                 \
  "bat_fk INTEGER NOT NULL, "                      \
  "nit_transport_fk INTEGER,"                      \
  "FOREIGN KEY (nit_transport_fk) REFERENCES "     \
  "  NitTransport (nit_transport_pk)  "            \
  "FOREIGN KEY (bat_fk) REFERENCES Bat (bat_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS BatTransport_index ON BatTransport (" \
  "original_network_id,"                                                   \
  "transport_id,"                                                          \
  "bat_fk);",

  "CREATE TABLE IF NOT EXISTS BatTransportDescriptor ( "  \
  "fkey INTEGER NOT NULL,"                                \
  "descriptor_id INTEGER NOT NULL,"                       \
  "descriptor BLOB NOT NULL,"                             \
  "FOREIGN KEY (fkey) REFERENCES BatTransport (bat_transport_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS BatTransportDescriptor_index ON BatTransportDescriptor (" \
  "fkey,"                                                                                      \
  "descriptor_id,"                                                                             \
  "descriptor);",

  // Parsed

  "CREATE TABLE IF NOT EXISTS Bouquet ("      \
  "bouquet_pk INTEGER PRIMARY KEY NOT NULL, " \
  "bouquet_id INTEGER NOT NULL,"              \
  "version INTEGER NOT NULL,"                 \
  "iso_639_language_code TEXT,"               \
  "name TEXT);",

  "CREATE UNIQUE INDEX IF NOT EXISTS Bouquet_index ON Bouquet (" \
  "bouquet_id);",

  "CREATE TABLE IF NOT EXISTS Network ("      \
  "network_pk INTEGER PRIMARY KEY NOT NULL, " \
  "network_id INTEGER NOT NULL,"              \
  "version INTEGER NOT NULL,"                 \
  "iso_639_language_code TEXT,"               \
  "name TEXT);",

  "CREATE UNIQUE INDEX IF NOT EXISTS Network_index ON Network (" \
  "network_id);",

  "CREATE TABLE IF NOT EXISTS Transport ("                    \
  "transport_pk INTEGER PRIMARY KEY NOT NULL,"                \
  "original_network_id INTEGER NOT NULL,"                     \
  "transport_id INTEGER NOT NULL,"                            \
  "network_fk INTEGER NOT NULL,"                              \
  "bouquet_fk INTEGER,"                                       \
  "frequency INTEGER NOT NULL,"                               \
  "modulation INTEGER NOT NULL,"                              \
  "symbol_rate INTEGER NOT NULL,"                             \
  "fec_outer INTEGER NOT NULL,"                               \
  "fec_inner INTEGER NOT NULL,"                               \
  "FOREIGN KEY (network_fk) REFERENCES Network (network_pk)," \
  "FOREIGN KEY (bouquet_fk) REFERENCES Bouquet (bouquet_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS Transport_index ON Transport (" \
  "original_network_id,"                                             \
  "transport_id,"                                                    \
  "network_fk);",

  "CREATE TABLE IF NOT EXISTS Service ("         \
  "service_pk INTEGER PRIMARY KEY NOT NULL,"     \
  "service_id INTEGER NOT NULL,"                 \
  "transport_fk INTEGER,"                        \
  "version INTEGER NOT NULL,"                    \
  "service_type INTEGER NOT NULL,"               \
  "logical_channel_number INTEGER,"              \
  "running INTEGER NOT NULL, "                   \
  "scrambled INTEGER NOT NULL,"                  \
  "schedule INTEGER NOT NULL,"                   \
  "present_following INTEGER NOT NULL,"          \
  "iso_639_language_code TEXT,"                  \
  "service_name TEXT,"                           \
  "provider_name TEXT,"                          \
  "FOREIGN KEY (transport_fk) REFERENCES Transport (transport_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS Service_index ON Service (" \
  "service_id,"                                                  \
  "transport_fk);",

  "CREATE TABLE IF NOT EXISTS ServiceComponent ("   \
  "fkey INTEGER NOT NULL,"                          \
  "stream_content INTEGER NOT NULL,"                \
  "component_type INTEGER NOT NULL,"                \
  "component_tag INTEGER NOT NULL,"                 \
  "iso_639_language_code TEXT,"                     \
  "description TEXT, "                              \
  "FOREIGN KEY (fkey) REFERENCES Service (service_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS ServiceComponent_index ON ServiceComponent (" \
  "fkey,"                                                                          \
  "stream_content,"                                                                \
  "component_type,"                                                                \
  "component_tag);",

  "CREATE TABLE IF NOT EXISTS Event ("           \
  "event_pk INTEGER PRIMARY KEY NOT NULL,"       \
  "network_id INTEGER NOT NULL,"                 \
  "transport_id INTEGER NOT NULL,"               \
  "service_id INTEGER NOT NULL,"                 \
  "event_id INTEGER NOT NULL,"                   \
  "version INTEGER NOT NULL,"                    \
  "present_following INTEGER NOT NULL,"          \
  "start_time INTEGER NOT NULL,"                 \
  "duration INTEGER NOT NULL,"                   \
  "scrambled INTEGER NOT NULL,"                  \
  "running INTEGER NOT NULL, "                   \
  "parental_rating TEXT,"                        \
  "content TEXT);",

  "CREATE UNIQUE INDEX IF NOT EXISTS Event_index ON Event (" \
  "network_id,"                                              \
  "transport_id,"                                            \
  "service_id,"                                              \
  "event_id);",

  "CREATE TABLE IF NOT EXISTS EventComponent ("     \
  "fkey INTEGER NOT NULL,"                          \
  "stream_content INTEGER NOT NULL,"                \
  "component_type INTEGER NOT NULL,"                \
  "component_tag INTEGER NOT NULL,"                 \
  "iso_639_language_code TEXT,"                     \
  "description TEXT, "                              \
  "FOREIGN KEY (fkey) REFERENCES Event (event_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS EventComponent_index ON EventComponent (" \
  "fkey,"                                                                      \
  "stream_content,"                                                            \
  "component_type,"                                                            \
  "component_tag);",

  "CREATE TABLE IF NOT EXISTS EventItem ("          \
  "event_fk INTEGER NOT NULL,"                      \
  "iso_639_language_code TEXT,"                     \
  "title TEXT, "                                    \
  "description TEXT, "                              \
  "FOREIGN KEY (event_fk) REFERENCES Event (event_pk));",

  "CREATE UNIQUE INDEX IF NOT EXISTS EventItem_index ON EventItem (" \
  "event_fk,"                                                        \
  "iso_639_language_code);"
};

ostream& operator<< (ostream& os, TElapseTime& et)
{
  os << et.EndTime << " seconds " << et.EndTime / 60.0 << " minutes.";
  return os;
}

void TElapseTime::Display()
{
  OS_LOG(DVB_DEBUG,  "<%s> - Elapsed time: %lf seconds\n", __FUNCTION__, EndTime);
}


TDvbDb::TDvbDb()
  : IsTotReceived(false),
    IsDataStale(false)
{
  // Empty
}

TDvbDb::~TDvbDb()
{
  // clear out all collections
  ClearUpdate();
  ClearScanSettings();
}

TFileStatus  TDvbDb::CreateDbFile(const std::string& dbname)
{
  TFileStatus  status(FILE_STATUS_ERROR);
  DbFileName = dbname;

  if (Sqlite3ppWrapper.connect_v2(DbFileName.c_str(), SQLITE_OPEN_READWRITE) == SQLITE_OK) {
    status = FILE_STATUS_OPENED;
    OS_LOG(DVB_DEBUG,  "<%s> - Opened database %s, Performing sanity checks.\n", __FUNCTION__, DbFileName.c_str());
    TSanityStatus sanity = SanityCheck();
    switch (sanity) {
    case SANITY_STATUS_POPULATED:
      OS_LOG(DVB_DEBUG,  "<%s> - Database %s is populated!\n", __FUNCTION__, DbFileName.c_str());
      status = FILE_STATUS_OPENED;
      break;
    case SANITY_STATUS_EMPTY:
    case SANITY_STATUS_CORRUPT:
    case SANITY_STATUS_FULL:
    case SANITY_STATUS_STALE:
    case SANITY_STATUS_UNKNOWN:
    default:
      OS_LOG(DVB_DEBUG,  "<%s> - Empty or corrupt Database %s Error:%d, %d, %s\n",
        __FUNCTION__, DbFileName.c_str(), static_cast<int>(sanity), Sqlite3ppWrapper.error_code(), Sqlite3ppWrapper.error_msg());
      status = FILE_STATUS_ERROR;
      break;
    }
  }

  if (status != FILE_STATUS_OPENED) {
    std::remove(DbFileName.c_str());

    status = OpenDb(SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
    if (status == FILE_STATUS_OPENED) {
      OS_LOG(DVB_DEBUG,  "<%s> - Created database: %s\n", __FUNCTION__, DbFileName.c_str());
      status = FILE_STATUS_CREATED;
      CreateSchema();
    }
    else {
      OS_LOG(DVB_ERROR,  "<%s> - Unable to create database file: %s\n", __FUNCTION__, DbFileName.c_str());
      status = FILE_STATUS_ERROR;
    }
  }
  return  status;
}


TFileStatus  TDvbDb::OpenDb(int /*flags*/)
{
  TFileStatus  status(FILE_STATUS_ERROR);

  if (Sqlite3ppWrapper.connect_v2(DbFileName.c_str(), (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)) == SQLITE_OK) {
    status = FILE_STATUS_OPENED;
  }
  else {
    OS_LOG(DVB_DEBUG,  "<%s> - Unable to create database file: %s : error: %d, %s\n",
      __FUNCTION__, DbFileName.c_str(), Sqlite3ppWrapper.error_code(), Sqlite3ppWrapper.error_msg());
    status = FILE_STATUS_ERROR;
  }
  return status;
}

void TDvbDb::CloseDbFile()
{
  std::lock_guard<std::mutex> lock(DbMutex);
  Sqlite3ppWrapper.disconnect();
}

// Stale check to determine if the database contents have expired
TDvbDb::TSanityStatus  TDvbDb::StaleCheck()
{
  TSanityStatus  status = SANITY_STATUS_UNKNOWN;
  uint64_t latestStart = LatestStartTime();
  if (latestStart > 0) {
    status = SANITY_STATUS_POPULATED;
    struct timeval timeVal;
    int rc = gettimeofday(&timeVal, NULL);
    // if current time is greater than last Eit event - DvbDb is stale.
    if (rc == 0 && timeVal.tv_sec > static_cast<int64_t>(latestStart)) {
      OS_LOG(DVB_INFO,  "<%s> SANITY_STATUS_STALE Db! latest start time %ld, %ld\n", __FUNCTION__, latestStart, timeVal.tv_sec);
      status = SANITY_STATUS_STALE;
    }
  }
  return  status;
}

// Determine if contents have expired
void  TDvbDb::StaleAudit()
{
  // received TOT event and have NOT checked for stale condition.
  if (IsTotReceived && !IsDataStale) {
    IsDataStale = true; // check stale condition once!
    TDvbDb::TSanityStatus sanity = StaleCheck();
    if (sanity == TDvbDb::SANITY_STATUS_STALE) {
      OS_LOG(DVB_INFO,  "<%s> SANITY_STATUS_STALE Db detected!\n", __FUNCTION__);
      DropSchema();
      CreateSchema();
    }
  }
}

// Sanity check to determine if the database file is valid
TDvbDb::TSanityStatus  TDvbDb::SanityCheck()
{
  TSanityStatus  status = SANITY_STATUS_UNKNOWN;
  // Run VACUUM on DvbDb for a sanity check. Then check for an error code.
  VacuumAudit();

  switch(Sqlite3ppWrapper.error_code()) {
  case SQLITE_NOTADB:
  case SQLITE_FORMAT:
  case SQLITE_CORRUPT:
  case SQLITE_IOERR:
  case SQLITE_PROTOCOL:
  case SQLITE_INTERNAL:
    status = SANITY_STATUS_CORRUPT;
    OS_LOG(DVB_ERROR,  "<%s> - Db is corrupt!\n", __FUNCTION__);
    break;
  case SQLITE_FULL:
    OS_LOG(DVB_ERROR,  "<%s> - Db is SANITY_STATUS_FULL!\n", __FUNCTION__);
    status = SANITY_STATUS_FULL;  // file limit size exceeded - not good.
    break;
  case SQLITE_OK:
    OS_LOG(DVB_DEBUG,  "<%s> - Db OK, checking content!\n", __FUNCTION__);
    status = ContentCheck();
    break;
  default:
    status = SANITY_STATUS_UNKNOWN; // could be benign
    break;
  }
  return  status;
}

// Content check to determine if the database has DVB SI data
TDvbDb::TSanityStatus TDvbDb::ContentCheck()
{
  TSanityStatus status(SANITY_STATUS_EMPTY);
  const char*  queryStr("SELECT nit_pk FROM Nit;");
  try {
    sqlite3pp::query qry(Sqlite3ppWrapper, queryStr);
    if (qry.column_count() != 1) {
      OS_LOG(DVB_DEBUG,  "<%s> - Query must contain 1 column - it has %d: %s\n", __FUNCTION__, qry.column_count(), queryStr);
      throw new logic_error("Query should contain only one column.");
    }
    int32_t  nitCount(0);
    for (query::iterator it=qry.begin(); it != qry.end(); ++it) {
      int64_t  nit_pk = 0;
      long long int tmp;
      (*it).getter() >> tmp;
      nit_pk = static_cast<int64_t>(tmp);
      if (nit_pk > 0) {
        ++nitCount;
      }
    }
    if (nitCount > 0) {
      status = SANITY_STATUS_POPULATED;
    }
  }
  catch (exception& ex) {
    OS_LOG(DVB_DEBUG,  "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), queryStr);
  }
  catch (...) {
    OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, queryStr);
  }
  return  status;
}

// Most recent start timestamp stored
uint64_t TDvbDb::LatestStartTime()
{
  long long int latestStartTime = 0;
  const char*  queryStr = "SELECT MAX(start_time) FROM Eit;";

  try {
    sqlite3pp::query qry(Sqlite3ppWrapper, queryStr);
    if (qry.column_count() != 1) {
      throw new logic_error("Query should contain only one column.");
    }
    for (query::iterator it=qry.begin(); it != qry.end(); ++it) {
      (*it).getter() >> latestStartTime;
       OS_LOG(DVB_DEBUG,  "<%s> - querylatestStartTime = %lld, %s\n", __FUNCTION__, latestStartTime, queryStr);
    }
  }
  catch (exception& ex) {
    OS_LOG(DVB_DEBUG,  "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), queryStr);
  }
  catch (...) {
    OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, queryStr);
  }
  return  latestStartTime;
}

size_t  TDvbDb::GetEnvVariablesFromDb()
{
  ClearScanSettings();
  SqlCommand("CREATE TABLE IF NOT EXISTS ScanSettings (" \
    "variable TEXT,"                            \
    "value TEXT);");
  const char* queryStr = "SELECT variable, value FROM ScanSettings";
  try {
    sqlite3pp::query qry(Sqlite3ppWrapper, queryStr);
    if (qry.column_count() != 2) {
      throw new logic_error("Query should contain only one column.");
    }
    for (query::iterator it=qry.begin(); it != qry.end(); ++it) {
      TDbSetting setting;
      (*it).getter() >> setting.EnvironmentVariableName >> setting.EnvironmentVariableValue;
      OS_LOG(DVB_DEBUG,  "<%s> - Retrieved variable: %s = %s\n", __FUNCTION__, 
        setting.EnvironmentVariableName.c_str(), setting.EnvironmentVariableValue.c_str());
      DbSettingsVector.push_back(setting);
    }
  }
  catch (exception& ex) {
    OS_LOG(DVB_DEBUG,  "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), queryStr);
  }
  catch(...) {
    OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, queryStr);
  }
  return  static_cast<size_t>(DbSettingsVector.size());
}

const char*  TDvbDb::GetScanSetting(const char* varStr)
{
  for (auto it = DbSettingsVector.cbegin(); it != DbSettingsVector.cend(); ++it) {
    const TDbSetting& setting = (*it);
    if (setting.EnvironmentVariableName.compare(varStr) == 0) {
      return  setting.EnvironmentVariableValue.c_str();
    }
  }
  return  NULL;
}

void TDvbDb::SetScanSetting(const char* variable, const char* value)
{
  if (variable && value) {
    OS_LOG(DVB_DEBUG,  "<%s> - Saving variable: %s = %s\n", __FUNCTION__, variable, value);
    string cmdStr("INSERT OR REPLACE INTO ScanSettings (variable, value ) VALUES (?, ?);");
    try {
      command cmd(Sqlite3ppWrapper, cmdStr.c_str());
      cmd.binder() << variable << value;
      cmd.execute();
    }
    catch (exception& ex) {
      OS_LOG(DVB_DEBUG,   "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), cmdStr.c_str());
    }
    catch (...) {
      OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, cmdStr.c_str());
    }
  }
}

void  TDvbDb::ClearScanSettings()
{
  DbSettingsVector.clear();
}

int64_t TDvbDb::FindPrimaryKey(string& queryStr)
{
  std::lock_guard<std::mutex> lock(DbMutex);
  int64_t key(0);

  try {
    sqlite3pp::query qry(Sqlite3ppWrapper, queryStr.c_str());
    if (qry.column_count() != 1) {
      throw new logic_error("Query should contain only one column.");
    }
    for (query::iterator it=qry.begin(); it != qry.end(); ++it) {
      long long int tmp;
      (*it).getter() >> tmp;
      key = static_cast<int64_t>(tmp);
    }
  }
  catch (exception& ex) {
    OS_LOG(DVB_DEBUG,   "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), queryStr.c_str());
  }
  catch (...) {
    OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, queryStr.c_str());
  }
  return  key;
}

// Find the primary key (rowid) and version
// @param queryStr string containing the SQL query to find the primary key
// @param version int8_t retreive the version column value specified in the queryStr parameter
// @return int64_t returns the rowid value from the last db insert operation
int64_t TDvbDb::FindPrimaryKey(string& queryStr, int8_t& version)
{
  std::lock_guard<std::mutex> lock(DbMutex);
  int64_t key(0);

  try {
    sqlite3pp::query qry(Sqlite3ppWrapper, queryStr.c_str());
    if (qry.column_count() != 2) {
      throw new logic_error("Query should contain two columns.");
    }
    for (query::iterator it=qry.begin(); it != qry.end(); ++it) {
      long long int tmp;
      (*it).getter() >> tmp >> version;
      key = static_cast<int64_t>(tmp);
    }
  }
  catch (exception& ex) {
    OS_LOG(DVB_DEBUG,  "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), queryStr.c_str());
  }
  catch (...) {
    OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, queryStr.c_str());
  }
  return  key;
}

int64_t TDvbDb::GetLastInsertedRowId()
{
  std::lock_guard<std::mutex> lock(DbMutex);
  return  static_cast<const int64_t>(Sqlite3ppWrapper.last_insert_rowid());
}

int32_t TDvbDb::GetNumberOfRowsModified()
{
  std::lock_guard<std::mutex> lock(DbMutex);
  return  static_cast<int32_t>(sqlite3_changes(Sqlite3ppWrapper.getHandle()));
}

vector<shared_ptr<TMpegDescriptor>>  TDvbDb::FindDescriptor(string queryStr)
{
  std::lock_guard<std::mutex> lock(DbMutex);
  vector<shared_ptr<TMpegDescriptor>>  results;
  try {
    if (!queryStr.empty()) {
      OS_LOG(DVB_ERROR,  "<%s> - No query specified.\n", __FUNCTION__);
      throw new logic_error("Invalid query string.");
    }

        sqlite3pp::query qry(Sqlite3ppWrapper, queryStr.c_str());

        if(qry.column_count() != 2 || 
            strcmp(qry.column_name(0), "descriptor_id") != 0 || 
            strcmp(qry.column_name(1), "descriptor") != 0)
        {
            OS_LOG(DVB_DEBUG,  "<%s> - Invalid columns specified.\n", __FUNCTION__);
            throw new logic_error("Invalid column selected.");
        }

        for(query::iterator it=qry.begin(); it != qry.end(); ++it)
        {
            string descriptor;
            int8_t descriptor_id;

            (*it).getter() >> descriptor_id >> descriptor;

            shared_ptr<TMpegDescriptor> mpeg(new TMpegDescriptor(static_cast<TDescriptorTag>(descriptor_id), 
                                                               (uint8_t*)descriptor.data(), 
                                                                static_cast<uint8_t>(descriptor.size())));
                results.push_back(mpeg);
        }
    }

    catch(exception& ex)
    {
        OS_LOG(DVB_DEBUG,  "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), queryStr.c_str());
    }

    catch(...)
    {
        OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, queryStr.c_str());
    }

    return  results;
}
int32_t  TDvbDb::InsertDescriptor(const char* tableName, const int64_t& fkey, const std::vector<TMpegDescriptor>& descList)
{
  std::lock_guard<std::mutex> lock(DbMutex);
  int32_t rc(-1);

  if (tableName && fkey > 0 && !descList.empty()) {
    // Using 'INSERT OR IGNORE' so duplicate entries do not trigger an exception or error.
    string cmdStr("INSERT OR IGNORE INTO ");
    string tableStr(tableName);
    cmdStr += tableStr;
    cmdStr += " (fkey, descriptor_id, descriptor) VALUES (?, ?, ?);";
    try {
      for (auto it = descList.cbegin(); it != descList.cend(); ++it) {
        const TMpegDescriptor& md = *it;
        command cmd(Sqlite3ppWrapper, cmdStr.c_str());
        cmd.binder() << static_cast<long long int>(fkey) << static_cast<uint8_t>(md.GetDescriptorTag());
        const std::vector<uint8_t>& descData = md.GetDescriptorData();
        cmd.bind(3, static_cast<const void*>(descData.data()), descData.size());
        cmd.execute();
        rc = 0;
      }
    }
    catch (exception& ex) {
      OS_LOG(DVB_DEBUG, "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), cmdStr.c_str());
    }
    catch (...) {
      OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, cmdStr.c_str());
    }
  }
  return rc;
}

// Insert Component vector of descriptors
// @param tablename const char pointer of the name of the descriptor table
// @param fkey int64_t parent foreign key value
// @param descList vector of Component descriptors
// @return int32_t returns the status of the operation 0 if successful, -1 for failure
int32_t  TDvbDb::InsertComponent(const char* tableName, const int64_t& fkey, const std::vector<TMpegDescriptor>& descList)
{
  std::lock_guard<std::mutex> lock(DbMutex);
  int32_t rc(-1);
  for (auto it = descList.cbegin(); it != descList.cend(); ++it) {
    const TMpegDescriptor& md = *it;
    if (md.GetDescriptorTag() == TDescriptorTag::COMPONENT_TAG) {
      TComponentDescriptor cd(md);
      OS_LOG(DVB_DEBUG,  "<%s> %s COMPONENT fkey %ld con: %d type: %d tag: %d lang: %s text: %s\n",
        __FUNCTION__, tableName, fkey, static_cast<int>(cd.GetStreamContent()), static_cast<int>(cd.GetComponentType()), 
        static_cast<int>(cd.GetComponentTag()), cd.GetLanguageCode().c_str(), cd.GetText().c_str());

      string cmdStr = "INSERT OR IGNORE INTO ";
      cmdStr += tableName;
      cmdStr += " (fkey, stream_content, component_type, component_tag, iso_639_language_code, description) " \
        " VALUES (?, ?, ?, ?, ?, ?);";
      try {
        command cmd(Sqlite3ppWrapper, cmdStr.c_str());
        cmd.binder() << static_cast<long long int>(fkey)
          << static_cast<int>(cd.GetStreamContent())
          << static_cast<int>(cd.GetComponentType())
          << static_cast<int>(cd.GetComponentTag())
          << cd.GetLanguageCode()
          << cd.GetText();
        cmd.execute();
        //int64_t component_fk = GetLastInsertedRowId();
      }
      catch (std::exception& ex) {
        OS_LOG(DVB_DEBUG,   "<%s> - Exception: %s, %s\n", __FUNCTION__, ex.what(), cmdStr.c_str());
      }
      catch (...) {
        OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, cmdStr.c_str());
      }
    }
  }
  return rc;
}

// Audits for database maintenance
void TDvbDb::Audits()
{
  // std::lock_guard<std::mutex> lock(DbMutex);
  // static uint32_t count = 0;

  TElapseTime elapse;
  elapse.StartTimeMeasurement();
  OS_LOG(DVB_DEBUG,  "%s(): Running Db audits\n", __FUNCTION__);

  // TODO: Enable audits
  // PerformUpdate();
  // StaleAudit();
  // PurgeAudit();
  // VacuumAudit();

  elapse.FinishTimeMeasurement();
  elapse.Display();
}

void TDvbDb::AddUpdate(string updateCmd)
{
  std::lock_guard<std::mutex> lock(DbMutex);
  if (!updateCmd.empty()) {
    bool found = false;
    for (auto it = UpdateList.begin(); it != UpdateList.end(); ++it) {
      TRepair& repair = (*it);
      if (repair.SqlUpdateCommandString.compare(updateCmd) == 0) {
        found = true;
        break;
      }
    }
    if (!found) {
      TRepair* repair = new TRepair(updateCmd);
      if (repair) {
        UpdateList.push_back(*repair);
      }
    }
  }
}

void TDvbDb::DumpUpdate()
{
  std::lock_guard<std::mutex> lock(DbMutex);
  int32_t i(0);
  for (auto it = UpdateList.begin(); it != UpdateList.end(); ++it,++i) {
    TRepair& repair = (*it);
    OS_LOG(DVB_DEBUG,  "<%s> %d> SqlCommandStatus %d, count %d, %s\n",
      __FUNCTION__,i,repair.SqlCommandStatus,repair.SqlCmdUpdateCount,repair.SqlUpdateCommandString.c_str());
  }
}

void TDvbDb::ClearUpdate()
{
  UpdateList.clear();
}

// Execute update commands contained in the collection
void TDvbDb::PerformUpdate()
{
  std::lock_guard<std::mutex> lock(DbMutex);
  OS_LOG(DVB_DEBUG,  "<%s> TRepair size: %lu\n", __FUNCTION__, UpdateList.size());
  for (auto it = UpdateList.begin(); it != UpdateList.end(); ++it) {
    TRepair& repair = (*it);
    if (!repair.SqlCommandStatus) {
      int32_t rc = SqlCommand(repair.SqlUpdateCommandString.c_str());
      int32_t mods = GetNumberOfRowsModified();
      repair.SqlCmdUpdateCount++;
      if (rc == 0 && mods > 0) {
        repair.SqlCommandStatus = true;  // mark as fixed.
        OS_LOG(DVB_DEBUG,  "<%s> FIXED! rc=%d,count=%d,SqlCommandStatus=%d,mods=%d\n", __FUNCTION__, rc, 
          repair.SqlCmdUpdateCount, repair.SqlCommandStatus, mods);
      }
      else {
        OS_LOG(DVB_DEBUG,  "<%s> rc=%d,count=%d,mods=%d\n", __FUNCTION__, rc, repair.SqlCmdUpdateCount, mods);
      }
    }
  }
  // Update sometimes reports a changed tuple.
  UpdateList.remove_if(TRepair::GetSqlCommandStatus);
  UpdateList.remove_if(TRepair::IsSqlUpdateCommandCountExpired);
}

// execute a single step SQL command
// @param cmdStr string containing the SQL command
// @return int32_t returns the status of the operation 0 if successful, -1 for failure
int32_t TDvbDb::SqlCommand(string cmdStr)
{
  std::lock_guard<std::mutex> lock(DbMutex);
  return  SqlCommand(cmdStr.c_str());
}

// Execute a single SQL command.
// NOTE: Private method does not lock mutex
int32_t TDvbDb::SqlCommand(const char* cmdStr)
{
  int32_t rc(-1);
  if (cmdStr) {
    try {
      command cmd(Sqlite3ppWrapper, cmdStr);
      cmd.execute();
      rc = 0;  // success
    }
    catch (exception& ex) {
      OS_LOG(DVB_DEBUG,  "<%s> - Exception: %s cmd: %s\n", __FUNCTION__, ex.what(), cmdStr);
    }
    catch (...) {
      OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, cmdStr);
        }
  }
  return  rc;
}

void TDvbDb::CreateTables()
{
  std::lock_guard<std::mutex> lock(DbMutex);
  CreateSchema();
}

void TDvbDb::CreateSchema()
{
  // DO NOT LOCK Mutex!
  if (Sqlite3ppWrapper.error_code() != 0) {
    OS_LOG(DVB_ERROR,  "<%s> - invalid database: %s\n", __FUNCTION__, DbFileName.c_str());
    return;
  }
  OS_LOG(DVB_DEBUG,  "<%s> - Creating schema.\n", __FUNCTION__);
  for (auto it=DatabaseSchema.cbegin(); it != DatabaseSchema.cend(); ++it) {
    const string& cmd = *it;
    if (!cmd.empty()) {
      SqlCommand(cmd.c_str());
    }
  }
}

void TDvbDb::DropTables()
{
  std::lock_guard<std::mutex> lock(DbMutex);
  DropSchema();
}

void TDvbDb::DropSchema()
{
  // Drop the leaf (child) tables first then drop the parent tables 
  // otherwise a foreign key constraint exception will be thrown.

  OS_LOG(DVB_DEBUG,  "<%s> - Dropping tables\n", __FUNCTION__);

  for (auto it=DatabaseTables.cbegin(); it != DatabaseTables.cend(); ++it) {
    const string& tableName = *it;
    if (!tableName.empty()) {
      string cmdStr("DROP TABLE IF EXISTS ");
      cmdStr += tableName;
      cmdStr += ";";   // terminate SQL command
      SqlCommand(cmdStr.c_str());
    }
  }
}

// Perform garbage collection on database file.
void TDvbDb::VacuumAudit()
{
  SqlCommand("VACUUM;");
}

// Remove expired database records
void TDvbDb::PurgeAudit()
{
  transaction trans(Sqlite3ppWrapper);

  // TODO: make the expired time configurable.
  SqlCommand("DELETE FROM Eit WHERE start_time + duration < date('now','-1 hour');");

  SqlCommand("DELETE FROM EitDescriptor WHERE fkey NOT IN " \
    " (SELECT DISTINCT eit_pk FROM Eit);");

  SqlCommand("DELETE FROM Event WHERE start_time + duration < date('now','-1 hour');"); 

  SqlCommand("DELETE FROM EventItem WHERE event_fk NOT IN " \
    " (SELECT DISTINCT event_pk FROM Event);");

  SqlCommand("DELETE FROM EventComponent WHERE fkey NOT IN " \
    " (SELECT DISTINCT event_pk FROM Event);");

  trans.commit();
}

// Query the database given a SQL statement
// @param cmd string containing the SQL command (select)
// @return a vector of vector of strings containing the results of the query
vector<vector<string>> TDvbDb::QueryDb(string& cmd)
{
  std::lock_guard<std::mutex> lock(DbMutex);
  vector<vector<string>>  results;

  if (!cmd.empty()) {
    try {
      sqlite3_stmt* statement;
      if (sqlite3_prepare_v2(Sqlite3ppWrapper.getHandle(), cmd.c_str(), cmd.size(), &statement, NULL) == SQLITE_OK) {
        size_t numColumns = sqlite3_column_count(statement);
        while(true) {
          int rc = sqlite3_step(statement);  
          if (rc != SQLITE_ROW) {
            break;
          }
          vector<string>  row;
          for (size_t i=0; i < numColumns; ++i) {
            string column;
            const char* text = (const char*)sqlite3_column_text(statement, i);
            if (text == NULL) {
              column = "";
            }
            else {
              column = text;
            }
            row.emplace_back(column);
          }
          if (!row.empty()) {
            results.emplace_back(row);
          }
        }
        sqlite3_finalize(statement);
      }
      else {
        OS_LOG(DVB_DEBUG,  "<%s> Unable to prepare statement: %s\n", __FUNCTION__, cmd.c_str());
      }
    }

    catch(exception& ex) {
      OS_LOG(DVB_DEBUG,   "<%s> - Exception: %s\n", __FUNCTION__, ex.what());
    }
    catch(...) {
      OS_LOG(DVB_ERROR,  "<%s> - Unknown Exception: cmd: %s\n", __FUNCTION__, cmd.c_str());
    }
  }
  return results;
}

TDvbDb::TCommand::TCommand(TDvbDb& db, string cmdStr)
  : DvbDbHandle(db)
{
  std::lock_guard<std::mutex> lock(db.DbMutex);

  if (!cmdStr.empty()) {
    Sqlite3ppWrapperCommand.reset(new command(DvbDbHandle.Sqlite3ppWrapper, cmdStr.c_str()));
    if (Sqlite3ppWrapperCommand) {
      CommandObjectStatus = true;
    }
  }
}

TDvbDb::TCommand::~TCommand()
{
  if (Sqlite3ppWrapperCommand) {
    Sqlite3ppWrapperCommand.reset();
  }
}

// Bind variable to SQL command statement.
// @param int32_t index (1's base) of the argument to bind the NULL TYPE variable to
// @return int32_t returns the status of the operation 0 if successful, -1 for failure
int32_t TDvbDb::TCommand::Bind(int index)
{
  int32_t rc(-1);

  if (Sqlite3ppWrapperCommand) {
    // NOTE: binding Database NULL type
    Sqlite3ppWrapperCommand->bind(index, null_type()); 
    rc = 0;
  }
  return rc;
}

// Bind variable to SQL command statement.
// @param int32_t index (1's base) of the argument to bind the variable to
// @param int variable to bind
// @return int32_t returns the status of the operation 0 if successful, -1 for failure
int32_t TDvbDb::TCommand::Bind(int index, int value)
{
  int32_t rc(-1);

  if (Sqlite3ppWrapperCommand) {
    Sqlite3ppWrapperCommand->bind(index, value);
    rc = 0;
  }
  return rc;
}

// Bind variable to SQL command statement.
// @param int32_t index (1's base) of the argument to bind the variable to
// @param long long int variable to bind
// @return int32_t returns the status of the operation 0 if successful, -1 for failure
int32_t TDvbDb::TCommand::Bind(int index, long long int value)
{
  int32_t rc(-1);

  if (Sqlite3ppWrapperCommand) {
    Sqlite3ppWrapperCommand->bind(index, value);
    rc = 0;
  }
  return rc;
}

// Bind variable to SQL command statement.
// @param int32_t index (1's base) of the argument to bind the variable to
// @param string variable to bind
// @return int32_t returns the status of the operation 0 if successful, -1 for failure
int32_t TDvbDb::TCommand::Bind(int index, string& value)
{
  int32_t rc(-1);

  if (Sqlite3ppWrapperCommand) {
    Sqlite3ppWrapperCommand->bind(index, value);
    rc = 0;
  }
  return rc;
}

int32_t TDvbDb::TCommand::Execute()
{
  std::lock_guard<std::mutex> lock(DvbDbHandle.DbMutex);
  int32_t rc(-1);

  if (Sqlite3ppWrapperCommand) {
    try {
      Sqlite3ppWrapperCommand->execute();
      rc = 0;
    }
    catch (exception& ex) {
      OS_LOG(DVB_DEBUG,  "<%s> - Exception: %s\n", __FUNCTION__, ex.what());
    }
    catch (...) {
      OS_LOG(DVB_DEBUG,  "<%s> - Unknown exception.\n", __FUNCTION__);
    }
  }
  return rc;
}

int32_t TDvbDb::TCommand::Execute(int64_t& fkey)
{
  std::lock_guard<std::mutex> lock(DvbDbHandle.DbMutex);
  fkey = 0;
  int32_t rc(-1);

  if (Sqlite3ppWrapperCommand) {
    try {
      Sqlite3ppWrapperCommand->execute();
      fkey = static_cast<const int64_t>(DvbDbHandle.Sqlite3ppWrapper.last_insert_rowid());
      rc = 0;
    }
    catch (exception& ex) {
      OS_LOG(DVB_DEBUG,   "<%s> - Exception: %s\n", __FUNCTION__, ex.what());
    }
    catch(...) {
      OS_LOG(DVB_DEBUG,  "<%s> - Unknown exception.\n", __FUNCTION__);
    }
  }
  return rc;
}

TDvbDb::TTransaction::TTransaction(TDvbDb& handle)
  : DvbDbHandle(handle),
    TransctionObjectStatus(false)
{
  Sqlite3ppWrapperTransaction.reset(new transaction(DvbDbHandle.Sqlite3ppWrapper));
  if (Sqlite3ppWrapperTransaction) {
    TransctionObjectStatus = true;
  }
}

TDvbDb::TTransaction::~TTransaction()
{
  Sqlite3ppWrapperTransaction.reset();
}

void TDvbDb::TTransaction::CommitSqlStatements()
{
  if (Sqlite3ppWrapperTransaction) {
    Sqlite3ppWrapperTransaction->commit();
  } 
}

// Rollback or do NOT commit the group of SQL statement to the database
void TDvbDb::TTransaction::RollBackSqlStatement()
{
  if (Sqlite3ppWrapperTransaction) {
    Sqlite3ppWrapperTransaction->rollback();
  } 
}
