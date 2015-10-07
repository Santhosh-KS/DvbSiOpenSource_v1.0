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
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA.

#ifndef TDVBDB_H
#define TDVBDB_H

#include "TDvbStorageNamespace.h"

// C system includes
#include <sys/time.h>

// C++ system includes
#include <string>
#include <list>
#include <vector>
#include <mutex>
#include <memory>
#include <iostream>

// Other libraries' includes
#include "sqlite3pp.h"


// Forward declarations
class TMpegDescriptor;

/**
 * TElapseTime class. Debug class used for runtime performance measurements.
 */
class TElapseTime
{
private:
  // system time data structure
  typedef struct timeval TSystemTime;
  TSystemTime SystemTime;
  double StartTime;
  double EndTime;

public:
  friend std::ostream& operator<<(std::ostream& os, TElapseTime& et);

  TElapseTime()
  : StartTime(0),
  EndTime(0)
  {
    // Empty
  };

  ~TElapseTime()
  {
    // Empty
  };

  void StartTimeMeasurement()
  {
    gettimeofday(&SystemTime, NULL);
    StartTime = SystemTime.tv_sec + (SystemTime.tv_usec / 1000000.0); \
  };

  void FinishTimeMeasurement()
  {
    gettimeofday(&SystemTime, NULL);
    EndTime = SystemTime.tv_sec + (SystemTime.tv_usec / 1000000.0);
    EndTime -= StartTime;
  };

  void Display();
};

typedef std::vector<std::string>  StringVector;

class TDvbDb
{
private:
class TRepair;
class TDbSetting;

  // Database - sanity return codes
  enum TSanityStatus {
    SANITY_STATUS_UNKNOWN,
    SANITY_STATUS_EMPTY,
    SANITY_STATUS_POPULATED,
    SANITY_STATUS_CORRUPT,
    SANITY_STATUS_FULL,
    SANITY_STATUS_STALE
  };

  static const StringVector DatabaseSchema;
  static const StringVector DatabaseTables;
  bool IsTotReceived;
  bool IsDataStale;
  std::mutex DbMutex;
  std::string DbFileName;

  sqlite3pp::database Sqlite3ppWrapper;
  std::list<TRepair> UpdateList;
  std::vector<TDbSetting> DbSettingsVector;

  // Disable Copy Constructor
  TDvbDb (const TDvbDb& rhs) = delete;
  TDvbDb& operator= (const TDvbDb& rhs) = delete;

  sqlite3pp::database& GetSqlite3ppHandle()
  {
    return Sqlite3ppWrapper;
  };


  // open database file
  // @param int flags SQLITE open/create flags SQLITE_OPEN_READWRITE or SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE
  // return TFileStatus CREATE enumeration upon success otherwise an error.
  TDvbStorageNamespace::TFileStatus OpenDb(int flags);

  // Create DVB database schema
  void CreateSchema();
  // Drop DVB database schema
  void DropSchema();
  // Remove expired database records
  void PurgeAudit();
  // Perform garbage collection on database file.
  void VacuumAudit();
  // Determine if contents have expired
  void StaleAudit();
  // Sanity check to determine if the database file is valid
  TSanityStatus SanityCheck();
  // Content check to determine if the database has DVB SI data
  TSanityStatus ContentCheck();
  // Stale check to determine if the database contents have expired
  TSanityStatus StaleCheck();
  // Most recent start timestamp stored
  uint64_t LatestStartTime();

  // NOTE: SqlCommand method does not lock mutex
  int32_t SqlCommand(const char* cmdStr);

  // Repair class. Used to perform update SQL commands later
  class TRepair
  {
  public:
    enum {
      NUM_TRIES = 3  // Maximum number of times to attempt to peform Update SQL command
     };
    bool SqlCommandStatus;
    int32_t SqlCmdUpdateCount;
    std::string  SqlUpdateCommandString;

    TRepair (std::string fStr)
      : SqlCommandStatus (false),
        SqlCmdUpdateCount (0),
        SqlUpdateCommandString (fStr)
    {
      // Empty
    }
    //  Update command succeded. Predicate function for remove_if operation to remove from collection
    //  @param rhs reference to a constant Repair object in collection
    //  @return bool true means to remove from collection; false to keep
    static bool GetSqlCommandStatus (const TRepair& rhs)
    {
      return  rhs.SqlCommandStatus;
    }

    //  Maximum number of attempts to perform update comamand. Predicate function for remove_if operation to remove from collection
    //  @param rhs reference to a constant Repair object in collection
    //  @return bool true means to remove from collection; false to keep
    static bool IsSqlUpdateCommandCountExpired (const TRepair& rhs)
    {
      return  (rhs.SqlCmdUpdateCount > static_cast<int32_t> (NUM_TRIES));
    }
  };

  // TDbSetting class stores the  Environment variables in the database.
  class TDbSetting
  {
  public:
    std::string  EnvironmentVariableName;
    std::string  EnvironmentVariableValue;
    TDbSetting() { /*Empty*/ }
    ~TDbSetting() { /*Empty*/ }

    TDbSetting(std::string var, std::string val)
    : EnvironmentVariableName(var),
      EnvironmentVariableValue(val)
    {
      // Empty
    };
  };

public:
  enum {
    CMD_STR_LEN = 512
  };
  TDvbDb();
  ~TDvbDb();
  TDvbStorageNamespace::TFileStatus CreateDbFile(const std::string& dbname);
  inline void UpdateTotStatus(bool val) { IsTotReceived = val; }
  inline bool GetTotStatus() { return  IsTotReceived; }

  void CloseDbFile();
  size_t GetEnvVariablesFromDb();
  const char* GetScanSetting(const char* variable);
  void SetScanSetting(const char* variable, const char* value);
  void ClearScanSettings();
  void CreateTables();
  void DropTables();
  int32_t GetNumberOfRowsModified();

  // Retreive the last rowid value from the previous insert db operation
  int64_t GetLastInsertedRowId ();
  
  // Find the primary key (rowid) value
  // @param queryStr string containing the SQL query to find the primary key
  // @return int64_t returns the rowid value from the last db insert operation
  int64_t FindPrimaryKey(std::string& queryStr);

  // Find the primary key (rowid) and version
  // @param queryStr string containing the SQL query to find the primary key
  // @param version int8_t retreive the version column value specified in the queryStr parameter
  // @return int64_t returns the rowid value from the last db insert operation
  int64_t FindPrimaryKey(std::string& queryStr, int8_t& version);

  // execute a single step SQL command
  // @param cmdStr string containing the SQL command
  // @return int32_t returns the status of the operation 0 if successful, -1 for failure
  int32_t SqlCommand(std::string cmdStr);

  // Find descriptors specified in SQL statement
  // @param queryStr string containing the SQL command
  // @return int32_t returns the status of the operation 0 if successful, -1 for failure
  std::vector<std::shared_ptr<TMpegDescriptor>> FindDescriptor(std::string queryStr);

  // Insert a vector of descriptors 
  // @param tablename const char pointer of the name of the descriptor table
  // @param fkey int64_t parent foreign key value
  // @param descList vector of TMpegDescriptors
  // @return int32_t returns the status of the operation 0 if successful, -1 for failure
    int32_t InsertDescriptor(const char* tableName, const int64_t& fkey, const std::vector<TMpegDescriptor>& descList);

  // Insert Component vector of descriptors 
  // @param tablename const char pointer of the name of the descriptor table
  // @param fkey int64_t parent foreign key value
  // @param descList vector of Component descriptors
  // @return int32_t returns the status of the operation 0 if successful, -1 for failure
    int32_t InsertComponent(const char* tableName, const int64_t& fkey, const std::vector<TMpegDescriptor>& descList);

  // Add an update command for processing at a later time.
  void AddUpdate(std::string updateCmd);

  // Dump the update command collection
  void DumpUpdate();

  // Clear the update command collection
  void ClearUpdate();

  // Execute update commands contained in the collection
  void PerformUpdate();

  // Audits for database maintenance
  void Audits();

  // Query the database given a SQL statement
  // @param cmd string containing the SQL command (select)
  // @return a vector of vector strings containing the results of the query
  std::vector<std::vector<std::string>> QueryDb(std::string& cmd);

  // Command class. Used for binding variables to SQL statements.
  class TCommand
  {
  private:
    bool CommandObjectStatus;
    TDvbDb& DvbDbHandle;
    std::shared_ptr<sqlite3pp::command> Sqlite3ppWrapperCommand;
    
    // Disable Copy Constructor
    TCommand();
    TCommand(const TCommand& other) = delete;
    TCommand& operator=(const TCommand&) = delete;
  public:
    TCommand(TDvbDb& handle, std::string cmdBuf);
    ~TCommand();

    inline bool IsCommandObjectValid() { return CommandObjectStatus; }

    // Bind variable to SQL command statement.
    // @param int32_t index (1's base) of the argument to bind the NULL TYPE variable to
    // @return int32_t returns the status of the operation 0 if successful, -1 for failure
    int32_t Bind(int32_t index);

    // Bind variable to SQL command statement.
    // @param int32_t index (1's base) of the argument to bind the variable to
    // @param int variable to bind
    // @return int32_t returns the status of the operation 0 if successful, -1 for failure
    int32_t Bind(int32_t index, int value);

    // Bind variable to SQL command statement.
    // @param int32_t index (1's base) of the argument to bind the variable to
    // @param long long int variable to bind
    // @return int32_t returns the status of the operation 0 if successful, -1 for failure
    int32_t Bind(int32_t index, long long int value);

    // Bind variable to SQL command statement.
    // @param int32_t index (1's base) of the argument to bind the variable to
    // @param string variable to bind
    // @return int32_t returns the status of the operation 0 if successful, -1 for failure
    int32_t Bind(int32_t index, std::string& value);

    // Execute SQL command statement.
    // @return int32_t returns the status of the operation 0 if successful, -1 for failure
    int32_t Execute();

    // Execute SQL command statement.
    // @param int64_t foreign key value from Db insert operation
    // @return int32_t returns the status of the operation 0 if successful, -1 for failure
    int32_t Execute(int64_t& fkey);
  };

  // TTransaction class. Used to group mutiple SQL statements as one Db operation
  class TTransaction
  {
  private:
    TDvbDb& DvbDbHandle;
    bool TransctionObjectStatus;
    std::shared_ptr<sqlite3pp::transaction> Sqlite3ppWrapperTransaction;
    
    // Disable Copy Constructor
    TTransaction();
    TTransaction(const TTransaction& other) = delete;
    TTransaction& operator=(const TTransaction&) = delete;
  public:
    TTransaction(TDvbDb& handle);
    ~TTransaction();

    // Commit group of SQL statements to the database
    void CommitSqlStatements();

    // Rollback or do NOT commit the group of SQL statement to the database
    void RollBackSqlStatement();
    inline bool IsTransactionObjectValid() { return  TransctionObjectStatus; }
  };
};

#endif // TDVBDB_H

