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

#include "TDvbJanssonParser.h"
#include <cstddef>
#include "oswrap.h"
#include "jansson.h"

TDvbJanssonParser::TDvbJanssonParser(const std::string& jsonfile)
  : DvbConfigProfileFile(jsonfile)
{
  // Empty
}

TDvbJanssonParser::~TDvbJanssonParser()
{
  // Empty
}

std::vector<std::shared_ptr<TDvbStorageNamespace::InbandTableInfoStruct>> TDvbJanssonParser::GetInbandTableInfo(std::string& profile)
{
  std::vector<std::shared_ptr<TDvbStorageNamespace::InbandTableInfoStruct>> ret;
  OS_LOG(DVB_DEBUG,   "%s:%d: called\n", __FUNCTION__, __LINE__);

  json_error_t error;
  json_t* json = json_load_file(DvbConfigProfileFile.c_str(), 0, &error);
  if (!json) {
    OS_LOG(DVB_ERROR,   "%s:%d: json_load_file(%s) failed, error = %s\n",
      __FUNCTION__, __LINE__, DvbConfigProfileFile.c_str(), error.text);
    return ret;
  }

  // Search for object with name == profile
  size_t size = json_array_size(json);
  OS_LOG(DVB_DEBUG,   "%s:%d: number of profiles = %d\n", __FUNCTION__, __LINE__, size);

  for (size_t i = 0; i < size; ++i) {
    json_t* obj = json_array_get(json, i);
    if (!obj) {
      continue;
    }
    json_t* name = json_object_get(obj, "name");
    if (!name) {
      continue;
    }
    OS_LOG(DVB_DEBUG,   "%s:%d: profile = %s\n", __FUNCTION__, __LINE__, json_string_value(name));
    if (profile.compare(json_string_value(name)) == 0) {
      OS_LOG(DVB_DEBUG,   "%s:%d: requested profile found\n", __FUNCTION__, __LINE__);
      // Get "tables" object
      json_t* tables = json_object_get(obj, "tables");
      if (tables) {
        OS_LOG(DVB_DEBUG,   "%s:%d: tables object found\n", __FUNCTION__, __LINE__);
        // Get "inband" array
        json_t* inband = json_object_get(tables, "inband");
        if (inband) {
          OS_LOG(DVB_DEBUG,   "%s:%d: inband array found\n", __FUNCTION__, __LINE__);
          // Read table objects
          size_t num = json_array_size(inband);
          for (size_t j = 0; j < num; j++) {
            json_t* t = json_array_get(inband, j);
            if (!t) {
              continue;
            }
            json_t* v = json_object_get(t, "pid");
            if (!v) {
              continue;
            }
            uint16_t pid = json_integer_value(v);
            v = json_object_get(t, "table_id");
            if (!v) {
              continue;
            }
            uint16_t tableId = json_integer_value(v);
            v = json_object_get(t, "table_id_ext");
            if (!v) {
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

std::string TDvbJanssonParser::GetProfiles()
{
  std::string res;
  json_error_t error;
  json_t* json = json_load_file(DvbConfigProfileFile.c_str(), 0, &error);
  if (json) {
    res = std::string(json_dumps(json, JSON_INDENT(2)|JSON_ENSURE_ASCII|JSON_PRESERVE_ORDER));
    json_decref(json);
  }
  else {
    OS_LOG(DVB_ERROR,   "%s:%d: json_load_file(%s) failed, error = %s\n",
      __FUNCTION__, __LINE__, DvbConfigProfileFile.c_str(), error.text);
  }
  return res;
}

bool TDvbJanssonParser::SetProfiles(std::string& profiles)
{
  bool ret(true);
  json_error_t error;
  json_t* json = json_loads(profiles.c_str(), 0, &error);
  if (json) {
    if (json_dump_file(json, DvbConfigProfileFile.c_str(), JSON_INDENT(2)|JSON_ENSURE_ASCII|JSON_PRESERVE_ORDER) == -1) {
      OS_LOG(DVB_ERROR,   "%s:%d: json_dump_file(%s) failed\n",
        __FUNCTION__, __LINE__, DvbConfigProfileFile.c_str());
      ret = false;
    }
    json_decref(json);
  }
  else {
    OS_LOG(DVB_ERROR,   "%s:%d: json_loads() failed, error = %s\n",
      __FUNCTION__, __LINE__, error.text);
    ret = false;
  }
  return ret;
}
