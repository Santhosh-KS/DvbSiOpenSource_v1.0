/*
* ============================================================================
* RDK MANAGEMENT, LLC CONFIDENTIAL AND PROPRIETARY
* ============================================================================
* This file (and its contents) are the intellectual property of RDK Management, LLC.
* It may not be used, copied, distributed or otherwise disclosed in whole or in
* part without the express written permission of RDK Management, LLC.
* ============================================================================
* Copyright (c) 2015 RDK Management, LLC. All rights reserved.
* ============================================================================
* ============================================================================
* Contributed by ARRIS Group, Inc.
* ============================================================================
*/

#ifndef TSECTIONPARSER_H
#define TSECTIONPARSER_H

#include <stdint.h>
#include <map>
#include <list>
#include <utility>
#include <memory>

#include "TSectionList.h"
#include "IDvbSectionParserSubject.h"
#include "IDvbSectionParserObserver.h"

typedef std::map<std::pair<uint8_t, uint16_t>, TSectionList> SectionMap_t;
//typedef void (*SendEventCallback) (void*, uint32_t, void*, size_t);

/**
 * TSectionParser
 *
 * Takes sections as an input and produces parsed tables as an output
 */
class TSectionParser : public IDvbSectionParserSubject
{
private:
  std::vector<IDvbSectionParserObserver*> ObserverVector;
  // Make TSectioParser singleton.
  // TODO: KSS check why this is made singleton
  TSectionParser(const TSectionParser& other);
  TSectionParser& operator=(const TSectionParser&);

  bool IsTableSupported(uint8_t tableId);
  SectionMap_t m_sectionMap;

public:
  TSectionParser();
  virtual ~TSectionParser();

  void ParseSiData(uint8_t *data, uint32_t size);

  // IDvbSectionParserSubject 
  virtual void RegisterDvbSectionParserObserver(IDvbSectionParserObserver* observerObject);
  virtual void RemoveDvbSectionParserObserver(IDvbSectionParserObserver* observerObject);
  virtual void NotifyDvbSectionParserObserver(uint32_t eventType, void *eventData, size_t dataSize);

};

#endif
