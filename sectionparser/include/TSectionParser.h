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
