// DVB_SI for Reference Design Kit (RDK)
//
// Copyright (C) 2015 Santhosh K S (Santhosh.KS@arris.com), Manu Shrot (Manu.Shrot@arris.com), Dmitry Barablin & Stan Partridge (stan.partridge@arris.com)
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

#ifndef TSECTIONLIST_H
#define TSECTIONLIST_H

// C system includes
#include <stdint.h>

// C++ system includes
#include <list>
#include <string>

// Project's includes
#include "TSiSection.h"

// Forward declarations
class TSiTable;
class TNitTable;
class TBatTable;
class TSdtTable;
class TEitTable;
class TTotTable;
class TUdtTable;

/**
 * Section list class
 */
class TSectionList
{
private:
  bool IsComplete;
  uint8_t FirstReceivedSectionNumber;
  std::list<TSiSection> SiSectionList;
  void InsertSectionIntoList(TSiSection& section);

  //  Check if the section list is complete (all sections of the table are in place)
  bool IsSectionListComplete(TSiSection& lastRcvdSect);
  void InitializeSectionList(TSiSection& section);
  bool IsEit(uint8_t tableId);

  TNitTable* BuildNit();
  TBatTable* BuildBat();
  TSdtTable* BuildSdt();
  TEitTable* BuildEit();
  TTotTable* BuildTot();
  TUdtTable* BuildUdt();

public:
  TSectionList();
  virtual ~TSectionList();

  bool AddSiSection(TSiSection& section);

  // TODO: KSS chk if this needs to be private?
  TSiTable* BuildTable();

  bool GetCompletenessFlag() const
  {
    return IsComplete;
  }

  std::string ToString() const;

};

#endif /* TSECTIONLIST_H */
