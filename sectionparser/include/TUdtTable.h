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

#ifndef TUSERDEFINEDTABLE_H
#define TUSERDEFINEDTABLE_H

// C++ system includes
#include <vector>

// Other libraries' includes

// Project's includes
#include "TSiTable.h"
#include "TSiSection.h"

/**
 * User Defined Table class
 */
class TUdtTable: public TSiTable
{
private:
  std::vector<TSiSection> SiSectionVector;

public:
  TUdtTable(uint8_t id, uint16_t extId, uint8_t ver, bool cur)
    : TSiTable(id, extId, ver, cur)
  {
    // Empty
  }
  
  virtual ~TUdtTable()
  {
    // Empty
  }

  void AddSection(TSiSection& section)
  {
    SiSectionVector.push_back(section);
  }

  void AddSections(const std::vector<TSiSection>& sections)
  {
    SiSectionVector.insert(SiSectionVector.end(), sections.begin(), sections.end());
  }

  const std::vector<TSiSection>& GetSectionList() const
  {
    return SiSectionVector;
  }

  //void removeSections()
  void FlushSections()
  {
    SiSectionVector.clear();
  }
};

#endif /* TUSERDEFINEDTABLE_H */
