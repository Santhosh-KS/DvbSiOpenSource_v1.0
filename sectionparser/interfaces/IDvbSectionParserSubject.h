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

#ifndef IDVBSECTIONPARSERSUBJECT_H 
#define IDVBSECTIONPARSERSUBJECT_H

#include "IDvbSectionParserObserver.h"

class IDvbSectionParserSubject
{
public:
  virtual void RegisterDvbSectionParserObserver(IDvbSectionParserObserver* observerObject) = 0;
  virtual void RemoveDvbSectionParserObserver(IDvbSectionParserObserver* observerObject) = 0;
  virtual void NotifyDvbSectionParserObserver(uint32_t eventType, void *eventData, size_t dataSize) = 0;
};

#endif // IDVBSECTIONPARSERSUBJECT_H
