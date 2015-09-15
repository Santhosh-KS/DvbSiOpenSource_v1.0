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

#ifndef IDVBSECTIONPARSEROBSERVER
#define IDVBSECTIONPARSEROBSERVER

#include "TDvbStorageNamespace.h"

class IDvbStorageObserver {
public:
  virtual void Tune(const uint32_t& freq, const TDvbStorageNamespace::TModulationMode& mod, const uint32_t& symbol, const uint8_t& tuneIndex) = 0;
  virtual void UnTune() = 0;
};

#endif // IDVBSECTIONPARSEROBSERVER 