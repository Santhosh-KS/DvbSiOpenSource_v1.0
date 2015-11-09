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

#include "TSiSection.h"

#include "oswrap.h"

using std::vector;

TSiSection::TSiSection(uint8_t* data, size_t len)
    : ExtensionTableId(0),
      VersionNumber(0),
      CurrentNextIndicator(true),
      SectionNumber(0),
      LastSectionNumber(0)
{
    // Sanity check
    if(!data || len < 3)
    {
        OS_LOG(DVB_ERROR, "<%s> Invalid section: data = %p, len = %lu\n",
                __FUNCTION__, data, len);
        return;
    }

    // Table Identifier
    TableId = data[0];

    //Section syntax indicator
    SectionSyntaxIndicator = data[1] & 0x80;

    // Section length
    SectionLength = ((uint16_t)(data[1] & 0xf)) << 8 | data[2];

    uint8_t offset = 3;

    // Let's check the syntax
    if(SectionSyntaxIndicator)
    {
        // Table ID extension
        ExtensionTableId = (data[3] << 8) | data[4];

        // Version number
        VersionNumber = (data[5] >> 1) & 0x1f;

        // Current/next indicator
        CurrentNextIndicator = data[5] & 1;

        // Section number
        SectionNumber = data[6];

        // Last section number
        LastSectionNumber = data[7];
        offset = 8;
    }

    OS_LOG(DVB_TRACE3, "<%s> TableId = 0x%x, extId = 0x%x, SectionLength = %d, len = %lu\n",
            __FUNCTION__, TableId, ExtensionTableId, SectionLength, len);

    // Let's read the payload
    if((offset < len) && ((SectionLength + 3) <= len))
    {
        Payload.assign(data + offset, data + SectionLength + 3);
    }
    else
    {
        OS_LOG(DVB_ERROR, "<%s> TableId = 0x%x(data len = %lu), section SectionLength(%d) <= payload offset(%d)\n",
                __FUNCTION__, TableId, len, SectionLength, offset);
    }
}

