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

#ifndef SISECTION_H_
#define SISECTION_H_

// C system includes
#include <stdint.h>
#include <cstddef>

// C++ system includes
#include <vector>

/**
 * SI Section structure
 */
struct TSiSection
{
    /**
     * @param data section data
     * @param len data length
     */
    TSiSection(uint8_t *data, size_t len);

    /**
     * Table Identifier
     * Defines the structure of the syntax section and other contained data.
     */
    uint8_t TableId;

    /**
     * Section syntax indicator
     * A flag that indicates if the syntax section follows the section length.
     */
    bool SectionSyntaxIndicator;

    /**
     * Section length
     * The number of bytes that follow for the syntax section (with CRC value) and/or table data.
     */
    uint16_t SectionLength;

    /**
     * Table ID extension
     * Used for identification of a sub_table
     */
    uint16_t ExtensionTableId;

    /**
     * Version number
     * Incremented when data is changed and wrapped around on overflow for values greater than 32.
     */
    uint8_t VersionNumber;

    /**
     * Current/next indicator
     * Indicates if data is current in effect or is for future use.
     * If the bit is flagged on, then the data is to be used at the present moment.
     */
    bool CurrentNextIndicator;

    /**
     * Section number
     * This is an index indicating which table this is in a related sequence of tables.
     * The first table starts from 0.
     */
    uint8_t SectionNumber;

    /**
     * Last section number
     * This indicates which table is the last table in the sequence of tables.
     */
    uint8_t LastSectionNumber;

    /**
     * Table data
     */
    std::vector<uint8_t> Payload;
};

#endif /* SISECTION_H_ */
