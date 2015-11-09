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

#include <cstddef>
#include "TMpegDescriptor.h"

using std::vector;

TMpegDescriptor::TMpegDescriptor(TDescriptorTag tag, uint8_t* data, uint8_t length): DescriptorTag(tag)
{
    DescriptorDataVector.assign(data, data+length);
}

/**
 * Parse descriptors
 *
 * @param p pointer to the raw data
 * @param length length of the raw data
 * @return vector of descriptors
 */
vector<TMpegDescriptor> TMpegDescriptor::ParseMpegDescriptors(uint8_t* p, uint16_t length)
{
    vector<TMpegDescriptor> ret;

    // Sanity check
    if(p)
    {
        uint8_t *end = p + length;

        // A valid descriptor should have at least two bytes (tag + length)
        while(p + 2 <= end)
        {
            TDescriptorTag tag = static_cast<TDescriptorTag>(p[0]);
            uint8_t len = p[1];

            // Boundary check
            if((len + 2) <= (end - p))
            {
                // Let's now add the descriptor (tag + data) to the vector
                ret.emplace_back(tag, p + 2, len);
            }

            p += len + 2;
        }
    }

    return ret;
}

const TMpegDescriptor* TMpegDescriptor::FindMpegDescriptor(const vector<TMpegDescriptor>& list, TDescriptorTag tag)
{
    const TMpegDescriptor *ret = NULL;

    // Let's find a descriptor by given tag in the given vector
    for(auto it = list.begin(), end = list.end(); it != end; ++it)
    {
        // found?
        if(it->GetDescriptorTag() == tag)
        {
            ret = &(*it);
            break;
        }
    }

    return ret;
}

vector<TMpegDescriptor> TMpegDescriptor::FindAllMpegDescriptors(const vector<TMpegDescriptor>& list, TDescriptorTag tag)
{
    vector<TMpegDescriptor> ret;

    // Let's find all descriptors with a given tag in the given vector
    for(auto it = list.begin(), end = list.end(); it != end; ++it)
    {
        if(it->GetDescriptorTag() == tag)
        {
            ret.push_back(*it);
        }
    }

    return ret;
}
