// DVB_SI for Reference Design Kit (RDK)
//
// Copyright (C) 2015 Santhosh KS(Santhosh.KS@arris.com) , Manu Shrot (Manu.Shrot@arris.com),
// Stan Partridge (Stan.Partridge@arris.com) & Dmitry Barablin
//
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

#ifndef DVBUTILS_H_
#define DVBUTILS_H_

// C system includes
#include <stdint.h>

// C++ system includes
#include <string>

/**
 * Convert a binary coded decimal (byte) to decimal
 *
 * @param bcd binary coded decimal (byte)
 * @return decimal
 */
uint16_t BcdByteToDec(uint8_t bcd);

/**
 * Convert a binary coded decimal to decimal
 *
 * @param bcd binary coded decimal
 * @return decimal
 */
uint16_t BcdToDec(uint16_t bcd);

/**
 * Convert a binary coded decimal to time
 *
 * @param encodedTime encoded time
 * @param hour
 * @param min
 * @param sec
 */
void BcdToTime(int64_t encodedTime, uint32_t& hour, uint32_t& min, uint32_t& sec);

/**
 * Convert Modified Julian Date to date
 *
 * @param encodedTime
 * @return time_t
 */
time_t MjdToDate (int64_t encodedTime);

/**
 * Decode text information that is coded as described in ETSI EN 300 468 annex A
 *
 * @param str string
 * @param len length
 * @return decoded text in UTF-8
 */
std::string DecodeText(const unsigned char *str, size_t len);

#endif /* DVBUTILS_H_ */
