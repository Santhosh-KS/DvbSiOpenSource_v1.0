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

// C system includes
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <string.h>
#include <iconv.h>

// Other libraries' includes

// Project's includes
#include "oswrap.h"
#include "DvbUtils.h"

using std::string;

/**
 * Convert a binary coded decimal (byte) to decimal
 *
 * @param bcd binary coded decimal (byte)
 * @return decimal
 */
uint16_t BcdByteToDec(uint8_t bcd)
{
    return (bcd >> 4)*10 + (bcd & 0x0f);
}

/**
 * Convert a binary coded decimal to decimal
 *
 * @param bcd binary coded decimal
 * @return decimal
 */
uint16_t BcdToDec(uint16_t bcd)
{
    uint16_t res = 0;
    uint16_t multiplier = 1;
    uint16_t digit = 0;

    // Decoding the binary coded decimal
    // Each decimal digit is represented by a four-bit binary value
    while (bcd > 0)
    {
        // Get the digit
        digit = bcd & 0x0F;

        // Shift to the next one
        bcd >>= 4;

        // Adding the digit to the result
        res += multiplier * digit;

        // Shift the multiplier
        multiplier *= 10;
    }

    return res;
}

/**
 * Convert a binary coded decimal to time
 *
 * @param encodedTime encoded time
 * @param hour
 * @param min
 * @param sec
 */
void BcdToTime(int64_t encodedTime, uint32_t& hour, uint32_t& min, uint32_t& sec)
{
    hour = BcdToDec((encodedTime >> 16) & 0xFF);
    min = BcdToDec((encodedTime >> 8) & 0xFF);
    sec = BcdToDec(encodedTime & 0xFF);
}

/**
 * Convert Modified Julian Date to date
 *
 * @param encodedTime
 * @return time_t
 */
time_t MjdToDate (int64_t encodedTime)
{
    int32_t J, C, Y, M;
    int32_t mjd = encodedTime >> 24;
    uint32_t hour = 0;
    uint32_t min = 0;
    uint32_t sec = 0;

    // Let's decode the time first
    BcdToTime(encodedTime, hour, min, sec);

    // Decoding the date
    // Adapted from Fliegel/van Flandern ACM 11/#10 p 657 Oct 1968
    J = mjd + 2400001 + 68569;
    C = 4 * J / 146097;
    J = J - (146097 * C + 3) / 4;
    Y = 4000 * (J + 1) / 1461001;
    J = J - 1461 * Y / 4 + 31;
    M = 80 * J / 2447;
    int32_t day = J - 2447 * M / 80;
    J = M / 11;
    int32_t month = M + 2 - (12 * J);
    int32_t year = 100 * (C - 49) + Y + J;

    // Converting to time_t
    //struct tm time = {0};
    struct tm time;
    time.tm_mday = day;
    time.tm_mon = month - 1;
    time.tm_year = year - 1900;
    time.tm_hour = hour;
    time.tm_min = min;
    time.tm_sec = sec;

    return mktime(&time);
}

/**
 * Convert a Latin00 string to a UTF-8 string
 *
 * @param in input
 * @param len length
 * @return string in UTF-8
 */
static string LatinToUtf(const unsigned char *in, size_t len)
{
    // Sanity check
    if(!in || len == 0)
    {
        OS_LOG(DVB_ERROR,    "<%s> Invalid argument passed (%p, %lu)\n",
                __FUNCTION__, in, len);
        return string("");
    }

    string res;

    // Let's convert each character from Latin00 to UTF-8 one by one
    for(size_t i = 0; i < len; i++)
    {
        // As per Character Code Table 00 in Annex A of ETSI EN 300 468 DVB Spec
        switch(in[i])
        {
            case 0xa4:
                res += 0xe2; res += 0x82; res += 0xac;
                break;
            case 0xa6:
                continue;
                break;
            case 0xa8:
                res += 0xc2; res += 0xa4;
                break;
            case 0xa9:
                res += 0xe2; res += 0x80; res += 0x98;
                break;
            case 0xaa:
                res += 0xe2; res += 0x80; res += 0x9c;
                break;
            case 0xac:
                res += 0xe2; res += 0x86; res += 0x90;
                break;
            case 0xad:
                res += 0xe2; res += 0x86; res += 0x91;
                break;
            case 0xae:
                res += 0xe2; res += 0x86; res += 0x92;
                break;
            case 0xaf:
                res += 0xe2; res += 0x86; res += 0x93;
                break;
            case 0xb4:
                res += 0xc3; res += 0x97;
                break;
            case 0xb9:
                res += 0xe2; res += 0x80; res += 0x99;
                break;
            case 0xba:
                res += 0xe2; res += 0x80; res += 0x9d;
                break;
            case 0xc1:
                res += 0xcc; res += 0x80;
                break;
            case 0xc2:
                res += 0xcc; res += 0x81;
                break;
            case 0xc3:
                res += 0xcc; res += 0x82;
                break;
            case 0xc4:
                res += 0xcc; res += 0x83;
                break;
            case 0xc5:
                res += 0xcc; res += 0x84;
                break;
            case 0xc6:
                res += 0xcc; res += 0x86;
                break;
            case 0xc7:
                res += 0xcc; res += 0x87;
                break;
            case 0xc8:
                res += 0xcc; res += 0x88;
                break;
            case 0xca:
                res += 0xcc; res += 0x8a;
                break;
            case 0xcb:
                res += 0xcc; res += 0xa7;
                break;
            case 0xcd:
                res += 0xcc; res += 0x8b;
                break;
            case 0xce:
                res += 0xcc; res += 0xa8;
                break;
            case 0xcf:
                res += 0xcc; res += 0x8c;
                break;
            case 0xd0:
                res += 0xe2; res += 0x80; res += 0x95;
                break;
            case 0xd1:
                res += 0xc2; res += 0xb9;
                break;
            case 0xd2:
                res += 0xc2; res += 0xae;
                break;
            case 0xd3:
                res += 0xc2; res += 0xa9;
                break;
            case 0xd4:
                res += 0xe2; res += 0x84; res += 0xa2;
                break;
            case 0xd5:
                res += 0xe2; res += 0x99; res += 0xaa;
                break;
            case 0xd6:
                res += 0xc2; res += 0xac;
                break;
            case 0xd7:
                res += 0xc2; res += 0xa6;
                break;
            case 0xdc:
                res += 0xe2; res += 0x85; res += 0x9b;
                break;
            case 0xdd:
                res += 0xe2; res += 0x85; res += 0x9c;
                break;
            case 0xde:
                res += 0xe2; res += 0x85; res += 0x9d;
                break;
            case 0xdf:
                res += 0xe2; res += 0x85; res += 0x9e;
                break;
            case 0xe0:
                res += 0xe2; res += 0x84; res += 0xa6;
                break;
            case 0xe1:
                res += 0xc3; res += 0x86;
                break;
            case 0xe2:
                res += 0xc3; res += 0x90;
                break;
            case 0xe3:
                res += 0xc2; res += 0xaa;
                break;
            case 0xe4:
                res += 0xc4; res += 0xa6;
                break;
            case 0xe6:
                res += 0xc4; res += 0xb2;
                break;
            case 0xe7:
                res += 0xc4; res += 0xbf;
                break;
            case 0xe8:
                res += 0xc5; res += 0x81;
                break;
            case 0xe9:
                res += 0xc3; res += 0x98;
                break;
            case 0xea:
                res += 0xc5; res += 0x92;
                break;
            case 0xeb:
                res += 0xc2; res += 0xba;
                break;
            case 0xec:
                res += 0xc3; res += 0x9e;
                break;
            case 0xed:
                res += 0xc5; res += 0xa6;
                break;
            case 0xee:
                res += 0xc5; res += 0x8a;
                break;
            case 0xef:
                res += 0xc5; res += 0x89;
                break;
            case 0xf0:
                res += 0xc4; res += 0xb8;
                break;
            case 0xf1:
                res += 0xc3; res += 0xa6;
                break;
            case 0xf2:
                res += 0xc4; res += 0x91;
                break;
            case 0xf3:
                res += 0xc3; res += 0xb0;
                break;
            case 0xf4:
                res += 0xc4; res += 0xa7;
                break;
            case 0xf5:
                res += 0xc4; res += 0xb1;
                break;
            case 0xf6:
                res += 0xc4; res += 0xb3;
                break;
            case 0xf7:
                res += 0xc5; res += 0x80;
                break;
            case 0xf8:
                res += 0xc5; res += 0x82;
                break;
            case 0xf9:
                res += 0xc3; res += 0xb8;
                break;
            case 0xfa:
                res += 0xc5; res += 0x93;
                break;
            case 0xfb:
                res += 0xc3; res += 0x9f;
                break;
            case 0xfc:
                res += 0xc3; res += 0xbe;
                break;
            case 0xfd:
                res += 0xc5; res += 0xa7;
                break;
            case 0xfe:
                res += 0xc5; res += 0x8b;
                break;
            case 0xff:
                res += 0xc2; res += 0xad;
                break;
            default:
                if(in[i] < 0x80)
                {
                    res += in[i];
                }
                else if((in[i] >= 0xa0) && (in[i] < 0xc0))
                {
                    // leading byte
                    res += 0xc0 | (in[i] & 0xc0) >> 6;
                    // trailing byte
                    res += 0x80 | (in[i] & 0x3f);
                }
                break;
        }
    }

    return res;
}

/**
 * Decode text information that is coded as described in ETSI EN 300 468 annex A
 *
 * @param str string
 * @param len length
 * @return decoded text in UTF-8
 */
string DecodeText(const unsigned char *str, size_t len)
{
    // Sanity check
    if(!str || len == 0)
    {
        OS_LOG(DVB_ERROR,    "<%s> Invalid argument passed (%p, %lu)\n", __FUNCTION__, str, len);
        return string("");
    }

    OS_LOG(DVB_TRACE1,    "<%s> called: str[0] = 0x%x, len = %lu\n", __FUNCTION__, str[0], len);

    // ETSI EN 300 468: Text information is coded using character sets and methods described in annex A.
    // A.2 Selection of character table:
    // Text fields can optionally start with non-spacing, non-displayed data which specifies the alternative character
    // table to be used for the remainder of the text item.

    string res;
    const char* type = NULL;
    bool bConvert = true;
    const char* in = (const char*)str;
    size_t inLength = len;

    // Let's check if the text is coded using the default charecter table 00 - Latin (ISO 6937 + Euro symbol)
    if(str[0] >= 0x20)
    {
        OS_LOG(DVB_TRACE1,    "<%s> Converting ISO-6937 string\n", __FUNCTION__);
        res = LatinToUtf(str, len);
        bConvert = false;
    }
    else if(str[0] >= 0x01 && str[0] <= 0x0b) // ISO 8859-5 - ISO 8859-15
    {
        switch(str[0])
        {
            case 0x01:
                type = "ISO-8859-5";
                break;
            case 0x02:
                type = "ISO-8859-6";
                break;
            case 0x03:
                type = "ISO-8859-7";
                break;
            case 0x04:
                type = "ISO-8859-8";
                break;
            case 0x05:
                type = "ISO-8859-9";
                break;
            case 0x06:
                type = "ISO-8859-10";
                break;
            case 0x09:
                type = "ISO-8859-13";
                break;
            case 0x0a:
                type = "ISO-8859-14";
                break;
            case 0x0b:
                type = "ISO-8859-15";
                break;
            default:
                OS_LOG(DVB_WARN,   
                        "<%s> character code table 0x%x is not supported, defaulting to ISO-8859-1\n",
                        __FUNCTION__, str[0]);
                type = "ISO-8859-1";
                break;
        }

        in += 1;
        inLength -= 1;
    }
    else if(str[0] == 0x10) // 3 byte code
    {
        uint16_t code = str[1] << 8 | str[2];
        switch(code)
        {
            case 0x01:
                type = "ISO-8859-1";
                break;
            case 0x02:
                type = "ISO-8859-2";
                break;
            case 0x03:
                type = "ISO-8859-3";
                break;
            case 0x04:
                type = "ISO-8859-4";
                break;
            case 0x05:
                type = "ISO-8859-5";
                break;
            case 0x06:
                type = "ISO-8859-6";
                break;
            case 0x07:
                type = "ISO-8859-7";
                break;
            case 0x08:
                type = "ISO-8859-8";
                break;
            case 0x09:
                type = "ISO-8859-9";
                break;
            case 0x0a:
                type = "ISO-8859-10";
                break;
            case 0x0d:
                type = "ISO-8859-13";
                break;
            case 0x0e:
                type = "ISO-8859-14";
                break;
            case 0x0f:
                type = "ISO-8859-15";
                break;
            default:
                OS_LOG(DVB_WARN,   
                        "<%s> character code table 0x10.0x%x is not supported, defaulting to ISO-8859-1\n",
                        __FUNCTION__, code);
                type = "ISO-8859-1";
                break;
        }

        in += 3;
        inLength -= 3;
    }
    else if(str[0] == 0x15) // UTF-8
    {
        // No conversion needed
        res = string((const char*)str + 1, len - 1);
        bConvert = false;
    }
    else
    {
        OS_LOG(DVB_WARN,    "<%s> character code table 0x%x is not supported\n", __FUNCTION__, str[0]);
        // TODO: Add ISO/IEC 10646, KSX1001-2004, GB-2312-1980, Big5 subset of ISO/IEC 10646 and encoding_type_id support
        res = string((const char*)str + 1, len - 1);
        bConvert = false;
    }

    // Let's perform the character set conversion
    if(bConvert && type)
    {
        OS_LOG(DVB_TRACE1,    "<%s> converting from UTF-8 to %s\n", __FUNCTION__, type);

        iconv_t cd = iconv_open("UTF-8", type);
        if(cd == (iconv_t)(-1))
        {
            OS_LOG(DVB_WARN,    "<%s> conversion from UTF-8 to %s is not supported by iconv\n", __FUNCTION__, type);
            res = string(in, inLength);
        }
        else
        {
            size_t outLength = (inLength*3) + 1;
            char* buf = new char[outLength]();
            char* out = buf;
            size_t ret = iconv(cd, (char**)&in, &inLength, &out, &outLength);
            if(ret == EILSEQ ||
               ret == E2BIG ||
               ret == EINVAL||
               ret == EBADF)
            {
                OS_LOG(DVB_ERROR, "<%s> iconv() failed with %s\n", __FUNCTION__, strerror(errno));
            }
            else
            {
                *out = '\0';
                res = string((const char*)buf);
            }

            delete [] buf;
            iconv_close(cd);
        }
    }

    return res;
}
