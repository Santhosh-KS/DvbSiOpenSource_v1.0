//
// DVB_SI for Reference Design Kit (RDK)
//
// Copyright (C) 2015  Dmitry Barablin & Stan Partridge (stan.partridge@arris.com)
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

#ifndef _OSWRAP_H
#define _OSWRAP_H 

// 1) Define a macro to Get Environmental variables
//  char* getenv(const char* name);
#define OS_GETENV  getenv

#include <cstdio>
// 2) Define a macro to log messages

#define DVB_INFO   1
#define DVB_DEBUG  2
#define DVB_ERROR  3
#define DVB_WARN   4
#define DVB_TRACE  5
#define DVB_TRACE1 6
#define DVB_TRACE2 7
#define DVB_TRACE3 8

#define OS_LOG(level,format,...) \
{ \
    fprintf(stderr, "%d:%s:",level,"INBSI"); \
    fprintf(stderr, format,__VA_ARGS__); \
}  

#endif // _OSWRAP_H

