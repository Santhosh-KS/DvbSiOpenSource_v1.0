## Copyright (c) 2015 RDK Management, LLC. All rights reserved.

SUBDIRS = 
LOCAL_DEFINES =  -DRMF_OSAL_LITTLE_ENDIAN

AM_CXXFLAGS = -std=c++0x -pthread $(GST_CFLAGS) $(LOCAL_DEFINES) \
              -I${top_srcdir}/osal/include \
              -I./common/include \
              -I./include \
              -I./interfaces

lib_LTLIBRARIES = librmfsectionparseropen.la 
librmfsectionparseropen_la_SOURCES = ./src/TSectionParser.cpp ./src/TSectionList.cpp ./src/TMpegDescriptor.cpp ./src/DvbUtils.cpp ./src/TSiSection.cpp
#librmfsectionparseropen_la_LDFLAGS = -lrdkloggers 
