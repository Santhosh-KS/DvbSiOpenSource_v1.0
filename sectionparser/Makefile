# DVB_SI for Reference Design Kit (RDK)
#
# Copyright (C) 2015 Santhosh KS(Santhosh.KS@arris.com) , Manu Shrot (Manu.Shrot@arris.com),
# Stan Partridge (Stan.Partridge@arris.com) & Dmitry Barablin
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

APP_NAME = sectionparser

LIBS = -lstdc++ 
LIBNAME=$(APP_NAME)

LIB_DIR := lib
SRC_DIR := src
OBJ_DIR := objs_$(LIBNAME)
LIBFILE=$(LIB_DIR)/lib$(LIBNAME).so

INCLUDES = -I./include -I../common/include -I./interfaces

COMPILE_OPTIONS = -Wall -Wextra -Wunused -fPIC -D_REENTRANT -std=c++0x -fno-short-enums -g -O2 -Wall -fno-strict-aliasing

CFLAGS += $(COMPILE_OPTIONS) $(INCLUDES)

OBJS = $(OBJ_DIR)/DvbUtils.o \
	$(OBJ_DIR)/TMpegDescriptor.o \
	$(OBJ_DIR)/TSectionList.o  \
	$(OBJ_DIR)/TSectionParser.o \
	$(OBJ_DIR)/TSiSection.o 

all: $(LIBFILE)

$(LIBFILE): $(LIB_DIR) $(OBJ_DIR) $(OBJS)
	$(CXX) -shared -lc -lrt -o $@ $(CFLAGS) ${OBJS}

$(OBJ_DIR)/%.o : $(SRC_DIR)/%.cpp
	$(CC) -c -o $@ $< $(CFLAGS)

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

clean:
	rm -rf $(LIBFILE) $(LIB_DIR) $(OBJ_DIR)

