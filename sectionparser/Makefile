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

