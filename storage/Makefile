APP_NAME = sistorage

LIBS = -lstdc++ 

LIBNAME=$(APP_NAME)

LIB_DIR := lib
SRC_DIR := src
OBJ_DIR := objs_$(LIBNAME)
LIBFILE=$(LIB_DIR)/lib$(LIBNAME).so

INCLUDES = -I./include -I../ -I../sectionparser/include -I../common/include -I../boost -I../sqlite3pp -I./interfaces -I../jansson/src

COMPILE_OPTIONS = -Wall -Wextra -Wunused -fPIC -D_REENTRANT -std=c++0x

CFLAGS += $(COMPILE_OPTIONS) $(INCLUDES)

OBJS = $(OBJ_DIR)/TDvbDb.o \
	$(OBJ_DIR)/TDvbSiStorage.o

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

