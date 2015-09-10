APP_NAME = sqlite3pp

LIBS = -lstdc++ 

LIBNAME=$(APP_NAME)

LIB_DIR := lib
OBJ_DIR := objs_$(LIBNAME)
LIBFILE=$(LIB_DIR)/lib$(LIBNAME).so

INCLUDES = -I./ -I../ -I../boost/boost_1_52_0

COMPILE_OPTIONS = -Wall -Wextra -Wunused -fPIC -D_REENTRANT

CFLAGS += $(COMPILE_OPTIONS) $(INCLUDES)

OBJS = $(OBJ_DIR)/sqlite3.o \
	$(OBJ_DIR)/sqlite3pp.o \
	$(OBJ_DIR)/sqlite3ppext.o 

all: $(LIBFILE)

$(LIBFILE): $(LIB_DIR) $(OBJ_DIR) $(OBJS)
	$(CXX) -shared -lc -lrt -o $@ $(CFLAGS) ${OBJS}

$(OBJ_DIR)/%.o : %.c
	$(CC) -c -o $@ $< $(CFLAGS)

$(OBJ_DIR)/%.o : %.cpp
	$(CC) -c -o $@ $< $(CFLAGS)

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

clean:
	rm -rf $(LIBFILE) $(LIB_DIR) $(OBJ_DIR)

