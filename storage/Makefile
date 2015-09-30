APP_NAME = sistorage

LIBS = -lstdc++ 

LOCAL_DEFINES =   -DMERGE_INTEL \
                  -DENABLE_NEW_LOGSVR \
                  -DCMCST_EXT_X1 \
                  -DUSE_POD \
                  -DUSE_DSG \
                  -DUSE_CDL \
                  -DMPOD_SUPPORT \
                  -DUSE_MFR \
                  -DUSE_TUNER \
                  -DNV_RAM_READ \
                  -DINTEL_PR13 \
                  -DINTEL_PR18 \
                  -DUSE_VIDSINK_API \
                  -g -O2 -Wall \
                  -fno-short-enums \
                  -ffloat-store \
                  -fno-strict-aliasing \
                  -DHAVE_STDINT_H \
                  -D_GNU_SOURCE \
                  -DMPE_FEATURE_DEBUG \
                  -DMPE_MEM_HANDLES \
                  -DRMF_OSAL_LITTLE_ENDIAN \
                  -DJGFX_DEV \
                  -D$(RMF_BUILD_OS) \
                  -D$(POD_PLATFORM) \
                  -DLINUX \
                  -DUSE_CC_GETTIMEOFDAY \
                  -DGCC4_XXX \
                  -DNUM_SURF=1 \
                  -DDEPRECATED_INTERFACE_SUPPORT \
                  -DUSE_NEW_MSPOD_DRIVER \
                  -DUSE_CDL_SERVICE \
                  -DTEST_WITHOUT_PODMGR \
                  -DCOMMON_DOWNLOAD \
                  -DSNMP_IPC_CLIENT  \
                  -D$(MFR_NAME) \
                  -DIPPV_CLIENT_ENABLED  \
                  -DUSE_POD_IPC  \
                  -DUSE_PXYSRVC  \
                  -DQAMSRC_PMTBUFFER_PROPERTY

AM_CXXFLAGS = -std=c++0x $(LOCAL_DEFINES) 

LIBNAME=$(APP_NAME)

LIB_DIR := lib
SRC_DIR := src
OBJ_DIR := objs_$(LIBNAME)
LIBFILE=$(LIB_DIR)/lib$(LIBNAME).so

INCLUDES = -I./include -I../ -I../sectionparser/include -I../common/include -I../boost -I../sqlite3pp -I./interfaces -I../jansson/src

COMPILE_OPTIONS = -Wall -Wextra -Wunused -fPIC -D_REENTRANT -std=c++0x

CFLAGS += $(COMPILE_OPTIONS) $(INCLUDES) $(AM_CXXFLAGS)

OBJS = $(OBJ_DIR)/TDvbDb.o \
	$(OBJ_DIR)/TDvbSiStorage.o \
	$(OBJ_DIR)/TDvbJanssonParser.o

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

