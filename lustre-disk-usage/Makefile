# Jet/Hera:
LOAD_THE_JET_MODULE=module load gnu/9.2.0
LOAD_THE_HERA_MODULE=module load gnu/9.2.0
LOAD_NO_MODULE=:

LOAD_THE_MODULE=$(LOAD_NO_MODULE)

ifneq ($(wildcard /scratch1/NCEPDEV/stmp?),)
  override LOAD_THE_MODULE=$(LOAD_THE_HERA_MODULE)
else
  ifneq ($(wildcard /lfs4/BMC/wrfru?),)
    override LOAD_THE_MODULE=$(LOAD_THE_JET_MODULE)
  endif
endif

lustre-disk-usage.exe: lustre-disk-usage.cpp
	$(LOAD_THE_MODULE) ; \
	g++ --version ; \
	g++ -std=gnu++11 -Wall -W -fpermissive -O3 -g lustre-disk-usage.cpp -o lustre-disk-usage.exe -lz -llustreapi
