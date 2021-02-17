# Jet/Hera:
THE_JET_MODULE=gnu/9.2.0
THE_HERA_MODULE=gnu/9.2.0
THE_ORION_MODULE=gcc/10.2.0

THE_MODULE=$(THE_ORION_MODULE)

ifneq ($(wildcard /scratch1/NCEPDEV/stmp?),)
  override THE_MODULE=$(THE_HERA_MODULE)
else
  ifneq ($(wildcard /lfs4/BMC/wrfru?),)
    override THE_MODULE=$(THE_JET_MODULE)
  else
    ifneq ($(wildcard /work/noa?),)
      override THE_MODULE=$(THE_ORION_MODULE)
    else
      $(error Cannot detect machine. Is it Jet? Is it Hera? Is it Orion? I do not know! What module should I use? Ag. I give up. You figure it out!)
    endif
  endif
endif

lustre-disk-usage.exe: lustre-disk-usage.cpp
	module load $(THE_MODULE) ; \
	g++ --version ; \
	g++ -std=gnu++11 -Wall -W -fpermissive -O3 -g lustre-disk-usage.cpp -o lustre-disk-usage.exe -lz -llustreapi
