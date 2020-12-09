lustre-disk-usage.exe: lustre-disk-usage.cpp
	if [[ -d /lfs4 ]] ; then \
	  module load gnu/9.2.0 ; \
	else true ; fi ; \
	g++ --version ; \
	g++ -std=gnu++11 -Wall -W -fpermissive -O3 -g lustre-disk-usage.cpp -o lustre-disk-usage.exe -lz -llustreapi
