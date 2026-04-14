# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

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

disk-usage.exe: disk-usage.cpp Makefile
	$(LOAD_THE_MODULE) ; \
	g++ --version ; \
	g++ -std=gnu++11 -Wall -W -fpermissive -O3 -g disk-usage.cpp -o disk-usage.exe -lz
