#! /bin/bash --login

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

source /apps/lmod/lmod/init/bash

#module use /scratch1/BMC/wrfruc/Samuel.Trahan/soft/modulefiles/
module load rocoto

set -e

mkdir -p "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"
cd "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"
mkdir -p out out/reports

echo -n "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}" > topdir.ent

areas=( $( cat hera-disk-areas.lst ) )
echo " " ${areas[@]} | sed 's, /, ,g ; s,/,--,g' > dir-entity-list.ent

bigareas=( $( cat hera-big-disk-areas.lst ) )
echo " " ${bigareas[@]} | sed 's, /, ,g ; s,/,--,g' > big-dir-entity-list.ent

echo " " ${areas[@]} ${bigareas[@]} | sed 's, /, ,g ; s,/,--,g' > all-dir-entity-list.ent

echo $( date +%Y%m%d0000 -d "7 days ago" ) $( date +%Y%m%d0000 -d "7 days" ) 24:00:00 > cycledef.ent

which rocotorun > /dev/null
if [[ -t 1 ]] ; then
    rocotorun -w hera-disk-usage.xml -d hera-disk-usage.db --verbose 10
else
    rocotorun -w hera-disk-usage.xml -d hera-disk-usage.db
fi
