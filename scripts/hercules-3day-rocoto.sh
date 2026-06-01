#! /bin/bash --login

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

# source /apps/lmod/lmod/init/bash

module load contrib ruby rocoto

set -e 

cd "$USAGE_MONITOR"

echo -n "$USAGE_MONITOR" > topdir.ent

areas=( $( cat hercules-disk-areas.lst ) )
echo " " ${areas[@]} | sed 's, /, ,g ; s,/,--,g' > dir-entity-list.ent

origin=$( date +%s -d "2026-05-26t00:00:00" )
now=$( date +%s )
delta=$(( (now-origin) % (3600*24*3) - 1 ))
deltam2=$(( delta + 3600*24*3*3 ))
deltap1=$(( delta - 3600*24*3*2 ))

ymd00m2=$( date +%Y%m%d -d "$deltam2 seconds ago" )0000
ymd00p1=$( date +%Y%m%d -d "$deltap1 seconds ago" )0000

echo "$ymd00m2 $ymd00p1 72:00:00" > cycledef.ent

which rocotorun > /dev/null
rocotorun -w hercules-disk-usage.xml -d hercules-disk-usage.db "$@"
