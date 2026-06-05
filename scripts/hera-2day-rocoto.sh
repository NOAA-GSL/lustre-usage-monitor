#! /bin/bash --login

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

# source /apps/lmod/lmod/init/bash

module load rocoto

set -e 

cd "$USAGE_MONITOR"

echo -n "$USAGE_MONITOR" > topdir.ent

areas=( $( cat hera-disk-areas.lst ) )
echo " " ${areas[@]} | sed 's, /, ,g ; s,/,--,g' > dir-entity-list.ent

bigareas=( $( cat hera-big-disk-areas.lst ) )
echo " " ${bigareas[@]} | sed 's, /, ,g ; s,/,--,g' > big-dir-entity-list.ent

echo " " ${areas[@]} ${bigareas[@]} | sed 's, /, ,g ; s,/,--,g' > all-dir-entity-list.ent

origin=$( date +%s -d "2026-06-05t00:00:00" )
now=$( date +%s )
delta=$(( (now-origin) % (3600*24*2) - 1 ))

earlier_seconds=$(( delta + 3600*24*2*3 ))
later_seconds=$(( delta - 3600*24*2*3 ))

earlier_minute=$( date +%Y%m%d -d "$earlier_seconds seconds ago" )0000
later_minute=$( date +%Y%m%d -d "$later_seconds seconds ago" )0000

echo "$earlier_minute $later_minute 48:00:00" > cycledef.ent

which rocotorun > /dev/null
rocotorun -w hera-disk-usage.xml -d hera-disk-usage.db "$@"
