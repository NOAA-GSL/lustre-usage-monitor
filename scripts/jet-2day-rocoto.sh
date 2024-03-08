#! /bin/bash --login

source /apps/lmod/lmod/init/bash

module load rocoto

set -e

cd "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"

echo -n "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}" > topdir.ent

areas=( $( cat jet-disk-areas.lst ) )
echo " " ${areas[@]} | sed 's, /, ,g ; s,/,--,g' > dir-entity-list.ent

origin=$( date +%s -d "2020-08-01t00:00:00" )
now=$( date +%s )
delta=$(( (now-origin) % (3600*24*2) - 1 ))
deltam2=$(( delta + 3600*24*2*3 ))
deltap1=$(( delta - 3600*24*2*2 ))

ymd00m2=$( date +%Y%m%d -d "$deltam2 seconds ago" )0000
ymd00p1=$( date +%Y%m%d -d "$deltap1 seconds ago" )0000

echo "$ymd00m2 $ymd00p1 48:00:00" > cycledef.ent

which rocotorun > /dev/null
if [[ -t 1 ]] ; then
    rocotorun -w jet-disk-usage.xml -d jet-disk-usage.db --verbose 10
else
    rocotorun -w jet-disk-usage.xml -d jet-disk-usage.db
fi
