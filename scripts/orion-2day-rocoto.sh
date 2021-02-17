#! /bin/bash --login

source /apps/lmod/lmod/init/bash

module load contrib/0.1
module load rocoto/1.3.3

set -e

cd "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"

echo -n "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}" > topdir.ent

areas=( $( cat orion-disk-areas.lst ) )
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
    rocotorun -w orion-disk-usage.xml -d orion-disk-usage.db --verbose 10
else
    rocotorun -w orion-disk-usage.xml -d orion-disk-usage.db
fi
