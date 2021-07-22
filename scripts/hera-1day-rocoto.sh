#! /bin/bash --login

source /apps/lmod/lmod/init/bash

module use /scratch1/BMC/wrfruc/Samuel.Trahan/soft/modulefiles/
module load rocoto/squeue-last-job-workaround

set -e

mkdir -p "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"
cd "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"
mkdir -p out out/reports

echo -n "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}" > topdir.ent

areas=( $( cat hera-disk-areas.lst ) )
echo " " ${areas[@]} | sed 's, /, ,g ; s,/,--,g' > dir-entity-list.ent

bigareas=( $( cat hera-big-disk-areas.lst ) )
echo " " ${bigareas[@]} | sed 's, /, ,g ; s,/,--,g' > big-dir-entity-list.ent

echo $( date +%Y%m%d0000 -d "7 days ago" ) $( date +%Y%m%d0000 -d "7 days" ) 24:00:00 > cycledef.ent

which rocotorun > /dev/null
if [[ -t 1 ]] ; then
    rocotorun -w hera-disk-usage.xml -d hera-disk-usage.db --verbose 10
else
    rocotorun -w hera-disk-usage.xml -d hera-disk-usage.db
fi
