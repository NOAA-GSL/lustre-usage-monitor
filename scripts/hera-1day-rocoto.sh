#! /bin/bash --login

source /apps/lmod/lmod/init/bash

module use /scratch1/BMC/wrfruc/Samuel.Trahan/soft/modulefiles/
module load rocoto/squeue-last-job-workaround

set -e

cd "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"

echo "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}" > topdir.ent

areas=( $( cat hera-disk-areas.lst ) )
echo " " ${areas[@]} | sed 's, /, ,g ; s,/,--,g' > dir-entity-list.ent

echo $( date +%Y%m%d0000 -d "7 days ago" ) $( date +%Y%m%d0000 -d "7 days" ) 24:00:00 > cycledef.ent

which rocotorun > /dev/null
if [[ -t 1 ]] ; then
    rocotorun -w hera-disk-usage.xml -d hera-disk-usage.db --verbose 10
else
    rocotorun -w hera-disk-usage.xml -d hera-disk-usage.db
fi
