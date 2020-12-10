#! /bin/bash

set -xue

YMD="$1"
report_to_xml="$2"
ago="$3"
# ago jet: $( seq -4 1 4 )
# ago hera: $( seq 0 3 )
shift 3

mkdir reports || true

tmpreport=reports/$YMD.txt-$$.tmp
echo "Project Directory             Sub-Directory           Dir Use (TB)  Dir Quota (%)   Dir Quota (TB)      Last Checked" > "$tmpreport"
echo >> "$tmpreport"
for area in "$@" ; do
    for ago in $ago ; do
        dir=$( date +%Y%m%d -d "$ago days ago" )
        donefile="$dir/$area.done"
        txtfile="$dir/$area.txt"
        if [[ -e "$donefile" ]] ; then
            cat "$txtfile" >> "$tmpreport"
            echo >> "$tmpreport"
            break
        fi
    done
done
tmpxml=reports/$YMD.xml-$$.tmp
cat "$tmpreport" | "$report_to_xml" > "$tmpxml"

mv "$tmpreport" reports/$YMD.txt
mv "$tmpxml" reports/$YMD.xml
ln -sf reports/$YMD.txt report.txt
ln -sf reports/$YMD.xml report.xml
if [[ -d /lfs4 ]] ; then
    /bin/cp -fpL $HOME/usage-monitor/out/report.txt /lfs1/BMC/rtfim/disk-usage/jet.txt
else
    scp $HOME/usage-monitor/out/report.txt jetscp.rdhpcs.noaa.gov:/lfs1/BMC/rtfim/disk-usage/hera.txt
fi
