#! /bin/bash

set -xue

YMD="$1"
report_to_xml="$2"
agos="$3"
# ago jet: $( seq -4 1 4 )
# ago hera: $( seq 0 3 )
shift 3

mkdir reports || true

tmpreport=reports/$YMD.txt-$$.tmp
echo "Project Directory             Sub-Directory           Dir Use (TB)  Dir Quota (%)   Dir Quota (TB)      Last Checked" > "$tmpreport"
echo >> "$tmpreport"
for area in "$@" ; do
    for ago in $agos ; do
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

update_github_txt() {
    set -uxe
    local dirname="$1"
    local system="$2"
    cd "$dirname"
    if ( ! cmp ../report.txt $system.txt ) ; then
        cat ../report.txt > $system.txt
        git add $system.txt
        git commit -m "$system disk usage report $YMD completed at $( date )"
        git push origin master
    fi
}

github_deliver() {
    local dirname=report.$$.$RANDOM.$RANDOM
    local system="$1"
    git clone --branch master ssh://git@github.com/SamuelTrahanNOAA/usage-reports "$dirname"
    set +e
    ( update_github_txt "$dirname" "$system" )
    success=$?
    rm -rf "$dirname"
    exit $success
}

if [[ -d /lfs4 ]] ; then
    #/bin/cp -fpL "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"/out/report.txt /lfs1/BMC/rtfim/disk-usage/jet.txt
    system=jet
elif [[ -d /scratch1/NCEPDEV ]] ; then
    #scp "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"/out/report.txt jetscp.rdhpcs.noaa.gov:/lfs1/BMC/rtfim/disk-usage/hera.txt
    system=hera
else
    system=orion
fi

github_deliver "$system" # exits script
