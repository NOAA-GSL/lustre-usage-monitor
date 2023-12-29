#! /bin/bash

set -xue

YMD="$1"
report_to_xml="$2"
agos="$3"
# ago jet: $( seq -4 1 4 )
# ago hera: $( seq 0 3 )
shift 3

mkdir reports || true

tmpshort=reports/$YMD-short.txt-$$.tmp
tmpfull=reports/$YMD-full.txt-$$.tmp
echo "Project Directory             Sub-Directory           Dir Use (TB)  Dir Quota (%)   Dir Quota (TB)      Last Checked                    age>90d (TB)    age>180d (TB)     Files          File Quota (%)   File Quota" > "$tmpfull"
echo >> "$tmpfull"
echo "Project Directory             Sub-Directory           Dir Use (TB)  Dir Quota (%)   Dir Quota (TB)      Last Checked" > "$tmpshort"
echo >> "$tmpfull"
echo >> "$tmpshort"
for area in "$@" ; do
    for ago in $agos ; do
        dir=$( date +%Y%m%d -d "$ago days ago" )
        donefile="$dir/$area.done"
        shortfile="$dir/$area-short.txt"
        fullfile="$dir/$area-full.txt"
        if [[ -e "$donefile" ]] ; then
            cat "$fullfile" >> "$tmpfull"
            echo >> "$tmpfull"
            cat "$shortfile" >> "$tmpshort"
            echo >> "$tmpshort"
            break
        fi
    done
done
tmpxml=reports/$YMD.xml-$$.tmp
cat "$tmpfull" | "$report_to_xml" > "$tmpxml"

mv "$tmpshort" reports/$YMD-short.txt
mv "$tmpfull" reports/$YMD-full.txt
mv "$tmpxml" reports/$YMD.xml
ln -sf reports/$YMD-short.txt report-short.txt
ln -sf reports/$YMD-short.txt report.txt
ln -sf reports/$YMD-full.txt report-full.txt
ln -sf reports/$YMD.xml report.xml

update_github_txt() {
    set -uxe
    local dirname="$1"
    local system="$2"
    cd "$dirname"
    local add_commit=1
    if ( ! cmp ../report.txt $system.txt ) ; then
        cat ../report.txt > $system.txt
        add_commit=0
    fi
    if ( ! cmp ../report-full.txt $system-full.txt ) ; then
        cat ../report-full.txt > $system-full.txt
        add_commit=0
    fi
    if (( add_commit == 0 )) ; then
        git add $system.txt $system-full.txt
        git commit -m "$system disk usage report $YMD completed at $( date )"
        git push origin master
    fi
}

github_deliver() {
    local dirname=report.$$.$RANDOM.$RANDOM
    local system="$1"
    git clone --branch master ssh://git@github.com/NOAA-GSL/usage-reports "$dirname"
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
