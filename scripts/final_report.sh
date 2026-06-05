#! /bin/bash

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

set -x

YMD="$1"
report_to_xml="$2"
agos="$3"
make_lustre_report="$4"
deliver_to_github="$5"
shift 5

echo_heading() {
    local mode=$1
    if [[ $mode == long ]] ; then
        echo "Project Directory             Sub-Directory           Dir Use (TB)  Dir Quota (%)   Dir Quota (TB)      Last Checked                    age>90d (TB)    age>180d (TB)     Files          File Quota (%)   File Quota"
    else
        echo "Project Directory             Sub-Directory           Dir Use (TB)  Dir Quota (%)   Dir Quota (TB)      Last Checked"
    fi
    echo
    echo
}

combine_xml_reports() {
    local mode="$1"
    shift 1

    local tmpdone=reports/$YMD-$mode.txt-$$.tmp
    local tmppart=reports/$YMD-$mode-part.txt-$$.tmp
    local area
    local target
    local tmpxml

    if [[ $mode == long ]] ; then
        target=reports/$YMD-full.txt
    else
        target=reports/$YMD.txt
    fi

    posix_ymd="${YMD:0:4}-${YMD:4:2}-${YMD:6:2}t12:00:00 UTC+0"

    echo_heading "$mode" > "$tmpdone"
    for area in "$@" ; do
        local most_recent=NONE
        local found_one=NO
        for ago in $agos ; do
            local dir=$( date +%Y%m%d -d "$posix_ymd -$ago days" )
            local donefile="$dir/$area.done"
            local fullfile="$dir/$area.xml"
            if [[ -e "$donefile" ]] && ( "$make_lustre_report" "$mode" "$fullfile" > "$tmppart" ) ; then
                cat "$tmppart" >> "$tmpdone"
                echo >> "$tmpdone"
                found_one=YES
                break
            elif [[ "$most_recent" == NONE && -s "$fullfile" ]] ; then
                most_recent="$fullfile"
            fi
        done
        if [[ "$found_one" == NO ]] ; then
            if [[ "$most_recent" != NONE ]] && ( "$make_lustre_report" "$mode" "$most_recent" > "$tmppart" ) ; then
                cat "$tmppart" >> "$tmpdone"
                echo >> "$tmpdone"
            else
                echo "WARNING: Cannot find xml report for \"$area\"" 1>&2
            fi
        fi
    done
    if [[ "$mode" == long ]] ; then
        tmpxml=reports/$YMD.xml-$$.tmp
        cat "$tmpdone" | "$report_to_xml" > "$tmpxml"
    fi

    rm -f "$tmppart"
    mv "$tmpdone" "$target"

    if [[ $mode == long ]] ; then
        ln -sf "$target" report-full.txt
    else
        ln -sf "$target" report.txt
    fi
}

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
    set -e
    git clone --branch master ssh://git@github.com/NOAA-GSL/usage-reports "$dirname"
    set +e
    ( update_github_txt "$dirname" "$system" )
    success=$?
    rm -rf "$dirname"
    exit $success
}

find_system() {
    if [[ -d /scratch1/NCEPDEV ]] ; then
        system=hera
    elif ( hostname | grep -i herc > /dev/null ) then
        system=hercules
    else
        system=unknown
    fi
}

mkdir reports || true
combine_xml_reports long "$@"
combine_xml_reports short "$@"

if [[ "$deliver_to_github" == YES ]] ; then
    find_system
    github_deliver "$system" # exits script
fi
