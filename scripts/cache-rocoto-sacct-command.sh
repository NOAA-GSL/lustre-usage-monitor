#! /bin/bash --login

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

if [[ -t 1 ]] ; then
    set -x
fi

set -ue

six_days_ago=$( date +%m%d%y -d "6 days ago" )
tgtfile="$HOME/sacct-cache/sacct.txt"
workdir=$( dirname "$tgtfile" )
[[ -d "$workdir" ]] || mkdir "$workdir" || sleep 3
temp=$( mktemp --tmpdir="$workdir" )

set +ue

(
    set -ue
    sacct -S "$six_days_ago" -L -o "jobid,user%30,jobname%30,partition%20,priority,submit,start,end,ncpus,exitcode,state%12" -P > "$temp" ;
    (( $( wc -l < "$temp" ) > 1 )) && /bin/mv -f "$temp" "$tgtfile"
)

if [[ -e "$temp" ]] ; then
    rm -f "$temp"
fi
