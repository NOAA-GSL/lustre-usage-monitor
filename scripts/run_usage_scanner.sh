#!/bin/bash

set -xue

disk_usage_program="$1"
report_maker="$2"
restart_interval="$3"
dir="$4"

if [[ ! -d "${dir}" ]] ; then
    mkdir "${dir}"
fi

set +e

$disk_usage_program \
    -t "$restart_interval" -d "${dir}.done" \
    -r `echo "/${dir}" | sed s,--,/,g` "${dir}.rst.gz" "${dir}/" \
    -o "${dir}.xml"

$report_maker short "${dir}.xml" > "${dir}.txt"
$report_maker long "${dir}.xml" > "${dir}-full.txt"
