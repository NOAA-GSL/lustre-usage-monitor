#! /bin/bash

if [[ -d /lfs4 ]] ; then
    /bin/cp -fpL $HOME/usage-monitor/out/report.txt /lfs1/BMC/rtfim/disk-usage/jet.txt
else
    scp $HOME/usage-monitor/out/report.txt jetscp.rdhpcs.noaa.gov:/lfs1/BMC/rtfim/disk-usage/hera.txt
fi
