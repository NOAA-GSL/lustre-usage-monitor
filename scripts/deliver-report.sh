#! /bin/bash

if [[ -d /lfs4 ]] ; then
    /bin/cp -fpL $HOME/usage-monitor/out/report.txt /lfs1/BMC/rtfim/disk-usage/jet.txt
else
    : # fixme: Hera
fi
