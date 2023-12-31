
When you clone the repository, here are the files you'll need to edit or run:

These files control the disk areas to scan:
    hera-disk-areas.lst <-- these disk areas use shared nodes
    hera-big-disk-areas.lst <-- these disk areas use exclusive jobs due to memory usage
    jet-disk-areas.lst <-- shared nodes

There are no disk areas on jet that need an exclusive job.

You will need to change the project used for job submission:
    jet-disk-usage.xml <-- change ACCOUNT
    hera-disk-usage.xml <-- change ACCOUNT

Also, you should turn off the delivery of reports to github by commenting out this line in scripts/final_report.sh:

    github_deliver "$system" # exits script

The scripts to run the workflow are:
    scripts/hera-1day-rocoto.sh
    scripts/jet-2day-rocoto.sh

To compile:

export USAGE_MONITOR=/path/to/directory/where/you/cloned/lustre-usage-monitor
cd $USAGE_MONITOR/lustre-disk-usage
make

To run it, compile first, and then:

export USAGE_MONITOR=/path/to/directory/where/you/cloned/lustre-usage-monitor
cd $USAGE_MONITOR
HERA: ./scripts/hera-1day-rocoto.sh
JET: ./scripts/jet-2day-rocoto.sh

I have workarounds for some issues by running scripts in my crontab.

scripts/rm-usage-tmp.bash <--- deletes *-*.tmp files
scripts/move-root-files.sh <--- moves root-owned files to another directory, to avoid breaking the workflow
