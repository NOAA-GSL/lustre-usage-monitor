#! /bin/bash

# Workaround for issue in SLURM: sometimes you will have a root-owned
# log file. These can break things if they're left where they are.

# This script assumes no whitespace in the filenames.

cd "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}"
mkdir -p junk/root
find . -uid 0 -a -not -name '.*' | grep -v junk/root | while read bad ; do
    f=$( printf "junk/root/rootfile_%s_%02x%02x%02x%02x" \
        $( date +%Y-%m-%d_%H-%M-%S ) \
        $(( RANDOM % 256 )) $(( RANDOM % 256 )) \
        $(( RANDOM % 256 )) $(( RANDOM % 256 )) )
    mv "$bad" "$f"
done
