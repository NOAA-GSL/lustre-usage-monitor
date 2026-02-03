#! /bin/bash

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

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
