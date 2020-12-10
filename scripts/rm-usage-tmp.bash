#! /bin/bash

find "${USAGE_MONITOR:-$HOME/lustre-usage-monitor}" -name '*-*.tmp' -a -mmin +30 -print0 | xargs -0 rm -f
