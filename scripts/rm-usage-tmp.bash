#! /bin/bash

find $HOME/usage-monitor/ -name '*-*.tmp' -a -mmin +30 -print0 | xargs -0 rm -f
