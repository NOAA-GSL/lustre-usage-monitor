#! /usr/bin/env perl

# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>. 

use strict;
use warnings;

sub quote {
    my $what=shift();
    $what=~s/[&]/\&amp;/g;
    $what=~s/[<]/\&lt;/g;
    return $what;
}

print('<?xml version="1.0" encoding="UTF-8"?>'."\n");
print('');
print "<report>\n";

my $in_tag;

while(<>) {
    if(m:^\s*$: && $in_tag) {
        print "  </area>\n";
        undef $in_tag;
    } elsif(m,^(/.*?)\s+([0-9]+[.][0-9]+)\s+([0-9]+[.][0-9]+)%\s+([0-9]+[.][0-9]+)\s+([A-Z][a-z][a-z] \d\d [A-Z][a-z][a-z] \d\d\d\d \d\d:\d\d:\d\d [A-Z]+),) {
        my ($used,$percent,$quota,$time)=($2,$3,$4,$5);
        my $path=quote($1);
        print "  <area>\n";
        print "    <path>$path</path>\n";
        print "    <used>$used</used>\n";
        print "    <percent>$percent</percent>\n";
        print "    <quota>$quota</quota>\n";
        print "    <time>$time</time>\n";
        $in_tag=1;
    } elsif(m,^([^/].*?)\s+([0-9]+[.][0-9]+)\s+([0-9]+[.][0-9]+)%\s+([A-Z][a-z][a-z] \d\d [A-Z][a-z][a-z] \d\d\d\d \d\d:\d\d:\d\d [A-Z]+),) {
        my ($used,$percent,$time)=($2,$3,$4);
        my $path=quote($1);
        if($path eq "--unseen-by-du--")
        {
            print "    <unseen>\n";
        } else {
            print "    <subdir>\n";
            print "      <path>$path</path>\n";
        }
        print "      <used>$used</used>\n";
        print "      <percent>$percent</percent>\n";
        print "      <time>$time</time>\n";
        if($path eq "--unseen-by-du--")
        {
            print "    </unseen>\n";
        } else {
            print "    </subdir>\n";
        }
    }
}
print("  </area>\n") if($in_tag);
print("</report>\n");
