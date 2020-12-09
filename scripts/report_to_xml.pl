#! /usr/bin/env perl

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
