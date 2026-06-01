#!/usr/bin/env python3


import datetime
import os
import subprocess
import sys
import xml.dom.minidom

from make_lustre_report import handle_file

def print_header():
    print("Project Directory             Sub-Directory           Dir Use (TB)  "
          "Dir Quota (%)   Dir Quota (TB)      Last Checked                    "
          "age>90d (TB)    age>180d (TB)     Files          File Quota (%)   "
          "File Quota")
    print()
    print()

def print_subreports(listing):
    for filename in listing:
        handle_file(filename)
        print()

def __main():
    print_subreports(sys.argv[1:])
