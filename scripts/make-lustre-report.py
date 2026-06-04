#!/usr/bin/env python3


import datetime
import os
import subprocess
import sys
import xml.dom.minidom

def get_element_text(parent, name):
    elements = parent.getElementsByTagName(name)
    for element in elements:
        return element.firstChild.nodeValue

def get_lustre_usage(path, indent):
    stat = os.stat(path, follow_symlinks=True)
    gid = stat.st_gid
    output = subprocess.check_output(f"lfs quota -p '{gid}' '{path}'", shell=True)

    for line in output.splitlines():
        if line:
            lastline = line

    split = lastline.replace(b'*',b'').strip().split()

    # lastline looks like one of these:
    # 16703521588 49807360000 52428800000 - 2494654 0 0 -
    # /work/noaa/zrtrr 16703521588 49807360000 52428800000 - 2494654 0 0 -

    return {
        'bytes_used': int(split[-8], 10),
        'bytes_soft_quota': int(split[-7], 10),
        'bytes_hard_quota': int(split[-6], 10),

        'files_used': int(split[-4], 10),
        'files_soft_quota': int(split[-3], 10),
        'files_hard_quota': int(split[-2], 10),
    }
        

def handle_usage(long_format, usage):
    for dir in usage.getElementsByTagName('dir'):
        handle_dir(long_format, dir)

def handle_dir(long_format, dir):
    path, lustre_usage, total_bytes, total_blocks, total_files, valid_time = \
        handle_subdir(long_format, dir, '  ', None, True)
    remaining_bytes = total_bytes
    remaining_blocks = total_blocks
    remaining_files = total_files
    for subdir in dir.getElementsByTagName('subdir'):
        _, _, bytes, blocks, files, _ = \
            handle_subdir(long_format, subdir, '    ', lustre_usage, False)
        remaining_bytes -= bytes
        remaining_blocks -= blocks
        remaining_files -= files

    remaining_bytes = max(0, remaining_bytes)
    remaining_blocks = max(0, remaining_blocks)
    remaining_files = max(0, remaining_files)

    remaining = { 'bytes': remaining_bytes, 'blocks': remaining_blocks, 'files': remaining_files }
    zero = { 'bytes': 0, 'blocks': 0, 'files': 0 }

    if long_format:
        print_long_usage(valid_time, '--unseen-by-scan--', remaining, zero, zero, lustre_usage, False)
    else:
        print_short_usage(valid_time, '--unseen-by-scan--', remaining, zero, zero, lustre_usage, False)

def print_long_usage(timestamp, path, total, old1, old2, lustre_usage, show_quota):
    # result=fprintf(tgt,"%-44s\t\t%5.1f\t\t%5.1f%%\t\t%5.1f\t\t%s\t%8.1f\t%8.1f\t%10lld\t%8.1f%%\t%lld\n",
    #                strrealpath(du.top_dir()).c_str(),
    #                double(curspace)/1048576.0/1048576,max(0.0,double(curspace)/softlimit*100),
    #                double(softlimit)/1048576.0/1048576,strstrftime("%a %d %b %Y %T %Z").c_str(),
    #                dot->at_age(1).bytes/1048576.0/1048576,
    #                dot->at_age(2).bytes/1048576.0/1048576,
    #                static_cast<long long int>(curfiles),
    #                fsoftlimit ? max(0.0,double(curfiles)/max(1.0,double(fsoftlimit))*100) : 0.0,
    #                static_cast<long long int>(fsoftlimit));

    softlimit = lustre_usage["bytes_soft_quota"] / 1048576.0 / 1024.0
    curspace = total['bytes'] / 1048576.0 / 1048576.0
    curspace_pct = max(0.0, curspace / softlimit) * 100
    time_string = timestamp.strftime("%a %d %b %Y %T GMT")
    old1space = old1['bytes'] / 1048576.0 / 1048576.0
    old2space = old2['bytes'] / 1048576.0 / 1048576.0

    curfiles = int(total['files'])
    filelimit = lustre_usage["files_soft_quota"]
    file_pct = 0.0
    if filelimit > 0:
        file_pct = max(0.0, float(curfiles) / max(1.0, float(filelimit)) * 100)

    printme = f'{path:44s}\t\t'
    printme += f'{curspace:5.1f}\t\t'
    printme += f'{curspace_pct:5.1f}%\t\t'
    if show_quota:
        printme += f'{softlimit:5.1f}'
    printme += f'\t\t{time_string}\t'
    printme += f'{old1space:8.1f}\t'
    printme += f'{old2space:8.1f}\t'
    printme += f'{curfiles:10d}\t'
    printme += f'{file_pct:8.1f}%\t'
    if show_quota:
        printme += f'{filelimit:d}'

    print(printme)

def print_short_usage(timestamp, path, total, old1, old2, lustre_usage, show_quota):
    # result=fprintf(tgt,"%-44s\t\t%5.1f\t\t%5.1f%%\t\t%5.1f\t\t%s\n",
    #                strrealpath(du.top_dir()).c_str(),
    #                double(curspace)/1048576.0/1048576,max(0.0,double(curspace)/softlimit*100),
    #                double(softlimit)/1048576.0/1048576,strstrftime("%a %d %b %Y %T %Z").c_str());

    softlimit = lustre_usage["bytes_soft_quota"] / 1048576.0 / 1024.0
    curspace = total['bytes'] / 1048576.0 / 1048576.0
    curspace_pct = max(0.0, curspace / softlimit) * 100
    time_string = timestamp.strftime("%a %d %b %Y %T GMT")

    curfiles = int(total['files'])
    filelimit = lustre_usage["files_soft_quota"]
    file_pct = 0.0
    if filelimit > 0:
        file_pct = max(0.0, float(curfiles) / max(1.0, float(filelimit)) * 100)

    printme = f'{path:44s}\t\t'
    printme += f'{curspace:5.1f}\t\t'
    printme += f'{curspace_pct:5.1f}%\t\t'
    if show_quota:
        printme += f'{softlimit:5.1f}'
    printme += f'\t\t{time_string}\t'
    printme += f'{curfiles:10d}\t'
    printme += f'{file_pct:8.1f}%\t'
    if show_quota:
        printme += f'{filelimit:d}'

    print(printme)

def handle_subdir(long_format, dir, indent, lustre_usage, show_quota):
    path = get_element_text(dir, 'path')
    #print(f'{indent}{path}')

    if not lustre_usage:
        lustre_usage = get_lustre_usage(path, indent+'  ')
        lu = lustre_usage
        #print(f'{indent}  bytes used={lu["bytes_used"]} soft={lu["bytes_soft_quota"]} hard={lu["bytes_hard_quota"]}')
        #print(f'{indent}  files used={lu["files_used"]} soft={lu["files_soft_quota"]} hard={lu["files_hard_quota"]}')

    valid_string = get_element_text(dir, 'valid')
    valid_time = datetime.datetime.strptime(valid_string, "%a %d %b %Y %H:%M:%S %Z")
    #print(f'{indent}  timestamp {valid_time:%a %d %b %Y %T %Z} (from "{valid_string}")')

    for total in dir.getElementsByTagName('total'):
        total_bytes, total_blocks, total_files, _ = handle_total(total, indent+'  ')
        break

    old=[]
    for tag in dir.getElementsByTagName('files_older_than'):
        old.append(handle_files_older_than(tag, indent+'  '))

    total_hash = { 'bytes':total_bytes, 'blocks':total_blocks, 'files':total_files }
    old1_hash = { 'bytes':old[0][0], 'blocks':old[0][1], 'files':old[0][2] }
    old2_hash = { 'bytes':old[1][0], 'blocks':old[1][1], 'files':old[1][2] }

    if long_format:
        print_long_usage(valid_time, path, total_hash, old1_hash, old2_hash, lustre_usage, show_quota)
    else:
        print_short_usage(valid_time, path, total_hash, old1_hash, old2_hash, lustre_usage, show_quota)

    return path, lustre_usage, total_bytes, total_blocks, total_files, valid_time

def find_bytes_blocks_files(node):
    bytes = float(get_element_text(node, 'bytes'))
    blocks = float(get_element_text(node, 'blocks'))
    files = float(get_element_text(node, 'files'))
    return bytes, blocks, files

def handle_total(total, indent):
    bytes, blocks, files = find_bytes_blocks_files(total)
    #print(f'{indent}total: bytes={bytes!r} blocks={blocks!r} files={files!r}')
    return bytes, blocks, files, -1

def handle_files_older_than(node, indent):
    age_in_seconds = node.getAttribute('age_in_seconds')
    bytes, blocks, files = find_bytes_blocks_files(node)
    #print(f'{indent}older than {age_in_seconds!r} seconds: bytes={bytes!r} blocks={blocks!r} files={files!r}')
    return bytes, blocks, files, age_in_seconds

def handle_file(long_format, filename):
    dom = xml.dom.minidom.parse(filename)
    handle_usage(long_format, dom)

def __main():
    if sys.argv[1] == 'long':
        handle_file(True, sys.argv[2])
    elif sys.argv[1] == 'short':
        handle_file(False, sys.argv[2])
    else:
        sys.stderr.write('Syntax: make-lustre-report.py [ long | short ] /path/to/input.xml\n')
        sys.stderr.write(f'Unknown mode "{sys.argv[1]}"\n')
        exit(1)

if __name__ == '__main__':
    __main()
