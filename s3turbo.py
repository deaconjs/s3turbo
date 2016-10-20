#!/usr/bin/env python
import math
import multiprocessing.pool
import os.path
import re
import shutil
import sys
import time
import traceback
import boto
import transferlib

owner = 'deaconjs'

# assume moltype data is in owner.moltype.in/moltypebatches


def recursive_glob(rootdir='.', suffix=''):
    return [os.path.join(looproot, filename)
            for looproot, _, filenames in os.walk(rootdir)
            for filename in filenames if filename.endswith(suffix)]

class NoDaemonProcess(multiprocessing.Process):
    def _get_daemon(self):
        return False
    def _set_daemon(self, value):
        pass
    daemon = property(_get_daemon, _set_daemon)


class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess

include_string = None
exclude_string = None
remove_prefix = None
quiet_flag = False
for x, arg in enumerate(sys.argv):
    if arg == "include" or arg == "-include" or arg == "--include":
        include_string = sys.argv[x+1]
        del sys.argv[x+1]
        del sys.argv[x]
for x, arg in enumerate(sys.argv):
    if arg == "exclude" or arg == '-exclude' or arg == '--exclude':
        exclude_string = sys.argv[x+1]
        del sys.argv[x+1]
        del sys.argv[x]
for x, arg in enumerate(sys.argv):
    if arg == "remove_prefix":
        remove_prefix = sys.argv[x+1]
        del sys.argv[x+1]
        del sys.argv[x]

for x, arg in enumerate(sys.argv):
    if arg == 'quiet':
        quiet_flag = True
        del sys.argv[x]

if include_string:
    print "including files with string %s"%(include_string)
if exclude_string:
    print "excluding files with string %s"%(exclude_string)
if remove_prefix:
    print "removing string %s from destination path"%(remove_prefix)

dryrun_flag = False
for x, arg in enumerate(sys.argv):
    if arg == "dryrun" or arg == '-dryrun' or arg == '--dryrun':
        dryrun_flag = True
        del sys.argv[x]
if dryrun_flag:
    print "dryrun set to true"

reduced_redundancy_flag = False
for x, arg in enumerate(sys.argv):
    if arg == 'reduced_redundancy' or arg == 'rr' or arg == '-reduced_redundancy' or arg == '-rr' or arg == '--reduced_redundancy' or arg == '--rr':
        reduced_redundancy_flag = True
        del sys.argv[x]
if reduced_redundancy_flag:
    print 'reduced redundancy set to true'

# add these to command-line arguments
overwrite = False
cores = multiprocessing.cpu_count()
concurrency = cores
remove_source_for_copies = False

conn = boto.connect_s3()
remap_flag = False        # note that the remap flag only currently works for move functions

# currently cannot map to top level directory. fix me please
copy_pattern     = r's3:\/\/.+\/.+ s3:\/\/.+\/.+'
download_pattern = r's3:\/\/.+\/.+ local:\/\/.+\/.+'
upload_pattern   = r'local:\/\/.+\/.+ s3:\/\/.+\/.+'

# command line example: s3turbo.py some_database:study1_equals_studyID sorted.bam sorted.bai
# e.g. criteria = 'some_database:study1_equals_studyID'
lines = []
criteria = ""
filetypes = []
if len(sys.argv) > 2:
    if sys.argv[2] == "remap":
        if os.path.isfile(sys.argv[1]):
            remap_flag = True
            lines = open(sys.argv[1], 'r').readlines()
        else:
            print "Fatal Error: key name file %s does not exist. A key file must be included for remapping"%(sys.argv[1])
            sys.exit()
    else:
        if (sys.argv[2].startswith('s3') or sys.argv[2].startswith('local')):
            command = ' '.join(sys.argv[1:]).strip()
            if re.match(copy_pattern, command) or re.match(download_pattern, command) or re.match(upload_pattern, command):
                source = sys.argv[1]
                destination = sys.argv[2]
                if not source.endswith('/') and not destination.endswith('/'):
                    lines = [command]
                elif (source.endswith('/') and not destination.endswith('/')) or (not source.endswith('/') and destination.endswith('/')):
                    print "Fatal error: for rsync functionality both source and target specifications should end with frontslashes"
                    sys.exit()
                else:    # rsync functionality
                    if re.match(copy_pattern, command):    # bucket-to-bucket
                        target_bucketname, target_keyprefix = destination.split('/')[2], '/'.join(destination.split('/')[3:])
                        source_bucketname, source_keyprefix = source.split('/')[2], '/'.join(source.split('/')[3:])
                        target_bucket = conn.get_bucket(target_bucketname)
                        source_bucket = conn.get_bucket(source_bucketname)
                        target_lst = target_bucket.list(prefix=target_keyprefix)
                        lst = source_bucket.list(prefix=source_keyprefix)
                        for key in lst:
                            if key.name.endswith('/'):
                                continue
                            source_key = source_bucket.get_key(key.name)
                            target_key = target_bucket.get_key('%s/%s'%(target_keyprefix, key.name))
                            # copy if the target key does not exist, or if the target and source keys are not the same size
                            if target_key:
                                if target_key.size != source_key.size:
                                    lines.append('s3://%s/%s s3://%s/%s%s'%(source_bucketname, key.name, target_bucketname, target_keyprefix, key.name))
                            else:
                                lines.append('s3://%s/%s s3://%s/%s%s'%(source_bucketname, key.name, target_bucketname, target_keyprefix, key.name))
                    elif re.match(download_pattern, command):
                        target_directory = '/'.join(destination.split('/')[2:])
                        source_bucketname, source_keyprefix = source.split('/')[2], '/'.join(source.split('/')[3:])
                        source_bucket = conn.get_bucket(source_bucketname)
                        lst = source_bucket.list(prefix=source_keyprefix)
                        
                        for key in lst:
                            if key.name.endswith('/'):
                                continue
                            source_key = source_bucket.get_key(key.name)
                            if os.path.isfile('%s/%s'%(target_directory, key.name)):
                                if source_key.size != os.path.getsize('%s/%s'%(target_directory, key.name)):
                                    lines.append('s3://%s/%s local://%s/%s'%(source_bucketname, key.name, target_directory, key.name))
                            else:
                                lines.append('s3://%s/%s local://%s/%s'%(source_bucketname, key.name, target_directory, key.name))
                    elif re.match(upload_pattern, command):
                        source_directory = '/'.join(source.split('/')[2:])
                        target_bucketname, target_keyprefix = destination.split('/')[2], '/'.join(destination.split('/')[3:])
                        source_files = recursive_glob('%s'%(source_directory))
                        print len(source_files)
                        target_bucket = conn.get_bucket(target_bucketname)
                        for source_file in source_files:
                            if not os.path.isfile(source_file):
                                continue
                            target_key = target_bucket.get_key('%s/%s'%(target_keyprefix, source_file))
                            if target_key:
                                if target_key.size != os.path.getsize(source_file):
                                    lines.append('local://%s s3://%s/%s%s'%(source_file, target_bucketname, target_keyprefix, source_file))
                            else:
                                lines.append('local://%s s3://%s/%s%s'%(source_file, target_bucketname, target_keyprefix, source_file))
                    else:
                        print "Fatal error: pattern %s does not match built-in patterns"%(command)
                        sys.exit()
            else:
                print 'command does not match any transfer commands\n  %s'%(command)
                sys.exit()
        else:
            criteria = sys.argv[1].split(':')
            filetypes = sys.argv[2:]
elif len(sys.argv) == 2:
    if os.path.isfile(sys.argv[1]):
        lines = open(sys.argv[1], 'r').readlines()
    else:
        if sys.argv[1].split(':') > 1 and sys.argv[1].split(':')[1].startswith('vw'):
            criteria = sys.argv[1].split(':')
            filetypes = ['all']
        else:
            print "Fatal Error: argument %s is not an existing file and is not a database selection criteria"%(sys.argv[1])
            sys.exit()
else:
    print "Usage:"
    print "key-name file"
    print "  s3turbo.py key_name_file [remap]"
    print "OR single-line transfer:"
    print "  to download - s3turbo.py s3://bucket_name/key_name local://file_path"
    print "  to upload   - s3turbo.py local://file_path s3://bucket_name/key_name"
    print "  to copy     - s3turbo.py s3://bucket_name/key_name s3://bucket_name/key_name"
    print "OR transfer by sql query and file names:"
    print "  s3turbo.py table_name:value_comparator_column filename_endswith filename_endswith ..."
    print "    e.g. s3turbo.py some_database:study1_equals_studyID sorted.bam sorted.bai"
    print "    comparator can currently only be 'equals'"
    print "OR rsync functionality (end with slashes)"
    print "  s3turbo.py (s3|local)://path/ (s3|local):path/ [include include_string] [exclude exclude_string]"
    print "    e.g. s3turbo.py local:///home/username/path/ s3://owner.run.etc/etc_dir include .py exclude .pyc"
    print "AND dryrun flag"
    print "  s3turbo.py args [dryrun] args"
    print "AND reduced_redundancy flag"
    print "  s3turbo.py args [reduced_redundancy] args"
    print ""
    print "The key_name_file format should follow the same conventions, one line per transfer."
    print "The remap flag is for transferring mapped data between buckets."
    print "  It removes entries from source mapping files and adds them to target mapping files."
    print "The dryrun flag prints out the files to be transferred, without transferring them. Output is in the standard s3/local://path format."
    print "The reduced_redundancy flag saves some money but has slightly higher odds of data loss"
    sys.exit()

# apply include and exclude strings
fixedlines = []
for line in lines:
    if include_string and include_string in line:
        fixedlines.append(line)
    elif not include_string:
        fixedlines.append(line)
fixedlines2 = []
for line in fixedlines:
    if exclude_string and exclude_string not in line:
        fixedlines2.append(line)
    elif not exclude_string:
        fixedlines2.append(line)
lines = fixedlines2

# dryrun?
if dryrun_flag:
    for line in lines:
        print line
    sys.exit()

"""
if criteria and len(filetypes) > 0:
    # this simple version just uses transferlib built-in SQL queries. More complex queries can be added in its place
    result = 'fileLocation'
    table = criteria[0]
    (value, comparator, query) = criteria[1].split('_')
    raw_lines = transferlib.get_batch_results_from_script(result, table, query, [value])
    filtered_hash = {}
    for line in raw_lines:
        for filetype in filetypes:
            if line[0].rstrip('\r\n').endswith(filetype):
                filtered_hash[line[0]] = 0

    print "%s lines returned from sql query"%(len(raw_lines))
    print "%s files meeting these criteria"%(len(filtered_hash.keys()))
    moltypes = []
    for line in filtered_hash.keys():
        if line.startswith('rnabatches') and 'rna' not in moltypes:
            moltypes.append('rna')
        elif line.startswith('mirnabatches') and 'mirna' not in moltypes:
            moltypes.append('mirna')
    for moltype in moltypes:
        mapping_filename = transferlib.fetch_mapping('%s.run.%s' % (owner, moltype))
        mapfilelines = open(mapping_filename, 'r').readlines()
        for line in mapfilelines:
            (key, sample, participant, batch, bucket, loc) = line.rstrip('\r\n').split(',')
            if loc in filtered_hash:
                lines.append('s3://%s.run.%s/%s local://%s'%(owner, moltype, key, loc))
    print "%s files found."%(len(lines))
"""

copy_file_list = []
upload_file_list = []
download_file_list = []


for line in lines:
    line = line.rstrip('\r\n')
    if len(line) > 0:
        # i.e. s3://owner.ready.dna/key_name s3://owner.in.dna/key_name
        move_type = None
        if re.match(copy_pattern, line):
            move_type = 'copy'
        elif re.match(download_pattern, line):
            move_type = 'download'
        elif re.match(upload_pattern, line):
            move_type = 'upload'
        else:
            print "key name pattern %s\ndoes not match standard formatting. Use:"%(line)
            print "  Copy     -> \"s3://bucket/keyname s3://bucket/keyname\""
            print "  Download -> \"s3://bucket/keyname local://path\""
            print "  Upload   -> \"local://path s3://bucket/keyname\""
            sys.exit()

        line = re.sub('local', '', line)
        line = re.sub('s3:', ':', line)
        source_stripped   = line.split("://")[1].strip()
        dest_stripped     = line.split("://")[2].strip()
        source_tokens     = source_stripped.split('/')
        dest_tokens       = dest_stripped.split('/')
        if move_type in ['copy', 'download']:
            source_bucketname = source_tokens[0].strip()
            source_keyname    = '/'.join(source_tokens[1:]).strip()
        else:
            source_bucketname = None
            source_keyname    = os.path.abspath(source_stripped)
        if move_type in ['copy', 'upload']:
            dest_bucketname   = dest_tokens[0].strip()
            dest_keyname      = '/'.join(dest_tokens[1:]).strip()
            if remove_prefix:
                dest_keyname = dest_keyname.replace(remove_prefix, "")
        else:
            dest_bucketname   = None
            dest_keyname      = os.path.abspath(dest_stripped)
            if remove_prefix:
                dest_keyname = dest_keyname.replace(remove_prefix, "")
        # validate source and destination
        if move_type in ['copy', 'download']:
            try:
                source_bucket = conn.get_bucket(source_bucketname)
            except Exception, exc:
                print "Fatal Error: source bucket %s not found: exception %s"%(source_bucketname, exc)
                sys.exit()

        if move_type in ['copy', 'upload']:
            try:
                dest_bucket = conn.get_bucket(dest_bucketname)
            except Exception, exc:
                print "Fatal Error: destination bucket %s not found: exception %s"%(dest_bucketname, exc)
                sys.exit()

        if move_type == 'copy':
            if not source_bucket.get_key(source_keyname):
                print "Error copying: source key %s not found in bucket %s"%(source_keyname, source_bucketname)
                continue
        elif move_type == 'download':
            if not source_bucket.get_key(source_keyname):
                print "Error downloading: source key %s not found in bucket %s"%(source_keyname, source_bucketname)
                continue
            targetdir = '/'.join(dest_keyname.split('/')[:-1])
            if not os.path.isdir(targetdir):
                try:
                    os.makedirs(targetdir)
                except OSError, exc:
                    print "Fatal Error: Failed to create directory %s for download %s, error %s"%(targetdir, dest_keyname, exc)
                    sys.exit()
        elif move_type == 'upload':
            if not os.path.isfile(source_keyname):
                print "Error: source file %s not found locally"%(source_keyname)

        # see if file has already been transferred
        if overwrite == False and  move_type == 'copy':
            #if not source_bucket.get_key(source_keyname) and dest_bucket.get_key(dest_keyname):
            if dest_bucket.get_key(dest_keyname):
                print "Source key %s has already been transferred from source bucket %s to dest bucket %s. Skipping."%(source_keyname, source_bucketname, dest_bucketname)
                continue
        elif overwrite == False and move_type == 'download':
            if os.path.isfile(dest_keyname):
                source_key = source_bucket.get_key(source_keyname)
                local_size = os.path.getsize(dest_keyname)
                if source_key and source_key.size == local_size:
                    print "Source key %s has already been downloaded and is the same size on disk. Skipping."%(source_keyname)
                    continue
        elif overwrite == False and move_type == 'upload':
            dest_key = dest_bucket.get_key(dest_keyname)
            if dest_key:
                local_size = os.path.getsize(source_keyname)
                if dest_key.size == local_size:
                    print "File %s has already been uploaded and is the same size in the destination bucket. Skipping."%(source_keyname)
                    continue

        # store to transfer
        if move_type == 'copy':
            source_key = source_bucket.get_key(source_keyname)
            source_size = source_key.size
            copy_file_list.append([source_bucketname, source_keyname, dest_bucketname, dest_keyname, source_size])
        elif move_type == 'upload':
            local_size = os.path.getsize(source_keyname)
            upload_file_list.append([source_bucketname, source_keyname, dest_bucketname, dest_keyname, local_size])
        elif move_type == "download":
            source_key = source_bucket.get_key(source_keyname)
            source_size = source_key.size
            # append filename to dest_keyname if dest_keyname is a directory
            if os.path.isdir(dest_keyname):
                source_filename = source_keyname.split('/')[-1]
                dest_keyname = '%s/%s'%(dest_keyname, source_filename)
            download_file_list.append([source_bucketname, source_keyname, dest_bucketname, dest_keyname, source_size])

# sort the lists
copy_file_list.sort(key=lambda x: x[4])
upload_file_list.sort(key=lambda x: x[4])
download_file_list.sort(key=lambda x: x[4])

sample_hash = {}

for transfer_type in ['download', 'copy', 'upload']:
    if transfer_type == 'copy' and remove_source_for_copies:
        transfer_type = 'move'
    transfer_files = []
    transfer_function = None
    if transfer_type == 'download':
        transfer_files = download_file_list
        transfer_function = transferlib.s3_download
    elif transfer_type == 'move':
        transfer_files = copy_file_list
        transfer_function = transferlib.s3_move
    elif transfer_type == 'copy':
        transfer_files = copy_file_list
        transfer_function = transferlib.s3_copy
    elif transfer_type == 'upload':
        transfer_files = upload_file_list
        transfer_function = transferlib.s3_upload
    cnt = 0
    for k in range(int(math.ceil(len(transfer_files)/(0.0+concurrency)))):
        keys_to_remove = []
        keys_to_add    = []
        rangestart, rangecap = k * concurrency, (k+1)*concurrency
        if k >= int(math.ceil(len(transfer_files)/(0.0+concurrency)))-1:
            rangecap = len(transfer_files)

        print "range %d to %d"%(rangestart, rangecap)

        results = []
        answers = []
        pool = MyPool(concurrency)
        for i, j in enumerate(range(rangestart, rangecap)):
            key = transfer_files[j]
            if transfer_type == 'move':
                keys_to_remove.append(key[1])
                keys_to_add.append(key[3])
            elif transfer_type == 'copy':
                keys_to_add.append(key[3])
            elif transfer_type == 'upload':
                keys_to_add.append(key[3])

            if cnt%50 == 0:
                print "\n**********  %s files done  **********\n"%(cnt)
            source_bucketname, source_keyname, dest_bucketname, dest_keyname = key[0], key[1], key[2], key[3]
            d = {}
            if transfer_type in ['copy', 'move']:
                args = [source_bucketname, source_keyname, dest_bucketname, dest_keyname]
                if reduced_redundancy_flag:
                    d = dict(reduced_redundancy=True)
            elif transfer_type == 'upload':
                args = [source_keyname, dest_bucketname, dest_keyname]
                if reduced_redundancy_flag:
                    d = dict(reduced_redundancy=True)
            elif transfer_type == 'download':
                args = [source_bucketname, source_keyname, dest_keyname]
            #if quiet_flag:
                
            if d:
                results.append(pool.apply_async(transfer_function, args, d))
            else:
                results.append(pool.apply_async(transfer_function, args))
            cnt += 1

        failkeys = []
    
        for i, j in enumerate(range(rangestart, rangecap)):
            try:
                answers.append(results[i].get(timeout=999999999))
            except Exception, exc:
                f = open('./fail_log', 'a')
                tim = time.strftime('%X %x %Z')
                frmt_exc = traceback.format_exc()
                print "Error %s\n   writing to fail_log at %s\n   %s - %s\n    %s"%(exc, tim, j, transfer_files[j], frmt_exc)
                f.write("pool.apply_async failed in s3_%s.py. Failed on %s, %s\n"%(transfer_type, j, transfer_files[j]))
                f.write("    ready=%s, successful=%s\n"%(results[i].ready(), results[i].successful()))
                f.write("    time %s, exception %s, %s"%(tim, exc, frmt_exc))
                f.close()
                failkeys.append(transfer_files[j][0])
    
        if remap_flag and transfer_type == 'move':
            outlines = []
            source_mapping_filename = transferlib.fetch_mapping(source_bucket)
            dest_mapping_filename   = transferlib.fetch_mapping(dest_bucketname)
            reserved_lines = open(dest_mapping_filename, 'r').readlines()

            f = open(source_mapping_filename, 'r')
            lines = f.readlines()
            for line in lines:
                key = line.rstrip('\r\n').split(',')[0]
                if key in keys_to_remove and key not in failkeys:
                    for i in range(len(keys_to_remove)):
                        if keys_to_remove[i] != keys_to_add[i]:
                            tokens = line.split(',')
                            tokens[0] = keys_to_add[i]
                            line = ','.join(tokens)
                    reserved_lines.append(line)
                else:
                    outlines.append(line)

            f = open('%s.tmp'%(source_bucket), 'w')
            f.writelines(outlines)
            f.close()
            f = open('%s.tmp'%(dest_bucketname), 'w')
            f.writelines(reserved_lines)
            f.close()

            shutil.copyfile('%s.tmp'%(source_bucket), source_mapping_filename)
            shutil.copyfile('%s.tmp'%(dest_bucketname), dest_mapping_filename)
            transferlib.upload_mapping(source_bucket)
            transferlib.upload_mapping(dest_bucketname)
        elif remap_flag:
            print "Note that the remap flag only works for move transfers"

        pool.close()
        pool.join()

