import boto
import os.path
from boto.s3.key import Key
import os
import sys
import mimetypes
import math
from filechunkio import FileChunkIO
import multiprocessing
import time
import __builtin__
import socket
import tempfile


""" QUICK REFERENCE """

""" EXAMPLE """
"""
import transferlib

# some basic transfers
transferlib.s3_upload("./transferlib.py", "deaconjs.etc.scripts", "transferlib.py")
transferlib.s3_move("deaconjs.etc.scripts", "transferlib.py", "deaconjs.etc.packages", "transferlib/transferlib.py")
transferlib.s3_download("deaconjs.etc.packages", "transferlib.py", "./transferlib.backedup.py")

upload was taken from gist.github.com/fabiant7t/924094
download was taken from github.com/mumrah/s3-multipart/blob/master/s3-mp-download.py

"""


""" TRANSFER FUNCTIONS """

# copy the key from source to destination bucket under a new name 
# s3_copy(source_bucket_name, source_keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={})

# remove a key from a bucket
# s3_remove(key, bucket_name)

# move a key from one bucket to another
# s3_move(source_bucket_name, keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={})

# upload a file
# s3_upload(source_path, bucketname, keyname, headers={}, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), reduced_redundancy=False)

# download a file
# s3_download(bucketname, keyname, dest_path, headers={}, parallel_processes=multiprocessing.cpu_count())

# download a part of a file
# s3_download_partial(bucketname, keyname, dest_path, headers={}, parallel_processes=multiprocessing.cpu_count(), startbit, endbit)

# add the transfer record to the local log file
# s3_log_append(type, source, destination, size, start_time, total_time, passed_size_check, logfile="./transferlib.log")

""" MD5 FUNCTIONS """

# the following two just compare etags and will fail for large files.
# compare_md5_local_to_s3(local_file_name, original_bucket, original_loc):
# compare_md5_s3_to_s3(dest_bucket, dest_key, orig_bucket, orig_key):

# this one needs to be implemented to read manifest files for md5s, and will be custom per s3turbo implementation
# fetch_vendor_md5(vendor_bucket, batch, sample):

""" DOCUMENTATION """


"""
s3_copy(source_bucket_name, source_keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={})
    s3_copy requires a boto Key object, boto connect_s3 object, source and destination bucket names (strings), 
    and the new key name. The function can make a lazy guess at mimetype.

    Copy is performed in a parallelized manner. It uses MultiPartUpload.copy_part_from_key for the transfer, 
    and if this function throws an exception s3_copy retries up to a default of 10 times. If not all parts
    are transferred properly s3_copy cancels the upload and produces an error message. 


s3_remove(key, bucket_name) 
    s3_remove simply uses the boto bucket.delete_key function to remove a key
    requires a key object, a string for the bucket name, and a boto connect_s3 object


s3_move(source_bucket_name, keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={})
    s3_move uses the s3_copy function, followed by s3_delete. Before deleting the file size is compared between old 
    and new keys. If the file size is different s3_move produces an error message and exits. See s3_copy for a
    decription of arguments.


s3_upload(source_path, bucketname, keyname, headers={}, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), reduced_redundancy=False)
    s3_upload requires a destination bucket name, the path to the local file, a key name to store it under, and a 
    boto connect_s3 object. headers can be included and s3_upload can make a guess as to mimetype.

    For files over 50 Mb, multi-part upload is performed in a parallelized manner, with a default of 8 threads. It uses 
    MultiPartUpload.copy_part_from_key for the transfer, and if this function throws an exception s3_copy retries 
    up to a default of 10 times. If not all parts are transferred properly s3_copy cancels the upload and produces 
    an error message. 

s3_download(bucketname, keyname, dest_path, headers={}, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count())
    s3_upload requires source bucket and key names, the destination path, and a boto connect_s3 object. For files over
    50 Mb, multi-part download is performed in a parallel manner, with a default of multiprocessing.cpu_count() threads.

s3_log_append(source, destination, size, start_time, total_time, logfile="./transferlib.log")

"""
 



""" FUNCTIONS """

def s3_copy(source_bucket_name, source_keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={}, quiet=False, reduced_redundancy=False):
    conn = boto.connect_s3()
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    t1 = time.time()
    try:
        sbucket = conn.get_bucket(source_bucket_name)
    except Exception, exc:
        print "Fatal Error: source bucket %s not found: exception %s"%(source_bucket_name, exc)
        sys.exit()    
    key = sbucket.get_key(source_keyname)
    if not key:
        print "Fatal Error in s3_copy: source key %s not found in bucket %s"%(key, sbucket)
        sys.exit()
    try:
        dbucket = conn.get_bucket(dest_bucket_name)
    except Exception, exc:
        print "Fatal Error: destination bucket %s not found: exeption %s"%(dest_bucket_name, exc)
        sys.exit()
    if guess_mimetype:
        mtype = mimetypes.guess_type(key.name)[0] or 'application/octet-stream'
        headers.update({'Content-Type':mtype})
    source_size = key.size
    bytes_per_chunk = 50 * 1024 * 1024
    chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
    while chunk_amount > 200:
        bytes_per_chunk += 5242880
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    size_mb = source_size / 1024 / 1024
    if not quiet:
        print "copying %s %s b (%s) in %s chunks"%(source_keyname, source_size, size_mb, chunk_amount)
    if source_size < bytes_per_chunk:
        key.copy(dest_bucket_name, newkeyname, reduced_redundancy=reduced_redundancy)
    else:
        mp = dbucket.initiate_multipart_upload(newkeyname, headers=headers, reduced_redundancy=reduced_redundancy)
        __builtin__.global_download_total = chunk_amount
        __builtin__.global_download_progress = 0
        pool = multiprocessing.Pool(processes=parallel_processes)
        for i in range(chunk_amount):
            offset = i * bytes_per_chunk
            remaining_bytes = source_size - offset
            bytes = min([bytes_per_chunk, remaining_bytes])
            part_num = i + 1
            pool.apply_async(_copy_part, [dest_bucket_name, mp.id, part_num, source_bucket_name, int(offset), int(bytes), conn, key, parallel_processes, quiet, reduced_redundancy])
        pool.close()
        pool.join()
        if len(mp.get_all_parts()) == chunk_amount:
            mp.complete_upload()
        else:
            print "cancelling copy for %s - %d chunks uploaded, %d needed\n"%(source_keyname, len(mp.get_all_parts()), chunk_amount)
            mp.cancel_upload()

    if not quiet:
        print
    t2 = time.time()-t1
    source = "S3:%s:%s"%(source_bucket_name, source_keyname)
    destination = "S3:%s:%s"%(dest_bucket_name, newkeyname)
    size = dbucket.lookup(newkeyname).size
    total_time = t2
    if size == source_size:
        passed_size_check = True
    else:
        passed_size_check = False
    s3_log_append("s3_copy", source, destination, size, start_time, total_time, passed_size_check, logfile="./itmi_s3lib.log")
    if not quiet:
        print "Finished copying %0.2fM in %0.2fs (%0.2fmbps)"%(size/1024/1024, t2, 8*size_mb/t2)

def _copy_part(dest_bucket_name, multipart_id, part_num, source_bucket_name, offset, bytes, conn, key, threads, quiet, reduced_redundancy, amount_of_retries=10):
    def _copy(retries_left=amount_of_retries):
        try:
            bucket = conn.get_bucket(dest_bucket_name)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    mp.copy_part_from_key(source_bucket_name, key.name, part_num, offset, offset+bytes-1)
                    break
        except Exception, exc:
            print "failed partial upload, exception %s\nRetries left %s"%(exc, retries_left-1)
            if retries_left:
                _copy(retries_left=retries_left-1)
            else:
                raise exc
    _copy()
    import __builtin__
    __builtin__.global_download_progress += threads
    g = __builtin__.global_download_progress
    t = __builtin__.global_download_total
    if not quiet:
        sys.stdout.write("\rsomewhere around %d%% complete"%(int(100*g/(0.0+t))))
        sys.stdout.flush()


def s3_remove(bucket_name, key_name):
    conn = boto.connect_s3()
    try:
        b = conn.get_bucket(bucket_name)
    except Exception, exc:
        print "Fatal Error: source bucket %s not found, exception %s"%(bucket_name, exc)
        sys.exit()
    key = b.get_key(key_name)
    if not key:
        print "Fatal Error in remove: source key %s not found in bucket %s"%(key_name, sbucket)
        sys.exit()
    
    b.delete_key(key)


def s3_move(source_bucket_name, keyname, dest_bucket_name, newkeyname, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), headers={}, quiet=False):
    conn = boto.connect_s3()
    try:
        sb = conn.get_bucket(source_bucket_name)
    except Exception, exc:
        print "Fatal Error: source bucket %s not found, exception %s"%(source_bucket_name, exc)
        sys.exit()
    oldkey = sb.get_key(keyname)
    if not oldkey:
        print "Fatal Error in move: key %s not found in bucket %s"%(keyname, source_bucket_name)
        sys.exit()
    s3_copy(source_bucket_name, keyname, dest_bucket_name, newkeyname, guess_mimetype, parallel_processes, headers, quiet)
    db = conn.get_bucket(dest_bucket_name)
    newkey = db.get_key(newkeyname)
    if newkey.size == oldkey.size:
        s3_remove(source_bucket_name, keyname)
    else:
        print "Fatal S3 copy error"
        print "source key %s from %s size %s is different from destination key %s from %s size %s"%(oldkey.name, source_bucket_name, oldkey.size, newkey.name, dest_bucket_name, newkey.size)
        sys.exit()

def _upload_part(bucketname, multipart_id, part_num, source_path, offset, bytes, conn, threads, quiet, amount_of_retries=10):
    def _upload(retries_left=amount_of_retries):
        try:
            bucket = conn.get_bucket(bucketname)
            for mp in bucket.get_all_multipart_uploads():
                if mp.id == multipart_id:
                    with FileChunkIO(source_path, 'r', offset=offset, bytes=bytes) as fp:
                        mp.upload_part_from_file(fp=fp, part_num=part_num)
        except Exception, exc:
            print "failed multi-part upload attempt, exception %s"%(exc)
            sys.exit()
            if retries_left:
                _upload(retries_left=retries_left-1)
            else:
                raise exc
    _upload()
    import __builtin__
    __builtin__.global_download_progress += threads
    g = __builtin__.global_download_progress
    t = __builtin__.global_download_total
    if not quiet:
        sys.stdout.write("\rsomewhere around %d%% complete"%(int(100*g/(0.0+t))))
        sys.stdout.flush()


def s3_upload(source_path, bucketname, keyname, headers={}, guess_mimetype=True, parallel_processes=multiprocessing.cpu_count(), quiet=False, reduced_redundancy=False):
    conn = boto.connect_s3()
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    t1 = time.time()
    if not os.path.isfile(source_path):
        print "Fatal Error: source path %s does not exist or is not a file"%(source_path)
        sys.exit()
    source_size = os.stat(source_path).st_size
    bytes_per_chunk = 50 * 1024 * 1024
    try:
        bucket = conn.get_bucket(bucketname)
    except Exception, exc:
        print "Fatal Error: destination bucket %s does not exist - exception %s"%(bucketname, exc)
        sys.exit()

    if source_size <= bytes_per_chunk:
        k = boto.s3.key.Key(bucket)
        k.key = keyname
        b = k.set_contents_from_filename(source_path, reduced_redundancy=reduced_redundancy)
    else:
        if guess_mimetype:
            mtype = mimetypes.guess_type(keyname)[0] or 'application/octet-stream'
            headers.update({'Content-Type':mtype})
        mp = bucket.initiate_multipart_upload(keyname, headers=headers, reduced_redundancy=reduced_redundancy)
 
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

        while chunk_amount > 200:
            bytes_per_chunk += 5242880
            chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))
        __builtin__.global_download_total = chunk_amount
        __builtin__.global_download_progress = 0

        pool = multiprocessing.Pool(processes=parallel_processes) 
        size_mb = source_size / 1024 / 1024
        if not quiet:
            print "uploading %s (%s) in %s chunks"%(source_size, size_mb, chunk_amount)
        for i in range(chunk_amount):
            offset = i*bytes_per_chunk
            remaining_bytes = source_size - offset
            bytes = min([bytes_per_chunk, remaining_bytes])
            part_num = i + 1
            pool.apply_async(_upload_part, [bucketname, mp.id, part_num, source_path, offset, bytes, conn, parallel_processes, quiet])
        pool.close()
        pool.join()
        if len(mp.get_all_parts()) == chunk_amount:
            mp.complete_upload()
        else:
            print "canceling upload for %s - %d chunks uploaded, %d needed\n"%(source_path, len(mp.get_all_parts()), chunk_amount)
            mp.cancel_upload()
    if not quiet:
        print
    t2 = time.time()-t1
    source = "local:%s:%s"%(socket.gethostname(), source_path)
    destination = "S3:%s:%s"%(bucketname, keyname)
    size = conn.get_bucket(bucketname).lookup(keyname).size
    total_time = t2
    if source_size == size:
        passed_size_check = True
    else:
        passed_size_check = False
    s3_log_append("s3_upload", source, destination, size, start_time, total_time, passed_size_check, logfile="./itmi_s3lib.log")
    if not quiet:
        print "Finished uploading %0.2fM in %0.2fs (%0.2fmbps)"%(size/1024/1024, t2, 8*size/t2)


def _download_part(args):
    conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet = args
    resp = conn.make_request("GET", bucket=bucketname, key=keyname, headers={'Range':"bytes=%d-%d"%(min_byte, max_byte)})
    fd = os.open(fname, os.O_WRONLY)
    os.lseek(fd, min_byte, os.SEEK_SET)
    chunk_size = min((max_byte-min_byte), split*1024*1024)
    s=0
    try:
        while True:
            data = resp.read(chunk_size)
            if data == "":
                break
            os.write(fd, data)
            s += len(data)
        os.close(fd)
        s = s / chunk_size
    except Exception, err:
        if (current_tries > max_tries):
            print "Error downloading, %s"%(err)
            sys.exit()
        else:
            time.sleep(3)
            current_tries += 1
            _download_part((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))
    import __builtin__
    __builtin__.global_download_progress += threads
    g = __builtin__.global_download_progress
    t = __builtin__.global_download_total
    if not quiet:
        sys.stdout.write("\rsomewhere around %d%% complete"%(int(100*g/(0.0+t))))
        sys.stdout.flush()

def _download_part_with_double_check(args):
    conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet = args
    resp1 = conn.make_request("GET", bucket=bucketname, key=keyname, headers={'Range':"bytes=%d-%d"%(min_byte, max_byte)})
    resp2 = conn.make_request("GET", bucket=bucketname, key=keyname, headers={'Range':"bytes=%d-%d"%(min_byte, max_byte)})
    buc = conn.get_bucket('itmi.run.etc')
    #k = boto.s3.key.Key(buc)
    #k.key = 'rm_me_asdf'
    #k.set_contents_from_stream(resp.fp)
    fd = os.open(fname, os.O_WRONLY)
    os.lseek(fd, min_byte, os.SEEK_SET)
    chunk_size = min((max_byte-min_byte), split*1024*1024)
    s=0
    try:
        while True:
            data1 = resp1.read(chunk_size)
            data2 = resp2.read(chunk_size)
            if data1 == "":
                break
	    if data1 == data2:
                os.write(fd, data1)
                s += len(data1)
            else:
                time.sleep(3)
		current_tries += 1
                _download_part_with_double_check((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))
        os.close(fd)
        s = s / chunk_size
    except Exception, err:
        if (current_tries > max_tries):
            print "Error downloading, %s"%(err)
            sys.exit()
        else:
            time.sleep(3)
            current_tries += 1
            _download_part_with_double_check((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))
    import __builtin__
    __builtin__.global_download_progress += threads
    g = __builtin__.global_download_progress
    t = __builtin__.global_download_total
    if not quiet:
        sys.stdout.write("\rsomewhere around %d%% complete"%(int(100*g/(0.0+t))))
        sys.stdout.flush()

def _download_part_with_size_check(args):
    conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet = args
    resp = conn.make_request("GET", bucket=bucketname, key=keyname, headers={'Range':"bytes=%d-%d"%(min_byte, max_byte)})
    fd = os.open(fname, os.O_WRONLY)
    os.lseek(fd, min_byte, os.SEEK_SET)
    chunk_size = min((max_byte-min_byte), split*1024*1024)
    s=0
    size_difference = -37
    second_message_size = 38
    try:
        while True:
            data = resp.read(chunk_size)
            if data == "":
                break
            sz = sys.getsizeof(data)
            #print "testing packet  %s %s %s %s attempt %s, %s, %s"%(chunk_size, sz, chunk_size-sz, size_difference, current_tries, min_byte, max_byte-min_byte)
            if chunk_size - sz != size_difference and sz != second_message_size:
                print "failed packet. retrying %s %s %s %s attempt %s, %s, %s"%(chunk_size, sz, chunk_size-sz, size_difference, current_tries, min_byte, max_byte-min_byte)
                time.sleep(1)
                current_tries += 1
                _download_part_with_size_check((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))
            os.write(fd, data)
            s += len(data)
        os.close(fd)
        s = s / chunk_size
    except Exception, err:
        if (current_tries > max_tries):
            print "Error downloading, %s"%(err)
            sys.exit()
        else:
            time.sleep(3)
            current_tries += 1
            _download_part_with_size_check((conn, bucketname, keyname, fname, split, min_byte, max_byte, max_tries, current_tries, threads, quiet))
    import __builtin__
    __builtin__.global_download_progress += threads
    g = __builtin__.global_download_progress
    t = __builtin__.global_download_total
    if not quiet:
        sys.stdout.write("\rsomewhere around %d%% complete"%(int(100*g/(0.0+t))))
        sys.stdout.flush()


def _gen_byte_ranges(size, num_parts):
    part_size = int(math.ceil(1. * size / num_parts))
    for i in range(num_parts):
    #for i in range(50, 60):
        yield(part_size*i, min(part_size*(i+1)-1, size-1), i)


# taken from github.com/mumrah/s3-multipart/blob/master/s3-mp-download.py
def s3_download(bucketname, keyname, dest_path, quiet=False, parallel_processes=multiprocessing.cpu_count(), headers={}, guess_mimetype=True):
    conn = boto.connect_s3()
    split = 50
    max_tries = 3
    try:
        bucket = conn.get_bucket(bucketname)
    except Exception, exc:
        print "Fatal Error: source bucket %s not found, exception %s"%(bucketname, exc)
        sys.exit()
    bytes_per_chunk = split * 1024 * 1024
    key = bucket.get_key(keyname)
    if not key:
        print "Fatal Error in download: key %s not found in bucket %s"%(keyname, bucketname)
        sys.exit()
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    if '/' in dest_path and not os.path.isdir(os.path.dirname(dest_path)):
        print "Fatal Error: destination path does not exist - args %s %s %s"%(bucketname, keyname, dest_path)
        sys.exit()
    if os.path.isdir(dest_path):
        print "Fatal Error: please supply file name in directory %s to store the file"%(dest_path)
        sys.exit()
        
    source_size = key.size
    size_mb = source_size / 1024 / 1024
    t1 = time.time()
    num_parts = (size_mb+(-size_mb%split))//split
    while num_parts > 200:
        split += 5
        num_parts = (size_mb+(-size_mb%split))//split
    __builtin__.global_download_total = num_parts
    __builtin__.global_download_progress = 0
    if not quiet:
        print "downloading %s (%s) in %s chunks"%(key.size, size_mb, num_parts)
    if source_size <= bytes_per_chunk:
        try:
            key.get_contents_to_filename(dest_path)
        except Exception, exc:
            print "problem downloading key %s - exception %s"%(keyname, exc)
    else:
        resp = conn.make_request("HEAD", bucket=bucket, key=key)
        if resp is None:
            raise ValueError("s3 response is invalid for bucket %s key %s"%(bucket, key))
        fd = os.open(dest_path, os.O_CREAT)
        os.close(fd)

        def arg_iterator(num_parts):
            for min_byte, max_byte, part in _gen_byte_ranges(source_size, num_parts):
                yield(conn, bucket.name, key.name, dest_path, split, min_byte, max_byte, max_tries, 0, parallel_processes, quiet)
        pool = multiprocessing.Pool(processes = parallel_processes)
        p = pool.map_async(_download_part, arg_iterator(num_parts))
        pool.close()
        pool.join()
        p.get(9999999)
        if not quiet:
            print
    t2 = time.time()-t1
    source = "S3:%s:/%s"%(bucketname, keyname)
    destination = "local:%s:%s"%(socket.gethostname(), dest_path)
    size = os.stat(dest_path).st_size
    total_time = t2
    if size == source_size:
        passed_size_check = True
    else:
        passed_size_check = False
    s3_log_append("s3_download", source, destination, size, start_time, total_time, passed_size_check, logfile="./itmi_s3lib.log")

    if not quiet:
        print "Finished downloading %0.2fM in %0.2fs (%0.2fmbps)"%(size_mb, t2, 8*size_mb/t2)



def s3_download_partial(bucketname, keyname, dest_path, quiet=False, parallel_processes=multiprocessing.cpu_count(), headers={}, guess_mimetype=True, min=None, max=None):
    conn = boto.connect_s3()
    split = 50
    max_tries = 3
    try:
        bucket = conn.get_bucket(bucketname)
    except Exception, exc:
        print "Fatal Error: source bucket %s not found, exception %s"%(bucketname, exc)
        sys.exit()
    bytes_per_chunk = split * 1024 * 1024
    key = bucket.get_key(keyname)
    if not key:
        print "Fatal Error in download: key %s not found in bucket %s"%(keyname, bucketname)
        sys.exit()
    start_time = "%s:%s"%(time.strftime("%d/%m/%Y"), time.strftime("%H:%M:%S"))
    if '/' in dest_path and not os.path.isdir(os.path.dirname(dest_path)):
        print "Fatal Error: destination path does not exist - args %s %s %s"%(bucketname, keyname, dest_path)
        sys.exit()
    if os.path.isdir(dest_path):
        print "Fatal Error: please supply file name in directory %s to store the file"%(dest_path)
        sys.exit()
        
    max = int(max)
    min = int(min)
    source_size = max-min
    size_mb = source_size / 1024 / 1024
    t1 = time.time()
    num_parts = (size_mb+(-size_mb%split))//split
    while num_parts > 200:
        split += 5
        num_parts = (size_mb+(-size_mb%split))//split
    __builtin__.global_download_total = num_parts
    __builtin__.global_download_progress = 0
    if not quiet:
        print "downloading %s (%s) in %s chunks"%(key.size, size_mb, num_parts)

    resp = conn.make_request("HEAD", bucket=bucket, key=key)
    if resp is None:
        raise ValueError("s3 response is invalid for bucket %s key %s"%(bucket, key))
    fd = os.open(dest_path, os.O_CREAT)
    os.close(fd)
    print source_size, num_parts

    def arg_iterator(num_parts):
        for min_byte, max_byte, part in _gen_byte_ranges(source_size, num_parts):
            if max_byte > max:
                break
            min_byte += min
            max_byte += max
            yield(conn, bucket.name, key.name, dest_path, split, min_byte, max_byte, max_tries, 0, parallel_processes, quiet)
    pool = multiprocessing.Pool(processes = parallel_processes)
    p = pool.map_async(_download_part, arg_iterator(num_parts))
    pool.close()
    pool.join()
    p.get(9999999)
    if not quiet:
        print

    t2 = time.time()-t1
    source = "S3:%s:/%s"%(bucketname, keyname)
    destination = "local:%s:%s"%(socket.gethostname(), dest_path)
    size = os.stat(dest_path).st_size
    total_time = t2
    passed_size_check=True
    #if size == source_size:
    #    passed_size_check = True
    #else:
    #    passed_size_check = False
    s3_log_append("s3_download", source, destination, size, start_time, total_time, passed_size_check, logfile="./itmi_s3lib.log")

    if not quiet:
        print "Finished downloading %0.2fM in %0.2fs (%0.2fmbps)"%(size_mb, t2, 8*size_mb/t2)


def s3_log_append(type, source, destination, size, start_time, total_time, passed_size_check, logfile="./transferlib.log"):
    f = None
    if os.path.isfile(logfile):
        f = open(logfile, 'a')
    else:
        f = open(logfile, 'w')
    h_size = 0
    if size > 1024*1024*1024:
        h_size = "%0.2fGb"%(size/(0.0+1024*1024*1024))
    elif size > 1024*1024:
        h_size = "%0.2fMb"%(size/(0.0+1024*1024))
    elif size > 1024:
        h_size = "%0.2fkb"%(size/(0.0+1024))
    else:
        h_size = "%0.2f"%(size)
    outlines = []
    outlines.append("%s\n"%(type))
    outlines.append("\tsource: %s\n"%(source))
    outlines.append("\tdestination: %s\n"%(destination))
    outlines.append("\tbytes transferred: %s (%s)\n"%(size, h_size))
    outlines.append("\tstart time: %s\n"%(start_time))
    outlines.append("\ttotal time: %0.3f\n"%(total_time))
    outlines.append("\tmbps: %0.2f\n"%(((size*8)/total_time)/1024/1024))
    outlines.append("\tpassed size check: %s\n"%(passed_size_check))
    f.writelines(outlines)
    f.close()
    



def compare_md5_local_to_s3(local_file_name, original_bucket, original_loc):
    if not os.path.isfile(local_file_name):
        print "comparing s3 and local md5s. file %s could not be found, exiting"%(local_file_name)

    (fd, fname) = tempfile.mkstemp()
    os.system("md5sum %s > %s"%(local_file_name, fname))
    f = os.fdopen(fd, 'r')
    checksum = None

    for line in f:
        tokens = line.rstrip('\r\n').split()
        checksum = tokens[0]
        break

    conn = boto.connect_s3()
    bucket = conn.get_bucket(original_bucket)
    key = bucket.get_key(original_loc)
    remote_md5 = None
    if not key:
        print "comparing s3 and local md5s. key %s not found in bucket %s"%(original_loc, original_bucket)
    else:
        try:
            remote_md5 = key.etag[1:-1]
        except TypeError:
            print "no md5 found for file %s bucket %s"%(original_loc, original_bucket)
    f.close()
    os.remove(fname)
    if checksum != remote_md5:
        print "fail in local md5 compare. local %s, s3 %s"%(checksum, remote_md5) 
        return False
    else:
        return True

def compare_md5_s3_to_s3(dest_bucket, dest_key, orig_bucket, orig_key):
    conn = boto.connect_s3()
    dbucket = conn.get_bucket(dest_bucket)
    dkey = dbucket.get_key(dest_key)
    if not dkey:
        print "comparing s3 md5s. key %s not found in bucket %s"%(dest_key, dest_bucket)
        return False

    obucket = conn.get_bucket(orig_bucket)
    okey = obucket.get_key(orig_key)
    if not okey:
        print "comparing s3 md5s. key %s not found in bucket %s"%(orig_key, orig_bucket)
        return False

    try:
        dmd5 = dkey.etag[1:-1]
    except TypeError:
        print "comparing s3 md5s. no md5 found for file %s bucket %s"%(dest_key, dest_bucket)
        return False

    try:
        omd5 = okey.etag[1:-1]
    except TypeError:
        print "comparing s3 md5s. no md5 found for file %s bucket %s"%(orig_key, orig_bucket)
        return False

    if dmd5 != omd5:
        return False
    else:
        return True
    


def fetch_vendor_md5(vendor_bucket, batch, sample):
    print "fetch_vendor_md5 not yet implemented"
    return None

