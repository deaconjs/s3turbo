# s3turbo
A Python command-line program for parallel, concurrent transfer of large files to, from, and between s3 buckets, ec2 instances, and local. s3turbo moved around 1.5 Pb of data in one project.


## Install

To use, first install boto and filechunkio.

`$ pip install boto, filechunkio`

Configure .boto credentials file as [here](boto.cloudhackers.com/en/latest/boto_config_tut.html), then clone the s3turbo repo.


## Usage

* single-line transfer:

  to download - `s3turbo.py s3://bucket_name/path/key_name local:///full_path/filename`

  to upload   - `s3turbo.py local:///full_path/filename s3://bucket_name/path/key_name`

  to copy     - `s3turbo.py s3://bucket1/path/key_name s3://bucket2/path/key_name`

* OR key-name file input:

  `s3turbo.py key_name_file`

* OR rsync functionality (end both args with slashes)

  `s3turbo.py (s3|local):path/ (s3|local):path/ [include include_string] [exclude exclude_string] [remove_prefix prefix]`

  e.g. `s3turbo.py local:///home/username/path s3://owner.run.etc/etc_dir/ include .py exclude .pyc remove_prefix /home/username`

The key_name_file format should follow the same conventions as single-line format, with one line per file to transfer. If an input file is used, the file list can contain a mixture of download, copy, and upload commands, in any order.

Files are by default not overwritten, so it is safe to restart multiple file transfer operations that were interrupted. Download functionality skips existing local files by the same name but only if they are the same size. The copy and upload functionalities do check file names, but do not yet check file sizes.

* Optional dryrun flag

  `s3turbo.py args [dryrun] [args]`

* Optional reduced_redndancy flag

  `s3turbo.py args [reduced_redundancy] [args]`


The dry_run flag prints out the files to be transferred, without transferring any. Output is standard input format. The reduced_redundancy flag uses that class of AWS storage. This saves some money but has slightly higher odds of data loss.

