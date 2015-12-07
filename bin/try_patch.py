#!/usr/bin/env python2.6

# try_patch.py -- slams the patch against jenkins
#                   should eventually report back the status
#                   so this can be used in git bisect

import json, subprocess, time, re
import tarfile, os, os.path, sys
import hashlib

from optparse import OptionParser
parser = OptionParser()
parser.add_option("--job-name", dest="job_name",
                  help="Name of job to run", default = "WMCore-UnitTests-try")

parser.add_option("--test-path", dest="test_path",
                  help = "What subset of tests to run (i.e. test/python/WMCore_t/WMBS_t)", 
                  default = "test/python/")

parser.add_option("--reset-after-run", dest="reset_after_run",
                  help = "Tells the script to do a git reset --hard once the test is done ( needed for git bisect )", 
                  default = False, action = 'store_true')

parser.add_option("--upstream-repo", dest="upstream",
                  help="Name of upstream remote repository", default="upstream")

(options, args) = parser.parse_args()

# get the root path of the repository we're wanting to test
p = subprocess.Popen(['git', 'rev-parse', '--show-toplevel'], stdout=subprocess.PIPE)
repos_path = p.communicate()[0].strip() + "/"

# is there a way to do this that doesn't involve simply tarring the directory?

exit_code = 99
hash_code = "***UNKNOWN***"
try:
    patchName = "trypatch-%s.patch" % time.time()
#    p = subprocess.Popen("git fetch %s" % options.upstream)
#    p.communicate()
    p = subprocess.Popen("git show remotes/%s/master" % options.upstream,
                         stdout=subprocess.PIPE,
                         shell = True)
    output, _ = p.communicate()
    for line in output.split("\n"):
        if line.startswith('commit'):
            masterCommit = line.split(' ')[1]
    
    fh = open(patchName, 'w')
    fh.write("%s\n" % masterCommit)
    fh.flush()
    fh.close()
    p = subprocess.Popen("git diff --full-index --binary %s -- . >> %s" % (masterCommit,
                                                                     patchName),
                         shell = True)
    p.communicate()

    patch_size = os.path.getsize( patchName )
    
    request = { 'job-name' : options.job_name, 'length' : patch_size, 'test-path' : options.test_path }
    ssh_call = subprocess.Popen( [ 'ssh','-q','-q', 'dmwm.cern.ch', '/opt/roundabout/scripts/receive_diff.py' ],
                                 stdin=subprocess.PIPE )

    # write the header
    ssh_call.stdin.write( json.dumps( request ) + "\n" )

    # write the data
    patch_handle = open( patchName, 'rb' )
    bufsize = 1024
    m = hashlib.md5()
    while True:
        bytes_read = patch_handle.read( bufsize )
        ssh_call.stdin.write( bytes_read )
        m.update( bytes_read )
        if len(bytes_read) != bufsize:
            # got the end of the file
            break
    hash_code = m.hexdigest()

    patch_handle.close()

    ssh_call.stdin.flush()
    ssh_call.stdin.close()

    ssh_call.communicate()

    exit_code = ssh_call.returncode

finally:
    if os.path.exists( patchName ):
        os.unlink( patchName )

if options.reset_after_run:
    p = subprocess.Popen(['git', 'reset', '--hard'], stdout=subprocess.PIPE)
    p.communicate()

print "Test done: Return code was %s" % exit_code
print "           Hash was %s" % hash_code
sys.exit( exit_code )
