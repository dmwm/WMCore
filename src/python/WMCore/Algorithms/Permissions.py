'''
A bunch of functions that check the permissions on a file are what they should 
be, or more restrictive. 
'''
__revision__ = "$Id: Permissions.py,v 1.1 2009/11/27 02:10:45 metson Exp $"
__version__ = "$Revision: 1.1 $"

import os
import stat

def check_permissions(filehandle, permission, pass_stronger = False):
    info = os.stat(filehandle)
    filepermission = oct(info[stat.ST_MODE] & 0777)
    if pass_stronger:
        assert filepermission <= permission, "file's permissions are too weak"
    else:
        assert filepermission == permission, "file does not have the correct permissions"
            
def owner_readonly(file):
    check_permissions(file, oct(0400))
    
def owner_readwrite(file):
    check_permissions(file, oct(0600))

def owner_readwriteexec(file):
    check_permissions(file, oct(0700))