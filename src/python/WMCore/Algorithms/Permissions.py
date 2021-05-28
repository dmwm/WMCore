'''
A bunch of functions that check the permissions on a file are what they should
be, or more restrictive.
'''



from builtins import oct
import os
import stat

def check_permissions(filehandle, permission, pass_stronger = False):
    info = os.stat(filehandle)
    filepermission = oct(info[stat.ST_MODE] & 0o777)
    if pass_stronger:
        assert filepermission <= permission, "file's permissions are too weak"
    else:
        assert filepermission == permission, "file does not have the correct permissions"

def owner_readonly(file_):
    check_permissions(file_, oct(0o400))

def owner_readwrite(file_):
    check_permissions(file_, oct(0o600))

def owner_readwriteexec(file_):
    check_permissions(file_, oct(0o700))
