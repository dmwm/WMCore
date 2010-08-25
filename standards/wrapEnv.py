#!/usr/bin/env python

""" wrapEnv.py [python version] [database schema] [command line]
        handles munging PATH, PYTHONPATH, DATABASE variables for tests
        read configuration from standards/buildslave.py .. there is an 
        example file at buildslave.py.sample"""
    
import sys
import os
if (len(sys.argv) < 3):
    print "Usage: %s <python version> <database type> <command> [arg1] [arg2] .." %\
                sys.argv[0]
    sys.exit(1)

requestedPython = sys.argv[1]
requestedDB     = sys.argv[2]

# the buildconfig file will end up being in the buildslave's main path


sys.path.append( os.path.realpath(os.path.join(os.getcwd(), "..",".." )) )
import buildslave

if ( not (requestedPython in buildslave.conf) ):
    print "Requested python version isn't in the slave configuration"
    sys.exit(1)

if 'PYTHONPATH' in buildslave.conf[requestedPython]:
    os.environ['PYTHONPATH'] = "%s:%s" % (buildslave.conf[requestedPython]['PYTHONPATH'], os.environ['PYTHONPATH'])

if 'PATH' in buildslave.conf[requestedPython]:
    os.environ['PATH'] = "%s:%s" % (buildslave.conf[requestedPython]['PATH'], os.environ['PATH'])

if 'PYTHONHOME' in buildslave.conf[requestedPython]:
    os.environ['PYTHONHOME'] = buildslave.conf[requestedPython]['PYTHONHOME']

if ( not (requestedDB in buildslave.conf) ):
    if ( requestedDB == 'sqlite'):
        # give a default for sqlite, since it doesnt matter
        os.environ['DATABASE'] = 'sqlite:///temp.db'
    else:
        print "Requested database type isn't in the slave configuration"
        sys.exit(1)
else:
    os.environ['DATABASE'] = buildslave.conf[requestedDB]

os.environ['WMCOREBASE'] = os.path.normpath(os.path.join(os.getcwd(), ".."))

commandLine = sys.argv[3:]

# actually run
print "Commandline: %s" % commandLine
print "PATH: %s" % os.environ['PATH']
if 'PYTHONHOME' in os.environ:
    print "PYTHONHOME: %s" % os.environ['PYTHONHOME']
if 'PYTHONPATH' in os.environ:
    print "PYTHONPATH: %s" % os.environ['PYTHONPATH']
sys.stdout.flush()
os.execvp( commandLine[0], commandLine[0:] )

        

    
    
