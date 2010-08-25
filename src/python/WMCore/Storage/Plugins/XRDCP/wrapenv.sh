#!/bin/sh

# this script simply sources an environment and runs the command
# passed to it

# lets us do 'sensible' things for shell commands instead of the monstrocity
# there was with passing in /bin/sh scripts to be eval'd

. /afs/cern.ch/user/c/cmsprod/scratch1/releases/CMSSW_1_8_0_pre0/src/runtime.sh
exec "$@"
