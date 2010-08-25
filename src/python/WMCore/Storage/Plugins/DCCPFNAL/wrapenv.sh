#!/bin/sh

# this script simply sources FNAL's environment and runs the command
# passed to it

# lets us do 'sensible' things for shell commands instead of the monstrocity
# there was with passing in /bin/sh scripts to be eval'd

. /opt/d-cache/dcap/bin/setenv-cmsprod.sh
exec "$@"
