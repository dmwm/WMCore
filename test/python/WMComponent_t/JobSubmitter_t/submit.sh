#!/bin/bash


SANDBOX=$1
INDEX=$2

ls

/usr/bin/env python Unpacker.py --sandbox=$SANDBOX --package=JobPackage.pkl --index=$INDEX

cd job
export PYTHONPATH=$PYTHONPATH:$PWD
/usr/bin/env python WMCore/WMRuntime/Startup.py
cp WMTaskSpace/Report.pkl ../

sleep 90

ls -l WMTaskSpace
ls -l WMTaskSpace/*

exit 0

