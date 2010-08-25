#!/bin/bash
# first argument is the CMSSW installation
# second is a pickled file of arguments
# third argument is the output file
echo  runCmsDriver.sh $1 $2 $3
runCmsDriver.py $1 $2 $3

