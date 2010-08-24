#!/bin/sh


#
# Cleanup ~ and pyc files for lazy people
# Dave Evans 26th Jun 05

function CleanDir () {
    echo "Cleaning Dir $1"
    THISDIR=`pwd`
    cd $1
    if [ -e ./cleanup.sh ] 
    then
	echo "Using Cleanup script $1/cleanup.sh"
	./cleanup.sh
    else
        rm -rf *.py~
        rm -rf *.pyc
    fi
    cd $THISDIR
}


# Clean this dir
rm -rf *.py~
rm -rf *.pyc
rm -rf *.sh~

CLEAN_DIRS=`/bin/ls $PWD`
for var in $CLEAN_DIRS
  do
    if [ -d $var ]
    then
	CleanDir $var
    fi
  done




