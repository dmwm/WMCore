#!/bin/sh

cd $WMCOREBASE
echo "-->remove log files"
find -name "*.log"|xargs rm
echo "-->remove pyc files"
find -name "*.pyc"|xargs rm
echo "-->removing code quality files"
find -name "quality*.txt"|xargs rm
cd $TESTDIR
echo "-->remove ComponentLog files"
find -name "ComponentLog"|xargs rm
