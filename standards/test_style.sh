#!/bin/sh

echo 'output goes to file: codeQuality.txt'
echo 'removing old codeQuality.txt'
rm codeQuality.txt
if [ $# -lt 1 ];then
  echo "Usage: "
  echo "   `basename $0` <filename> "
  echo "   `basename $0` <dir>"
  echo "for example: "
  echo "  `basename $0` /my/own/path/thefile.py"
  echo "  `basename $0` /my/own/path/"
  exit 1
fi
echo "starting scan for $1"
echo "using config file: $STYLE"

exec find $1 -name "*.py" |xargs pylint --rcfile=$STYLE > codeQuality.txt


