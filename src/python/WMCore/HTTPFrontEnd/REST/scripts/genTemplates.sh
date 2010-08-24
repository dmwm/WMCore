#!/bin/sh

if [ $# == 1 ]; then

cheetah compile --flat --odir Templates $1
rm -f $1.bak

else

rm -rf Templates/*.py*
find Templates -name "*.tmpl" | \
awk '{print "cheetah compile --flat --odir Templates "$1""}' | /bin/sh
find Templates -name "*.bak" -exec rm {} \;
cat > Templates/__init__.py << EOF
#!/usr/bin/env python

"""
Templates used by REST service
"""
__author__ = "Valentin Kuznetsov <vkuznet at gmail dot com>"
__revision__ = 1

EOF
#ls Templates/*.py | awk '{split($1,a,"."); split(a[1],b,"/"); print "import "b[1]""}' >> __init__.py
ls Templates/*.py | grep -v __init__ | awk '{split($1,a,"."); split(a[1],b,"/"); print "from "b[1]"."b[2]" import "b[2]""}' >> Templates/__init__.py

fi
