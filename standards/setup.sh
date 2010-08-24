#!/bin/sh

# Edit the first variables to fit your environment
# Edit the init.sh file of the prodagent/prodcommon/wmcore in the cms package you download
# such that it does not include prodagent, prodcommon or wmcore lib in the pythonpath
# (we do want the other dependencies as we want to the cvs versions as a proper intstallation)
export STYLE=/home/fvlingen/programFiles/CMS_CVS/WMCORE/standards/.pylintrc
export CMSBASE=/home/fvlingen/programFiles/CMSBASE/PA_0_12_5/slc4_ia32_gcc345
export PRODAGENTBASE=$CMSBASE/cms/prodagent/PRODAGENT_0_12_5-cmp
export CVSBASE=/home/fvlingen/programFiles/CMS_CVS
export TESTDIR=/home/fvlingen/programFiles/CMS_CVS/WMCORE_TEST
export WMCOREBASE=$CVSBASE/WMCORE
export WTBASE=$WMCOREBASE/src
export YUIHOME=/put/your/value/here
export PATH=$WMCOREBASE/bin:$PATH
export PYTHONPATH=$WMCOREBASE/src/python:$PYTHONPATH
export PYTHONPATH=$WMCOREBASE/test/python:$PYTHONPATH
#export PYTHONPATH=$WMCOREBASE/test/python:$PYTHONPATH
echo "-->Sourcing CMS environment"
source $PRODAGENTBASE/etc/profile.d/init.sh
source setup_mysql.sh
source setup_mysql_proxy.sh
./start_mysql.sh
