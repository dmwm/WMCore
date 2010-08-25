#!/bin/sh

# Edit the first variables to fit your environment
# Edit the init.sh file and dependency file of the prodagent/prodcommon/wmcore 
# in the cms package you download such that it does not include prodagent, 
# prodcommon or wmcore lib in the pythonpath (we do want the other dependencies 
# as we want to test the cvs versions against a proper intstallation)

# code style used in project.
export STYLE=/home/fvlingen/programFiles/CMS_CVS/WMCORE/standards/.pylintrc
# directory used for tests.
export TESTDIR=/home/fvlingen/programFiles/CMS_CVS/WMCORE_TEST
# location of WMCORE
export WMCORE_ROOT=/home/fvlingen/programFiles/CMS_CVS/WMCORE
#TODO: replace to WMCORE_ROOT - remove this line - backward compatibility
export WMCOREBASE=$WMCORE_ROOT

# webtools base (only for webtools related files and modules)
#TODO: remove this as well - backward compatibility
export WTBASE=$WMCORE_ROOT/src
# yui home (only for webtools related files and modules)
export YUI_ROOT=/put/your/value/here
#TODO: remove this as well - backward compatibility
export YUIHOME=$YUI_ROOT

# path settings for incorporating wmcore binaries.

export PATH=$WMCORE_ROOT/bin:$PATH
# add wmcore library and tests to pythonpath.
export PYTHONPATH=$WMCORE_ROOT/src/python:$PYTHONPATH
export PYTHONPATH=$WMCORE_ROOT/test/python:$PYTHONPATH

#TODO: this should eventually defined with in WMCore envrioment. but for not get it from prodagent setting.
echo "-->Sourcing CMS environment"
#edit the profile.d/init.sh file such that you do not include
#the prodagent and prodcommon libraries of the installed package.
# location of prodagent package.
export PRODAGENTPACKAGEBASE=/home/fvlingen/programFiles/CMSBASE/PA_0_12_5/slc4_ia32_gcc345/cms/prodagent/PRODAGENT_0_12_5-cmp
source $PRODAGENTPACKAGEBASE/etc/profile.d/init.sh
# source mysql and oracle related scripts for backends.
source $WMCOREBASE/standards/setup_mysql.sh
$WMCOREBASE/standards/./start_mysql.sh
