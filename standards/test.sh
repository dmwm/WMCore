#!/bin/sh
source setup_mysql.sh
./cleanup.sh
./cleanup_mysql.sh

# testing with mysql parameters:

cd $WMCOREBASE/test/python/WMCore/JobSplitting_t
##python FileBased_unit.py
#$WMCOREBASE/standards/./cleanup_mysql.sh

cd ../WMBS
#python files_DAOFactory_unit.py
#$WMCOREBASE/standards/./cleanup_mysql.sh

cd ..
python WMException_t.py

cd MsgService_t
python MsgService_t.py
$WMCOREBASE/standards/./cleanup_mysql.sh

cd ../ThreadPool_t
python ThreadPool_t.py
$WMCOREBASE/standards/./cleanup_mysql.sh

cd ../Agent_t
python Harness_t.py
$WMCOREBASE/standards/./cleanup_mysql.sh
python Configuration_t.py

cd ../Trigger_t
python Trigger_t.py
$WMCOREBASE/standards/./cleanup_mysql.sh

cd ../Database_t
python DBFormatter_t.py
$WMCOREBASE/standards/./cleanup_mysql.sh

cd ../../WMComponent_t/ErrorHandler_t
python ErrorHandler_t.py
$WMCOREBASE/standards/./cleanup_mysql.sh

cd ../../WMComponent_t/Proxy_t
source $WMCOREBASE/standards/setup_mysql_proxy.sh
$WMCOREBASE/standards/./cleanup_mysql_proxy.sh
python Proxy_t.py
$WMCOREBASE/standards/./cleanup_mysql.sh
$WMCOREBASE/standards/./cleanup_mysql_proxy.sh

# testing with oracle parameters:

source $WMCOREBASE/standards/setup_oracle.sh
cd $WMCOREBASE/test/python/WMCore/Trigger_t
python Trigger_t.py
