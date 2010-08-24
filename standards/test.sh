#!/bin/sh

cd $WMCOREBASE/test/python/WMCore/JobSplitting
#python FileBased_unit.py
cd ../WMBS
#python files_DAOFactory_unit.py
cd ..
#python WMException_t.py
cd MsgService_t
#python MsgService_t.py
cd ../ThreadPool_t
#python ThreadPool_t.py
cd ../Agent_t
#python Harness_t.py
#python Configuration_t.py
cd ../Trigger_t
python Trigger_t.py
cd ../Database_t
#python DBFormatter_t.py
