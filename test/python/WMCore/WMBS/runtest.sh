export PYTHONPATH=$PYTHONPATH:../../../src/python/WMCore/

rm database.lite
yes | mysqladmin -u root drop wmbs; mysqladmin -u root create wmbs
rm *.log *.lite
time python test.py 
echo
time python filesettest.py
echo
echo Database/DBCore.py
pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/Database/DBCore.py | grep -A 2 "Global evaluation"
echo
echo WMBS/Factory.py
pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Factory.py | grep -A 2 "Global evaluation"
echo
echo WMBS/File.py
pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/File.py | grep -A 2 "Global evaluation"
echo
echo WMBS/Fileset.py
pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Fileset.py | grep -A 2 "Global evaluation"
echo
echo WMBS/Job.py
pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Job.py | grep -A 2 "Global evaluation"
echo
echo WMBS/MySQL.py
pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/MySQL.py | grep -A 2 "Global evaluation"
echo
echo WMBS/SQLite.py
pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/SQLite.py | grep -A 2 "Global evaluation"
echo
echo WMBS/Subscription.py
pylint --rcfile=../../../../standards/.pylintrc  ../../../../src/python/WMCore/WMBS/Subscription.py | grep -A 2 "Global evaluation"
echo
echo WMBS/Workflow.py
pylint --rcfile=../../../../standards/.pylintrc ../../../../src/python/WMCore/WMBS/Workflow.py | grep -A 2 "Global evaluation"
