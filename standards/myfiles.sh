
echo "Core lib:"
./quality.sh $WMCOREBASE/src/python/WMCore/WMException.py
mv codeQuality.txt qualityWMException.txt
#./quality.sh $WMCOREBASE/src/python/WMCore/DataStructs/
#mv codeQuality.txt qualityDataStructs.txt
./quality.sh $WMCOREBASE/src/python/WMCore/MsgService/
mv codeQuality.txt qualityMsgService.txt
./quality.sh $WMCOREBASE/src/python/WMCore/ThreadPool/
mv codeQuality.txt qualityThreadPool.txt
./quality.sh $WMCOREBASE/src/python/WMCore/Agent/
mv codeQuality.txt qualityAgent.txt
./quality.sh $WMCOREBASE/src/python/WMCore/WMFactory.py
mv codeQuality.txt qualityWMFactory.txt
./quality.sh $WMCOREBASE/src/python/WMCore/Trigger/
mv codeQuality.txt qualityTrigger.txt
./quality.sh $WMCOREBASE/src/python/WMComponent/ErrorHandler/
mv codeQuality.txt qualityErrorHandler.txt
#./quality.sh $WMCOREBASE/src/python/WMCore/Database/
#mv codeQuality.txt qualityDatabase.txt
#./quality.sh $WMCOREBASE/src/python/WMCore/JobSplitting
#mv codeQuality.txt qualityJobSplitting.txt
#./quality.sh $WMCOREBASE/src/python/WMCore/Services
#mv codeQuality.txt qualityServices.txt
#./quality.sh $WMCOREBASE/src/python/WMCore/WMBS        
#mv codeQuality.txt qualityWMBS.txt
#./quality.sh $WMCOREBASE/src/python/WMCore/JobFactory
#mv codeQuality.txt qualityJobFactory.txt
#./quality.sh $WMCOREBASE/src/python/WMCore/WMBSFeeder  
#mv codeQuality.txt qualityWMBSFeeder.txt

echo "Test core lib:"
./quality.sh $WMCOREBASE/test/python/WMCore/WMException_t.py
mv codeQuality.txt qualityWMException_t.txt
./quality.sh $WMCOREBASE/test/python/WMCore/MsgService_t/
mv codeQuality.txt qualityMsgService_t.txt
./quality.sh $WMCOREBASE/test/python/WMCore/ThreadPool_t/
mv codeQuality.txt qualityThreadPool_t.txt
./quality.sh $WMCOREBASE/test/python/WMCore/Agent_t/
mv codeQuality.txt qualityAgent_t.txt
./quality.sh $WMCOREBASE/test/python/WMCore/Trigger_t/
mv codeQuality.txt qualityTrigger_t.txt
./quality.sh $WMCOREBASE/test/python/WMComponent_t/ErrorHandler_t/
mv codeQuality.txt qualityErrorHandler_t.txt
#./quality.sh $WMCOREBASE/test/python/WMCore/Database_t/
#mv codeQuality.txt qualityDatabase_t.txt

find -name "quality*.txt"|xargs grep 'Your code has been rated.*'

