
echo "Core lib:"
./quality.sh $WMCOREBASE/src/python/WMCore/WMException.py
mv codeQuality.txt qualityWMException.txt
./quality.sh $WMCOREBASE/src/python/WMCore/DataStructs/WMFactory.py
mv codeQuality.txt qualityWMFactory.txt
./quality.sh $WMCOREBASE/src/python/WMCore/MsgService/
mv codeQuality.txt qualityMsgService.txt
./quality.sh $WMCOREBASE/src/python/WMCore/Agent/
mv codeQuality.txt qualityAgent.txt

echo "Test core lib:"
./quality.sh $WMCOREBASE/test/python/WMCore/WMException_t.py
mv codeQuality.txt qualityWMException_t.txt
./quality.sh $WMCOREBASE/test/python/WMCore/MsgService_t/
mv codeQuality.txt qualityMsgService_t.txt
./quality.sh $WMCOREBASE/test/python/WMCore/Agent_t/
mv codeQuality.txt qualityAgent_t.txt

find -name "quality*.txt"|xargs grep 'Your code has been rated.*'

