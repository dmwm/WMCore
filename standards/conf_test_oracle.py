#!/usr/bin/env python

from WMQuality.Test import Test

        
from WMCore_t.Agent_t.Daemon_t.Daemon_t import DaemonTest
from WMComponent_t.ErrorHandler_t.ErrorHandler_t import ErrorHandlerTest
from WMCore_t.JobSplitting_t.EventBased_t import EventBasedTest
from WMCore_t.DataStructs_t.Job_t import JobTest
from WMCore_t.Database_t.DBFactory_t import DBFactoryTest
from WMCore_t.ThreadPool_t.ThreadPool_t import ThreadPoolTest
from WMCore_t.DataStructs_t.Run_t import RunTest
from WMCore_t.DataStructs_t.WMObject_t import WMObjectTest
from WMCore_t.FwkJobReport_t.FJR_t import FJRTest
from WMCore_t.FwkJobReport_t.FileInfo_t import FileInfoTest
from WMCore_t.Database_t.Transaction_t import TransactionTest
from WMCore_t.WMException_t import WMExceptionTest
from WMCore_t.MsgService_t.MsgService_t import MsgServiceTest
from WMCore_t.Agent_t.Configuration_t import ConfigurationTest
from WMCore_t.WMFactory_t.WMFactory_t import WMFactoryTest
from WMCore_t.WMBS_t.DBPerformance_t import DBPerformanceTest
from WMCore_t.Database_t.DBFormatter_t import DBFormatterTest
from WMCore_t.Agent_t.Harness_t import HarnessTest
from WMComponent_t.Proxy_t.Proxy_t import ProxyTest
from WMCore_t.DataStructs_t.Fileset_t import FilesetTest
from WMComponent_t.DBSUpload_t.DBSUpload_t import DBSUploadTest
from WMCore_t.Trigger_t.Trigger_t import TriggerTest
from WMCore_t.Alerts_t.Alerts_t import AlertsTest
from WMCore_t.DataStructs_t.Subscription_t import SubscriptionTest
from WMCore_t.DataStructs_t.JobGroup_t import JobGroupTest
from WMCore_t.Database_t.DBCore_t import DBCoreTest
from WMCore_t.SiteScreening_t.BlackWhiteListParser_t import BlackWhiteListParserTest
from WMCore_t.WMBS_t.JobGroup_t import JobGroupTest
from WMComponent_t.DBSBuffer_t.DBSBuffer_t import DBSBufferTest

errors = {}
tests = []


try:
   x=BlackWhiteListParserTest()
   tests.append((x,"ewv"))
except Exception,ex:
   if not errors.has_key("ewv"):
       errors["ewv"] = []
   errors["ewv"].append(("BlackWhiteListParserTest",str(ex)))

try:
   x=ThreadPoolTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("ThreadPoolTest",str(ex)))

try:
   x=DBSUploadTest()
   #tests.append((x,"afaq"))
except Exception,ex:
   if not errors.has_key("afaq"):
       errors["afaq"] = []
   errors["afaq"].append(("DBSUploadTest",str(ex)))

try:
   x=RunTest()
   #tests.append((x,"metson"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("RunTest",str(ex)))

try:
   x=TriggerTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("TriggerTest",str(ex)))

try:
   x=WMExceptionTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("WMExceptionTest",str(ex)))

try:
   x=ConfigurationTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("ConfigurationTest",str(ex)))

try:
   x=JobTest()
   #tests.append((x,"jcgon"))
except Exception,ex:
   if not errors.has_key("jcgon"):
       errors["jcgon"] = []
   errors["jcgon"].append(("JobTest",str(ex)))

try:
   x=JobGroupTest()
   #tests.append((x,"metson"))
except Exception,ex:
   if not errors.has_key("metson"):
       errors["metson"] = []
   errors["metson"].append(("JobGroupTest",str(ex)))

try:
   x=WMObjectTest()
   #tests.append((x,"jcgon"))
except Exception,ex:
   if not errors.has_key("jcgon"):
       errors["jcgon"] = []
   errors["jcgon"].append(("WMObjectTest",str(ex)))

try:
   x=DBFormatterTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("DBFormatterTest",str(ex)))

try:
   x=TransactionTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("TransactionTest",str(ex)))

try:
   x=HarnessTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("HarnessTest",str(ex)))

try:
   x=DBCoreTest()
   #tests.append((x,"metson"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("DBCoreTest",str(ex)))

try:
   x=ErrorHandlerTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("ErrorHandlerTest",str(ex)))

try:
   x=WMFactoryTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("WMFactoryTest",str(ex)))

try:
   x=DBPerformanceTest()
   #tests.append((x,"jcgon"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("DBPerformanceTest",str(ex)))

try:
   x=MsgServiceTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("MsgServiceTest",str(ex)))

try:
   x=FileInfoTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("FileInfoTest",str(ex)))

try:
   x=FilesetTest()
   #tests.append((x,"jcgon"))
except Exception,ex:
   if not errors.has_key("jcgon"):
       errors["jcgon"] = []
   errors["jcgon"].append(("FilesetTest",str(ex)))

try:
   x=EventBasedTest()
   #tests.append((x,"metson"))
except Exception,ex:
   if not errors.has_key("metson"):
       errors["metson"] = []
   errors["metson"].append(("EventBasedTest",str(ex)))

try:
   x=DBSBufferTest()
   #tests.append((x,"afaq"))
except Exception,ex:
   if not errors.has_key("afaq"):
       errors["afaq"] = []
   errors["afaq"].append(("DBSBufferTest",str(ex)))

try:
   x=ProxyTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("ProxyTest",str(ex)))

try:
   x=DaemonTest()
   #tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("DaemonTest",str(ex)))

try:
   x=FJRTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("FJRTest",str(ex)))

try:
   x=AlertsTest()
   #tests.append((x,"sfoulkes"))
except Exception,ex:
   if not errors.has_key("sfoulkes"):
       errors["sfoulkes"] = []
   errors["sfoulkes"].append(("AlertsTest",str(ex)))

try:
   x=DBFactoryTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("DBFactoryTest",str(ex)))

try:
   x=SubscriptionTest()
   #tests.append((x,"jcgon"))
except Exception,ex:
   if not errors.has_key("jcgon"):
       errors["jcgon"] = []
   errors["jcgon"].append(("SubscriptionTest",str(ex)))

raw_input('Writing level 2 failures to file: failures2.log (press key to continue)')
failures = open('failures2_oracle.rep','w')

failures.writelines('Failed instantiation summary (level 2): \n')
for author in errors.keys():
    failures.writelines('\n*****Author: '+author+'********\n')
    for errorInstance, errorMsg in  errors[author]:
        failures.writelines('Test: '+errorInstance)
        failures.writelines(errorMsg)
        failures.writelines('\n\n')
failures.close()



test = Test(tests,'failures3_oracle.rep')
test.run()
test.summaryText()
        
