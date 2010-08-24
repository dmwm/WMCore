#!/usr/bin/env python

from WMQuality.Test import Test

        
from WMCore_t.Database_t.DBFactory_t import DBFactoryTest
from WMCore_t.WorkerThreads_t.WorkerThreads_t import WorkerThreadsTest
from WMCore_t.FwkJobReport_t.FJR_t import FJRTest
from WMCore_t.FwkJobReport_t.FileInfo_t import FileInfoTest
from WMCore_t.Agent_t.Configuration_t import ConfigurationTest
from WMCore_t.WMFactory_t.WMFactory_t import WMFactoryTest

errors = {}
tests = []

try:
   x=DBFactoryTest()
   tests.append((x,"metson"))
except Exception,ex:
   if not errors.has_key("metson"):
       errors["metson"] = []
   errors["metson"].append(("DBFactoryTest",str(ex)))

try:
   x=FJRTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("FJRTest",str(ex)))

try:
   x=FileInfoTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("FileInfoTest",str(ex)))


try:
   x=ConfigurationTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("ConfigurationTest",str(ex)))

try:
   x=WMFactoryTest()
   tests.append((x,"fvlingen"))
except Exception,ex:
   if not errors.has_key("fvlingen"):
       errors["fvlingen"] = []
   errors["fvlingen"].append(("WMFactoryTest",str(ex)))

try:
   x=WorkerThreadsTest()
   tests.append((x,"jacksonj"))
except Exception,ex:
   if not errors.has_key("jacksonj"):
       errors["jacksonj"] = []
   errors["jacksonje"].append(("WorkerThreadsTest",str(ex)))



print('Writing level 2 failures to file: failures_oracle_2.rep')
failures = open('failures_oracle_2.rep','w')

failures.writelines('Failed instantiation summary (level 2): \n')
for author in errors.keys():
    failures.writelines('\n*****Author: '+author+'********\n')
    for errorInstance, errorMsg in  errors[author]:
        failures.writelines('Test: '+errorInstance)
        failures.writelines(errorMsg)
        failures.writelines('\n\n')
failures.close()



test = Test(tests,'failures_oracle_3.rep')
test.run()
test.summaryText()
        
