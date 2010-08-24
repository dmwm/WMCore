
from WMQuality.Test import Test

from WMCore_t.Agent_t.Configuration_t import ConfigurationTest
from WMCore_t.Trigger_t.Trigger_t import TriggerTest
from WMCore_t.WMException_t import WMExceptionTest
from WMCore_t.Database_t.DBFormatter_t import DBFormatterTest

# FIXME: this is some simple formatting. 
# This is the order for the tests.
tests = [(WMExceptionTest(),   'fvlingen'),\
         (ConfigurationTest(), 'fvlingen'),\
         (TriggerTest(),       'fvlingen')]

test = Test(tests)
test.run()
test.summaryText()

