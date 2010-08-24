#!/usr/bin/env python

from WMQuality.Test import Test

        
from WMCore_t.Trigger_t.Trigger_t import TriggerTest


tests = [\
     (TriggerTest(),'fvlingen'),\
    ]

test = Test(tests)
test.run()
test.summaryText()
        
