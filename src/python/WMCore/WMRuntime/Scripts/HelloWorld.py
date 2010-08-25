#!/usr/bin/env python


from WMCore.WMRuntime.ScriptInterface import ScriptInterface

class HelloWorld(ScriptInterface):


    def __call__(self):

        print "Hello World"
        return 0
