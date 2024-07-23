#!/usr/bin/env python
"""
Generate.py

A class that parses a workflow specification file
(or agent specification file) and generates the appropriate
stub classes.

"""
from __future__ import print_function





from builtins import str, object
from future.utils import viewvalues

import os
import sys
import pickle

from WMCore.Agent.Configuration import loadConfigurationFile


class Generate(object):
    """
    Generate

    A class that parses a workflow specification file
    (or agent specification file) and generates the appropriate
    stub classes.
    """


    def __init__(self, configFile):
        self.config = loadConfigurationFile(configFile)
        # current dir when writing stub files.
        self.currentDir = None

        # parsed component information from configuration file.
        self.components = {}
        # parsed synchronizers (trigger) information from configuration file.
        self.synchronizers = {}
        # default stub message that is put in every generated file.
        self.stubmsg = """
\"\"\"
Auto generated stub be careful with editing,
Inheritance is preferred.
\"\"\"


"""
    def generate(self):
        """
        Main method to be called, parses the file and generates the classes.
        """

        try:
            print('Creating output directory '+\
                str(self.config.General.srcDir))
            os.makedirs(self.config.General.srcDir)
        except:
            print('ERROR: Make sure directory does not exist already')
            sys.exit(0)
        self.parse()
        print('Starting generation')
        self.componentStubs()
        self.componentTests()
        print('Flow generated')

    def parse(self):
        """
        Parses the input file.
        """

        print('Parsing config file')
        # preprocess
        for synchronizer in self.config.General.synchronizers:
            synchro = pickle.loads(synchronizer)
            self.synchronizers[synchro['ID']] = synchro
            self.synchronizers[synchro['ID']]['components'] = []
        for handler in self.config.General.handlers:
            hndlr = pickle.loads(handler)
            if hndlr['component'] not in self.components:
                self.components[hndlr['component']] = {}
            self.components[hndlr['component']][hndlr['messageIn']] = hndlr
            if 'synchronize' in hndlr:
                self.synchronizers[hndlr['synchronize']]['components'].\
                    append(hndlr['component'])
        for plugin in self.config.General.plugins:
            plgn = pickle.loads(plugin)
            self.components[plgn['component']][plgn['handler']]['plugin'] = \
                 plgn


    def componentTests(self):
        msg = """
class %sTest(unittest.TestCase):

    _setup_done = False

    def setUp(self):

        if not %sTest._setup_done:
            self.testInit = TestInit(__file__)
            self.testInit.setLogging()
            self.testInit.setDatabaseConnection()
            %sTest._setup_done = True

    def testA(self):
        config = self.testInit.getConfiguration(\
            os.path.join(os.getenv('APPBASE'), \
            '%s/%s/DefaultConfig.py'))
        theComponent = %s(config)
        # make sure database settings are set properly.
        #theComponent.prepareToStart()
        print('I am a placeholder for a test')

    def runTest(self):

        #Run test.
        self.testA()

if __name__ == '__main__':
    unittest.main()
"""
        for componentName in self.components:
            print('Creating test dir for:'+componentName)
            self.currentDir = os.path.join(self.config.General.testDir, \
                componentName+'_t')
            os.makedirs(self.currentDir)
            print('Creating test stub')
            with open(os.path.join(self.currentDir,'__init__.py'), 'w') as stfile:
                stfile.write('#!/usr/bin/env python\n')

            with open(os.path.join(self.currentDir, componentName + '_t.py'), 'w') as stfile:
                stfile.write('#!/usr/bin/env python\n')
                stfile.write(self.stubmsg)
                stfile.write("# test file skeletons.\n\n\n")
                stfile.write('import os\n')
                stfile.write('import unittest\n\n\n')
                stfile.write("from "+self.config.General.pythonPrefix+'.'+componentName+'.'+componentName+' import '+componentName+'\n')
                stfile.write("from WMCore.Agent.Configuration import Configuration\n")
                stfile.write("from WMQuality.TestInit import TestInit\n")
                stfile.write(msg % (componentName, componentName, componentName,
                                    self.config.General.srcDir, componentName, componentName))

        self.currentDir = os.path.join(self.config.General.baseDir, 'standards')
        try:
            os.makedirs(self.currentDir)
        except:
            pass
        print('Creating test suite')
        with open(os.path.join(self.currentDir,'defaultTest.py'), 'w') as stfile:
            stfile.write("#!/usr/bin/env python\n")
            stfile.write("from WMQuality.Test import Test\n\n\n")
            for componentName in self.components:
                msg = "from "+self.config.General.pythonTestPrefix+"."+componentName+'_t.'+componentName+'_t import '+componentName+'Test'
                stfile.write(msg+'\n')
            stfile.write('\n\n\n')
            stfile.write('errors = {}\n')
            stfile.write('tests = []\n')

        msg1 = """

try:
   x=%sTest()
   tests.append((x,"nobody"))
except Exception,ex:
   if not errors.has_key("nobody"):
       errors["nobody"] = []
   errors["nobody"].append(("%sTest",str(ex)))

"""
        msg2 = """
print('Writing level 2 failures to file: failures2.log ')
failures = open('failures2.log','w')

failures.writelines('Failed instantiation summary (level 2): \\\n')
for author in errors.keys():
    failures.writelines('\\\n*****Author: '+author+'********\\\n')
    for errorInstance, errorMsg in  errors[author]:
        failures.writelines('Test: '+errorInstance)
        failures.writelines(errorMsg)
        failures.writelines('\\\n\\\n')
failures.close()

test = Test(tests,'failures3.log')
test.run()
test.summaryText()
"""
        for componentName in self.components:
            stfile.write(msg1 % (componentName, componentName))
            stfile.write(msg2)

    def componentStubs(self):
        """
        Generates the component stubs.
        """

        for componentName in self.components:
            print('Creating component dir for:'+componentName)
            self.currentDir = os.path.join(self.config.General.srcDir, \
                componentName)
            os.makedirs(self.currentDir)
            print('Creating component stub')
            with open(os.path.join(self.currentDir,'__init__.py'), 'w') as stfile:
                stfile.write('#!/usr/bin/env python\n')

            stfile = \
                open(os.path.join(self.currentDir, componentName + '.py'), 'w')
            stfile.write('#!/usr/bin/env python\n')
            stfile.write(self.stubmsg)
            stfile.write("# harness class that encapsulates the basic component logic.\n")
            stfile.write("from WMCore.Agent.Harness import Harness\n\n")
            importFact = False
            try:
                for handlerName in self.components[componentName]:
                    handler = self.components[componentName][handlerName]
                    if 'configurable' in handler and handler['configurable'] == 'yes':
                        if not importFact:
                            stfile.write("# we do not import handler " + self.convert(handlerName) + " as they are dynamicly\n")
                            stfile.write("# loaded from the config file.\n")
                            stfile.write("from WMCore.WMFactory import WMFactory\n")
                            importFact = True
                    else:
                        stfile.write('from ' + self.config.General.pythonPrefix + '.' )
                        stfile.write(componentName +'.Handler.' + self.convert(handlerName) +' import')
                        stfile.write(' ' + self.convert(handlerName)+'\n')
            except Exception as ex:
                print('No messages for component: '+ componentName)
            msg = """
class %s(Harness):


    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)


    def preInitialization(self):
    # mapping from message types to handlers
""" % (componentName)
            stfile.write(msg)

            if importFact:
                stfile.write('        # use a factory to dynamically load handlers.\n')
                stfile.write("        factory = WMFactory('generic')\n")
            try:
                for handlerName in self.components[componentName]:
                    handler = self.components[componentName][handlerName]
                    if 'configurable' in handler and \
                        handler['configurable'] == 'yes':
                        msg = """
        # in case nothing was configured we have a fallback.
        if not hasattr(self.config.%s, "%sHandler"):
            self.config.%s.%sHandler =\\
            '%s.%s.Handler.%s'
        self.messages['%s'] = \\
            factory.loadObject(\\
            self.config.%s.%sHandler, self)
""" % (componentName, self.convert(handlerName), componentName, self.convert(handlerName), \
      self.config.General.pythonPrefix, componentName, self.convert(handlerName), \
      handlerName, componentName, self.convert(handlerName))
                        stfile.write(msg)
                    else:
                        stfile.write("        self.messages['"+handlerName+"'] = "+ self.convert(handlerName)+"(self)\n\n")
            except Exception as ex:
                print('is this an ERROR? :'+str(ex))
            stfile.flush()
            stfile.close()
            print('Creating handler stubs')
            self.defaultConfig(componentName)
            self.handlerStubs(componentName)

    def defaultConfig(self, componentName):
        """
        Generates the default config file.
        """

        stfile = open(os.path.join(self.currentDir, 'DefaultConfig.py'), 'w')
        stfile.write('#!/usr/bin/env python\n')
        stfile.write(self.stubmsg)
        msg = """
from WMCore.Agent.Configuration import Configuration

config = Configuration()
config.component_("%s")
#The log level of the component.
config.%s.logLevel = "INFO"
""" % (componentName, componentName)
        stfile.write(msg)
        # check if we need a thread parameter
        threadParam = False
        for handler in viewvalues(self.components[componentName]):
            if 'threading' in handler and handler['threading'] == 'yes':
                threadParam = True
        if threadParam:
            stfile.write('# maximum number of threads we want to deal\n')
            stfile.write('# with messages per pool.\n')
            stfile.write('config.'+componentName+'.maxThreads = 30\n')
        for handlerName in self.components[componentName]:
            handler = self.components[componentName][handlerName]
            if 'configurable' in handler and handler['configurable'] == 'yes':
                stfile.write('config.'+componentName+'.'+self.convert(handlerName)+'Handler =\\\n')
                stfile.write('    "'+self.config.General.pythonPrefix+'.'+componentName+'.Handler.'+self.convert(handlerName)+'"\n')

        stfile.flush()
        stfile.close()


    def handlerStubs(self, componentName):
        """
        Generates the handler stub files.
        """

        handlerDir = os.path.join(self.currentDir, 'Handler')
        try:
            os.makedirs(handlerDir)
        except:
            pass
        with open(os.path.join(handlerDir, '__init__.py'), 'w') as stfile:
            stfile.write('#!/usr/bin/env python\n')
        for handlerName in self.components[componentName]:
            # check if we need to create a threaded version
            handler = self.components[componentName][handlerName]
            threaded = False
            if 'threading' in handler:
                if handler['threading'] == 'yes':
                    threaded = True

            stfile = open(os.path.join(handlerDir, self.convert(handlerName) +'.py'),'w')
            stfile.write('#!/usr/bin/env python\n')
            stfile.write(self.stubmsg)
            if not threaded:
                stfile.write('import threading\n\n')
            stfile.write('from WMCore.Agent.BaseHandler import BaseHandler\n')
            if threaded:
                stfile.write('from WMCore.ThreadPool.ThreadPool import ThreadPool\n')
            msg = """


class %s(BaseHandler):

    def __init__(self, component):
        BaseHandler.__init__(self, component)

""" % (self.convert(handlerName))
            stfile.write(msg)
            if threaded:
                msg = """
        self.threadpool = ThreadPool(\\
            "%s.%s.Handler.%sSlave",\\
            self.component, "%s",\\
            self.component.config.%s.maxThreads)

""" %(self.config.General.pythonPrefix, componentName, \
                    self.convert(handlerName), self.convert(handlerName), componentName)
                stfile.write(msg)

            msg = """
     # this we overload from the base handler
    def __call__(self, event, payload):
        \"\"\"
        Implement the handler here.
        Assign values to "messageToBePublished","yourTaskId"
        ,"yourPayloadString,"yourActionPayload"".
        Where necessary
        \"\"\"
"""
            stfile.write(msg)
            if not threaded:
                msg = """
        print("Implement me: %s.%s.Handler.%s")

        myThread = threading.currentThread()

""" % (self.config.General.pythonPrefix, \
                    componentName, self.convert(handlerName))
                stfile.write(msg)
                self.triggers(stfile, componentName, handlerName)
                self.messages(stfile, componentName, handlerName)
            else:
                stfile.write('        self.threadpool.enqueue(event, {"event" : event, "payload" : payload})\n')
                self.threadSlave(componentName, handlerName)

            stfile.flush()
            stfile.close()

    def threadSlave(self, componentName, handlerName):
        """
        Generates thread slave stubs.
        """

        slaveDir = os.path.join(self.currentDir, 'Handler')
        className = self.convert(handlerName) + 'Slave'
        with open(os.path.join(slaveDir, className + '.py'),'w') as stfile:
            stfile.write('#!/usr/bin/env python\n')
            stfile.write(self.stubmsg)
            stfile.write('import threading\n')
            stfile.write('#inherit from our default slave implementation\n')
            stfile.write('from WMCore.ThreadPool.ThreadSlave import ThreadSlave\n')
            stfile.write('\n')
            stfile.write('class '+className + '(ThreadSlave):\n')
            stfile.write('\n')
            stfile.write('    def __call__(self, parameters):\n')
            stfile.write('        """\n')
            stfile.write('        Implement the handler here.\n')
            stfile.write('        Assign values to "messageToBePublished","yourTaskId"\n')
            stfile.write('        ,"yourPayloadString,"yourActionPayload"".\n')
            stfile.write('        Where necessary\n')
            stfile.write('        """\n')
            stfile.write('\n')
            stfile.write('        print("Implement me:')
            stfile.write(self.config.General.pythonPrefix+'.'+componentName+'.Handler.'+self.convert(handlerName) + '")')
            stfile.write('\n\n')
            stfile.write('        myThread = threading.currentThread()\n\n')
            self.triggers(stfile, componentName, handlerName)
            self.messages(stfile, componentName, handlerName)
            stfile.write('\n\n')
            stfile.write('        # we need to do this in our slave otherwise the \n')
            stfile.write('        # messages that might have been published, will not be send.\n')
            stfile.write('        myThread.msgService.finish()\n')

    def messages(self, stfile, componentName, handlerName):
        """
        Generates message statements in handler, slave.
        """
        # check what messages need to be published and how (e.g. choice or no choice)
        messagesOut = self.components[componentName][handlerName]['messageOut']
        if messagesOut.find('|') > -1 and messagesOut != '':
            messagesOut = messagesOut.split('|')
            for messageOut in messagesOut:
                stfile.write("        if messageToBePublished == '")
                stfile.write(messageOut+ "':\n")
                msg = "            msg = {'name':'"+messageOut+"', 'payload': yourPayloadString}"
                stfile.write(msg+'\n')
                stfile.write("            myThread.msgService.publish(msg)\n")
        elif messagesOut != '':
            messagesOut = messagesOut.split(',')
            for messageOut in messagesOut:
                msg = "        msg = {'name':'"+messageOut+"', 'payload': yourPayloadString}"
                stfile.write(msg+'\n')
                stfile.write("        myThread.msgService.publish(msg)\n")

    def triggers(self, stfile, componentName, handlerName):
        """
        Generates trigger statements.
        """

        handler = self.components[componentName][handlerName]

        if 'createSynchronizer' in handler:
            stfile.write('        flags = []\n')
            for synchronizer in self.synchronizers[handler['createSynchronizer']]['components']:
                flag = "{'trigger_id' : '"+handler['createSynchronizer']+ "',\\\n" + \
                        "                'id' : yourTaskId,\\\n"+ \
                        "                'flag_id' : '"+synchronizer+"'}"
                stfile.write('        flag = '+flag+'\n')
                stfile.write('        flags.append(flag)\n')
            stfile.write('        myThread.trigger.addFlag(flags)\n')
            action = "{'trigger_id' : '"+ handler['createSynchronizer']+ "',\\\n" + \
                     "                  'id' : yourTaskId,\\\n"+ \
                     "                  'action_name' : '"+self.synchronizers[handler['createSynchronizer']]['action']+"',\\\n" +\
                     "                  'payload' : yourActionPayload}"
            stfile.write('        action = '+action+'\n')
            stfile.write('        myThread.trigger.setAction(action)\n')
        if 'synchronize' in self.components[componentName][handlerName]:
            flag = "{'trigger_id' : '"+handler['synchronize']+ "'," + \
                    "'id' : yourTaskId,"+ \
                    "'flag_id' : '"+componentName+"'}"
            stfile.write('        flag = '+flag+'\n')
            stfile.write('        myThread.trigger.addFlag(flags)\n')

    def convert(self, textStr):
        """
        filters out unwanted symbols excluded in python
        class names
        """
        no = [':','/']

        for symbol in no:
            textStr = textStr.replace(symbol, '_')
        return textStr
