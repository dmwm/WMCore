
# harness class that encapsulates the basic component logic.
from WMCore.Agent.Harness import Harness
# we do not import failure handlers as they are dynamicly 
# loaded from the config file.
from WMCore.WMFactory import WMFactory
import os
from WMCore.Configuration import loadConfigurationFile
from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory
from WMCore.HTTPFrontEnd import Downloader
from WMCore.WebTools.Root import Root

factory = WMFactory('generic')


class PhEDExInjector(Harness):
    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
        #self.start()


    def preInitialization(self):
        """
        Initializes plugins for different messages
        """
        self.messages['PhEDExInjectorNewInjection'] = \
            factory.loadObject('WMComponent.PhEDExInjector.NewInjectionHandler', self) 

    def inject(self, event, payload):
        print "event is %s, payload is %s " % (event, payload)
        