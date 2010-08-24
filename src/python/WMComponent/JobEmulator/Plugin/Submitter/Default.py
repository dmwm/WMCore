
import threading

#FIXME: need to inherit from proper submitter interface.
class Default:


    def __init__(self):
        #FIXME: placeholder code
        self.toSubmit = {}
        self.specFiles = {}



    def doSubmit(self):
        """
        _doSubmit_

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """

        myThread = threading.currentThread()
        for jobId, cacheDir in self.toSubmit.items():
            # for the emulator, points to a jobspec file.
            payload = self.specFiles[jobId]
            msg = {'name':'EmulateJob', 'payload': payload}
            myThread.msgService.publish(msg)
