from WMCore.CherryPyThread.Tasks.SequentialTaskBase import SequentialTaskBase

class DataCollectTask(SequentialTaskBase):

    def initialize(self, getFunc, dataFormatFunc, putFunc):
        """
        getFunc: function reference which returns the data and take no args
        dataFormatFunc: function reference which returns the data (takes one args - data to be formated)
        putFunc: function refrence which returns the data (takes one args - data to put)
        """
        self.data = None
        self.getFunc = getFunc
        self.dataFormatFunc = dataFormatFunc
        self.putFunc = putFunc

    def setCallSequence(self):
        """
        sets the list of func which needs to be called sequencially
        """
        self._callSequence = [self.getData, self.formatData, self.putData]


    def getData(self):
        print "getting data ..."
        self.data = self.getFunc()
        print "getting data %s" % len(self.data)


    def formatData(self):
        self.data = self.dataFormatFunc(self.data)
        print "formmatting %s" % len(self.data)

    def putData(self):
        self.putFunc(self.data)
        print "putting %s" % len(self.data)
