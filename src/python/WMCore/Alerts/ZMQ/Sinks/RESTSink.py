"""
RESTSink - REST client to forward alerts to REST server.

Currently, the only REST server option is in fact CouchDB server.
This means that CouchSink and this RESTSink do the same job.
Should the necessity of a generic REST server arise in the future,
this RESTSink class shall be able to handle both such server
as well as CouchDB: then, alerts buffering, bulk posts, etc will
need to be implemented here taking a few necessary features
implemented along the chain
CMSCouch->CouchRequests->JSONRequests->Requests
rather than using only CMSCouch class directly.

"""


from WMCore.Database.CMSCouch import Database



class RESTSink(object):
    """
    Alert sink for posting alerts to a REST server.
    The class acts as a REST client.
    
    """     
    def __init__(self, config):
        # configuration values:
        #     'uri' attribute (URL of the REST server and resource name)
        #         in case of CouchDB, the resource name is the database name
        #         http://servername:port/databaseName
        #     'bufferSize' - size of the queue until the REST call is performed
        self.config = config
        
        # the class currently relies only on 1 REST server possibility - the
        # CouchDB server. as explained above, .database will be replaced by
        # .connection if both a generic REST server as well as CouchDB are to
        # be talked to
        split = self.config.uri.rfind('/')
        dbName = self.config.uri[split + 1:] # get last item of URI - database name
        url = self.config.uri[:split]
        self._database = Database(dbName, url, size = self.config.bufferSize)
        
        
    def send(self, alerts):
        """
        Send a list of alerts to a REST server.
        
        """
        for alert in alerts:
            self._database.queue(alert)
            
        # two options here: either to call commit on the couch myself
        # or leave the alerts buffered in the Database queue which means
        # the .commit() would be called automatically if size is exceeded
        # 1st option:
        retVal = self._database.commit()
        return retVal