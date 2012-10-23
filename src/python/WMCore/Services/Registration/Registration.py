#!/usr/bin/env python
'''
A very simple interface to the Registration service. All we need to do here is
send a JSON encoded dictionary periodically (hourly) to the service. To achieve
this do something like:

reg_info ={
    "url": "https://globaldbs",
    "admin": "joe.bloggs@cern.ch",
    "type": "DBS",
    "name": "Global DBS",
    "timeout": 2
}

reg = Registration({"inputdata": reg_info})

this will create a Registration object with all the relevant registration
information (_id, admin, type, name, and timeout are the minimal set, you could
add more such as description, configuration files etc should your app need it).

Once instatiated you will then want to poll it hourly like so:

reg.refreshCache()

This will push the configuration up to the Registration service
'''
from WMCore.Database.CMSCouch import CouchServer

class Registration():
    def __init__(self, cfg_dict = {}, reg_info = {}):
        """
        Initialise the regsvc for this component,
        """
        try:
            config_dict = {
                            'server': 'https://cmsweb.cern.ch/',
                            'database': 'registration',
                            'cacheduration': 1,
                           }

            config_dict.update(cfg_dict)

            self.server = CouchServer(config_dict['server'])
            self.db = self.server.connectDatabase(config_dict['database'])

            if 'location' not in reg_info.keys():
                raise KeyError('Registration needs a location in its reg_info')
            self.location_hash = str(reg_info['location'].__hash__())
            reg_info['_id'] = self.location_hash
            reg_info['#config_hash'] = hash(str(reg_info))
            push_cfg = True
            if self.db.documentExists(self.location_hash):
                # If the doc exists, check that the configuration hasn't changed
                doc = self.db.document(self.location_hash)
                push_cfg = doc['#config_hash'] != reg_info['#config_hash']
                reg_info['_rev'] = doc['_rev']
            if push_cfg:
                self.db.commitOne(reg_info)
        except:
            # Don't want to raise anything here
            # TODO: but should probably log...
            pass
        self.report()


    def report(self):
        """
        'Ping' the RegSvc with a doc containing the service doc's ID and a
        timestamp, this can be used to provide uptime information.
        """
        try:
            self.db.commitOne({'service': self.location_hash}, timestamp=True)
        except:
            # Don't want to raise anything here
            # TODO: but should probably log...
            pass
