"""
WMCore/WorkQueue/Database/MySQL/Monitor/JobStatusByRequest.py
DAO object for WorkQueue

"""


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class JobStatusByRequest(DBFormatter):
    
    sql = """SELECT request_name, status, CAST(SUM(num_jobs) AS UNSIGNED) as jobs  
             FROM wq_element GROUP BY request_name, status"""
    
    def convertToQueueState(self, data):
        """
        Take data and convert status number to string
        TODO: overwrite formatDict to prevent this loop.
        """
        #total = 0
        convertedResult = {}
        for item in data:
            item.update(status = States[item['status']])
            if convertedResult.has_key(item['request_name']):
                if item['status'] == 'Available' \
                    or item['status'] == 'Negotiating':
                    convertedResult[item['request_name']]['inQueue'] += item['jobs']
                else:
                    convertedResult[item['request_name']]['inWMBS'] += item['jobs']
            else:
                convertedResult[item['request_name']] = {
                                    'request_name': item['request_name'],
                                    'inQueue': 0, 'inWMBS': 0}
                if item['status'] == 'Available' \
                    or item['status'] == 'Negotiating':
                    convertedResult[item['request_name']]['inQueue'] = item['jobs']
                else:
                    convertedResult[item['request_name']]['inWMBS'] = item['jobs']
                
        return convertedResult.values()
            
    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, conn = conn,
                                       transaction = transaction)
        formResults = self.formatDict(results)
        return self.convertToQueueState(formResults)
        
