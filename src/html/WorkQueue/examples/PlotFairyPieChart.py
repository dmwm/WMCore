from __future__ import print_function

# HOWTO run:
# 1) set up WMCore
# 2) db init
#    bin/wmcore-db-init --config src/python/WMCore/examples/WorkQueue/GlobalWQConfig.py --create --modules=WMCore.WorkQueue.Database,WMCore.MsgService,WMCore.Agent.Database
# 3) run compoments:
#    bin/wmcoreD --start --config src/python/WMCore/examples/WorkQueue/MonitorServiceConfig.py
# 4) python __file__
#     -> this prints out html file, normally resulting html snippet will be used ...


from future import standard_library
standard_library.install_aliases()

import json
import random
import urllib.parse

from WMCore.WorkQueue.DataStructs.WorkQueueElement import STATES
from WMQuality.WebTools.RESTClientAPI import makeRequest


# random colour chooser (or use matplotlib colourmap)
def random_colour():
    return '#%02x%02x%02x'%(random.randint(0,255),random.randint(0,255),random.randint(0,255))


# output file name
FILE_NAME = "PlotFairyPieChart.html"
PLOT_SERVICE_URL = "http://localhost:8888"
DATA_SOURCE_URL = "http://localhost:8888/workqueue/elementsinfo"


pageTemplate = \
"""
<html>\n<body>
<p>
<img src=%(url)s/plotfairy/plot/?type=%(plotType)s&data=%(hardCodedData)s>
</p>
<br><br>
<p>
<img src=%(url)s/plotfairy/plot/?type=%(plotType)s&data=%(systemData)s>
</p>
</body>\n</html>
"""



# hard-coded data
series = [{"label": x, "colour":  random_colour(), "value": random.randint(500, 1500)} for x in STATES]
plotDefinition = \
{
    "title": "Element status",
    "series": series,
    "shadow": True,
    "percentage": False,
    "labels": False, # don't print labels when having legend
    "legend": "topright",
    "sort": "value"
}
plotDefinition["title"] = "Element status - hard-coded"
hardCodedPlotData = urllib.parse.quote(json.dumps(plotDefinition, ensure_ascii = True))




# retrieve data from the system
# URL http://localhost:8888/workqueue/elementsinfo
data, _, _, _ = makeRequest(DATA_SOURCE_URL, verb = "GET", accept = "text/json+das",
                            contentType = "application/json")
data = json.loads(data)

systemSeries = series[:]
states = {}
for s in STATES:
    states[s] = 0 # status labels tuple
for elements in data["results"]:
    states[elements["status"]] = states[elements["status"]] + 1
for s in systemSeries:
    s["value"] = states[s["label"]]

plotDefinition["title"] = "Element status - WorkQueue"
plotDefinition["series"] = systemSeries
systemData = urllib.parse.quote(json.dumps(plotDefinition, ensure_ascii = True))



# write result file / generate result html snippet
outputFile = open(FILE_NAME, 'w')
finalPage = pageTemplate % {"url": PLOT_SERVICE_URL, "plotType": "Pie",
                            "hardCodedData": hardCodedPlotData,
                            "systemData": systemData}
outputFile.write(finalPage)
outputFile.close()
print("result html page '%s' written" % FILE_NAME)
