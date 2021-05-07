from __future__ import print_function

# HOWTO run:
# 1) set up WMCore
# 2) db init
#    bin/wmcore-db-init --config src/python/WMCore/examples/WorkQueue/GlobalWQConfig.py --create --modules=WMCore.WorkQueue.Database,WMCore.MsgService,WMCore.Agent.Database
# 3) run compoments:
#    bin/wmcoreD --start --config src/python/WMCore/examples/WorkQueue/MonitorServiceConfig.py
# 4) python __file__
#     -> this prints out html file, normally resulting html snippet will be used ...


from builtins import range
from future import standard_library
standard_library.install_aliases()

import json
import random
import urllib.parse


from WMCore.WorkQueue.DataStructs.WorkQueueElement import STATES


# random colour chooser (or use matplotlib colourmap)
def random_colour():
    return '#%02x%02x%02x'%(random.randint(0,255),random.randint(0,255),random.randint(0,255))

def random_data():
    return [random.randint(10, 20) for _ in range(4)]


# output file name
FILE_NAME = "PlotFairyLineChart.html"
PLOT_SERVICE_URL = "http://localhost:8888"
DATA_SOURCE_URL = "http://localhost:8888/workqueue/elementsinfo"


pageTemplate = \
"""
<html>\n<body>
<p>
<img src=%(url)s/plotfairy/plot/?type=%(plotType)s&data=%(hardCodedData)s>
</p>
<br><br>
</body>\n</html>
"""


# hard-coded data
series = [{"label": x, "colour": random_colour(), 'x': random_data(), 'y': random_data(), "marker": '*'} for x in STATES]
plotDefinition = \
{
    "title": "Job element recent evolution",
    "shadow": True,
    "series": series,
    "sort": "value",
    "legend": "right",
    "draw_lines": True,
    # TODO
    # time axis format doesn't seem to work properly - even in PlotFairy:Example.py
    "xaxis": {"label": "time", "format": "time"},
    # TODO
    # how to set y-axis description just a text label?
    "yaxis": {"label": "thousands of jobs", "format": "si"}
}
hardCodedPlotData = urllib.parse.quote(json.dumps(plotDefinition, ensure_ascii = True))


# TODO
# get data from the system (URL) ... should have particular visualisation request ...
# see PlotFairyPieChart for reference ...
# retrieve data from the system
# URL http://localhost:8888/workqueue/elementsinfo



# write result file / generate result html snippet
outputFile = open(FILE_NAME, 'w')
finalPage = pageTemplate % {"url": PLOT_SERVICE_URL, "plotType": "Scatter",
                            "hardCodedData": hardCodedPlotData}
outputFile.write(finalPage)
outputFile.close()
print("result html page '%s' written" % FILE_NAME)
