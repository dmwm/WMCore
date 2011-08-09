"""
Module for handling RSS (most commonly expanded as Really Simple Syndication)
XML document containing Alerts instances representation.
Usual structure of the RSS XML document is as follows, custom tags
corresponding to the attributes of the Alert class are added.
Only subset of possible RSS XML tags is used by RSSFeedSink.

Uses lxml library for XML handling: http://lxml.de/

<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
<channel>
<title> ... title of the feed ... </title>
<link> ... link </link>
<language>en-uk</language>
<pubDate>Thu, 09 Jun 2011 19:32:32 GMT</pubDate>
<description> ... more wordy description ...</description>
<image> - useful when the feed gets rendered e.g. in a web browser
<title> image title </title>
<url> image source link </url>
<link> link the image points to </link>
</image>

now follow particular <item> elements, their parent is <channel>
RSSFeedSink adds into into <item> as sub-elements Alert class attributes

<item>
<title> alert title </title>
<link> not used, but there may be link to details, perhaps some component monitoring ? ... </link>
<description> not used </description>
<pubDate>Thu, 09 Jun 2011 19:28:00 GMT</pubDate> formatted Timestamp
Timestamp Alert attribute itself is repeated, easier to check descending order in tests
    may later have some other sorting-like practical usage
<guid> not used </guid>
... other RSS elements are not used, others come from Alert.
</item>

<item>
...
</item>

<item>
...
</item>

...

</channel>
</rss>

"""



import sys
import os
import time
from StringIO import StringIO

#from lxml import etree
#from lxml import objectify

from WMCore.Alerts.ZMQ.Sinks.FileSink import FileSink



DATETIME_FORMAT = "%a, %d %b %Y %H:%M:%S %Z" # date-time format: Fri, 10 Jun 2011 10:19:44 CEST 
HEADER = """<?xml version="1.0" encoding="UTF-8"?>"""



class RSSFeedSink(FileSink):
    """
    Class inherits from FileSink just to use merely secured file handling
    via context manager. Nothing further.
    
    On new alerts arrival, a brand new document is coined adding up to
    self.config.depth Alert instances (as elements 'item'). If further items
    can be added (self.config.depth has not been exceeded in the previous
    step), then further items (alerts) are loaded from the RSS file, starting
    from the top of the document. Hence the items overall shall always
    remain sorted according to Timestamp in the final document which
    is written out as formatted XML.
    
    """
    
    
    def __init__(self, config):
        self.config = config
        self.config.link = "%s/%s" % (self.config.linkBase, self.config.outputfile)


    def load(self):
        """
        Load, parse, RSS XML document and return objectified document's
        representation. Returns root element of the document. 
        
        """
        with self._handleFile('r') as f:
            tree = etree.parse(StringIO(f.read()))
        return tree.getroot()

    
    def _addElem(self, parent, elem, text = None):
        """
        Method creates subelement elem of the parent element.
        Sets text content of the element if specified.
        
        """
        e = etree.Element(elem)
        parent.append(e)
        if text:
            e.text = text
        return e

    
    def _addElemsForAlerts(self, alerts, parent):
        """
        Creates <item> representation of Alert items in alerts list
        and adds custom subelements according to class Alert attributes.
        <item> is child element of parent element.
        Only up to self.config.depth alerts are turned into <item> elements.
        Returns number of added <item> elements.  
        
        """
        alerts.reverse() # the most recent appear first in the feed
        for count, a in zip(range(self.config.depth), alerts):
            item = self._addElem(parent, "item")
            self._addElem(item, "title", "Component '%s' alert." % a["Component"])
            dateTime = time.localtime(a["Timestamp"])
            dateTimeForm = time.strftime(DATETIME_FORMAT, dateTime)
            self._addElem(item, "pubDate", dateTimeForm)
            # for each Alert instance item create a custom subelements
            # alternatively, could str(a.items()) into standard <description> element
            [self._addElem(item, k, str(v)) for k, v in a.items()]
        return count + 1 # counting started from 0, total count is +1

    
    def _getNewRoot(self):
        """
        Starts new document according to the above summarized structure.
        
        """        
        root = etree.Element("rss", nsmap = {"atom": "http://www.w3.org/2005/Atom"})
        root.set("version", "2.0")
        
        channel = self._addElem(root, "channel")
        self._addElem(channel, "title", "CMS WMAgent alerts.")
        self._addElem(channel, "language", "en-uk")
        self._addElem(channel, "link", self.config.link)
        dateTimeNow = time.strftime(DATETIME_FORMAT, time.localtime())
        pubDate = self._addElem(channel, "pubDate", dateTimeNow)                                 
        self._addElem(channel, "description", "CMS WMAgent components alert condition messages.")
        return root, channel
    

    def send(self, alerts):
        """
        Translate incoming alerts list items into <item> RSS elements.
        If total self.config.depth has not been exceeded append also
        corresponding number of items from self.config.outputfile should
        it exit from previous send() call.
        
        """ 
        root, channel = self._getNewRoot()
        
        rootPrev = None
        if(os.path.exists(self.config.outputfile)):
            rootPrev = self.load()

        addedItemsCount = self._addElemsForAlerts(alerts, channel)
        if rootPrev is not None and addedItemsCount < self.config.depth:
            # the max. depth of feed has not been reached with newly arrived
            # alerts (each added as 'item'), replenish from file
            items = rootPrev.iterchildren(tag = "channel").next().iterchildren(tag = "item")
            for count, item in zip(range(addedItemsCount, self.config.depth), items):
                channel.append(item)
        
        with self._handleFile('w') as f:
            f.write("%s\n" % HEADER)
            # there is etree.tostring(root, pretty_print = True) but when
            # old <item> are added as above (append), this string formatting
            # considers all above as one tag and puts it on a single line ->
            # ugly formatted XML (just graphically, otherwise correct)
            # RSS-wise would be more logical if there was <items> <item> ... </items> ...
            # ugly hack (two unnecessary operations just for XML prettiness)
            # would be:
            """
            s = etree.tostring(root, pretty_print = True)
            o = objectify.fromstring(s)
            s = etree.tostring(o, pretty_print = True)
            """
            # other possibilities like using .addnext(), .extend() from
            # lxml.etree._Element were also tried in vain
            # for now, ok with ugly looking XML if elements are added from the file
            f.write(etree.tostring(root, pretty_print = True))
