'''
Builds a plot from json data.
'''
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.Services.Requests import JSONRequests
from matplotlib.pyplot import figure
import matplotlib.cm as cm
from matplotlib.patches import Rectangle
from cherrypy import request
import numpy as np
import urllib
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json
    
class Plotter(RESTModel):
    '''
    A class that generates a plot object to send to the formatter, possibly 
    downloading the data from a remote resource along the way
     
    '''
    def __init__(self, config):
        RESTModel.__init__(self, config)
        
        self.methods['POST'] = {'plot': {'version':1,
                                         'call':self.plot,
                                         'args': ['type', 'data', 'url'],
                                         'expires': 300}}
        self.methods['GET'] = {'plot': {'version':1,
                                         'call':self.plot,
                                         'args': ['type', 'data', 'url'],
                                         'expires': 300}}
        self.plot_types = {'bar': self.bar, 
                           'pie':self.pie, 
                           'scatter':self.scatter,
                           'cumulative':self.cumulative,
                           'baobab':self.baobab,
                           'quality_map':self.quality_map}

    def baobab(self,input):
        """
        Make a baobab/filelight hierarchical pie chart.

        Requires input:
        { width, height, title...
          data: {
            label:'root element (not drawn)',
            value:1000,
            children:[
              {label: 'top level element', value: 500, children: []},
              {label: 'top level element2', value: 250, children: [
                {label: 'second level element', value: 100, children: []}
              ]}
            ]
          },
        }
        The sum of all first-level child elements must be less than or equal to the parent value.
        Optional top-level elements include
        'labelled':False - draw names on each element
        'cm':'name of a matplotlib colourmap' - colouring to use for elements
        'threshold':0.05 - threshold of parent value below which children are culled to unclutter the plot
        """
        xy = (input['width']/input.get('dpi',96),input['height']/input.get('dpi',96))
        fig = figure(figsize=xy)
    
        axes = fig.add_axes([0.1,0.1,0.8,0.8],polar=True)
        axes.set_title(input.get('title',''))
        axes.set_axis_off()
    
        data_root = input['data']
        cmap = input.get('cm','Accent')
        cmap = cm.get_cmap(cmap)
        if not cmap:
            cmap = cm.Accent 
    
        theta = lambda x: x*(2*math.pi)/data_root['value']
        threshold = input.get('threshold',0.05)
        
        
    
        def fontsize(n):
            if len(n)>16:
                return 6
            if len(n)>8:
                return 8
            return 10
     
    
        def bar_recursor(depth,here,startx):
            if len(here['children'])>0:
                left=[theta(startx)]
                height=[1]
                width=[theta(here['value'])]
                bottom=[depth]
                name=[here['label']]
                value=[here['value']]
                dumped = 0
                for c in here['children']:
                    #if c['value']>here['value']*threshold:
                    if theta(c['value'])>(9./(depth+1)**2)*math.pi/180:
                        cleft,cheight,cwidth,cbottom,cname,cvalue = bar_recursor(depth+1,c,startx)
                        left.extend(cleft)
                        height.extend(cheight)
                        width.extend(cwidth)
                        bottom.extend(cbottom)
                        name.extend(cname)
                        value.extend(cvalue)
                        startx += c['value']
                    else:
                        dumped += c['value']
                if dumped>0:
                    left.append(theta(startx))
                    height.append(0.75)
                    width.append(theta(dumped))
                    bottom.append(depth+1)
                    name.append('')
                    value.append(dumped)
                return left,height,width,bottom,name,value
            else:
                return [theta(startx)],[1],[theta(here['value'])],[depth],[here['label']],[here['value']]
    
        left,height,width,bottom,name,value = bar_recursor(0,data_root,0)
    
        colours = [cmap(l/(2*math.pi)) for l in left]
        for i,h in enumerate(height):
            if h<1:
                colours[i] = '#cccccc'
            
    
        font = FontProperties()
    
        max_height = max(bottom)
    
        unit = input.get('unit','')
        
        if input.get('scale',False):
            bar_location = data_root['value']/5.
            magnitude = int(math.log10(bar_location))
            possible = [i*10**magnitude for i in (1.,2.,5.,10.)]
            best_delta = 10*10**magnitude
            use_bar_location = 0
            for p in possible:
                if abs(p-bar_location)<best_delta:
                    best_delta=abs(p-bar_location)
                    use_bar_location=p
    
            for i in range(int(data_root['value']/use_bar_location)+1):
                lx = theta(i*use_bar_location)
                ly = max_height+3
            
                line = Line2D((lx,lx),(0.75,ly),linewidth=1,linestyle='-.',zorder=-2,color='blue')
                axes.add_line(line)
                axes.text(lx,ly,SIFormatter(i*use_bar_location,unit),horizontalalignment='center',verticalalignment='center',zorder=-1,color='blue')
    
    
        bars = axes.bar(left=left[1:],height=height[1:],width=width[1:],bottom=bottom[1:],color=colours[1:])
    
        if input.get('labelled',False):
            max_height = max(bottom[1:])
            min_height = min(bottom[1:])
            for l,h,w,b,n,v in zip(left[1:],height[1:],width[1:],bottom[1:],name[1:],value[1:]):
                cx = l+w*0.5
                cy = b+h*0.5
                
                angle_deg = cx*(180/math.pi)
                angle_rad = angle_deg
                angle_tan = angle_deg - 90
                
                if angle_deg>90 and angle_deg<=180:
                    angle_rad += 180
                if angle_deg>180 and angle_deg<=270:
                    angle_rad -= 180
                    angle_tan -= 180
                if angle_deg>270 and angle_deg<=360:
                    angle_tan -= 180                
                if b==max_height:
                    axes.text(cx,cy+1.5,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=fontsize(n))
                elif b==min_height:
                    axes.text(cx,cy,n,horizontalalignment='center',verticalalignment='center',rotation=angle_tan,size=fontsize(n))
                else:
                    axes.text(cx,cy,n,horizontalalignment='center',verticalalignment='center',rotation=angle_rad,size=fontsize(n))
    
            axes.text(0,0,SIFormatter(data_root['value'],unit),horizontalalignment='center',verticalalignment='center',weight='bold')
    
        return fig
    
    def bar(self, input):
        """
        Produce an object representing a bar chart

        This produces two disparate types of plots. The first is a labelled bar chart containing a single series
        { width, height, title etc
          xaxis: {
            type:'labels',
            labels:['label0', 'label1', 'label2']
          },
          series: [
            {label: 'label0', value: 100, colour: '#ffffff'},
            {label: 'label1', value: 200, colour: '#ffffff'}, ...
          ]
        }
        Labels in the series must correspond to labels defined for the axis.
        
        The second case is a numerical-axis plot, which can have multiple series.
        { width, height, title etc
          xaxis: {
            type:'num',
            min:0,
            max:100,
            width:10
          },
          series: [
            {label: 'label0', values: [0,1,2,3,4,5,6,7,8,9], colour: '#ffffff'},
            {label: 'label1', values: [0,1,2,3,4,5,6,7,8,9], colour: '#ffffff'}, ...
          ]
        }
        Optional arguments for axes are
        'log': make the numerical axis logarithmic
        'label': axis title
        'type': 'labels' for labelled case, 'num' for general numeric case, 'time' for time formatting
        Please note matplotlib uses a daft time format whereby time x-axis values are interpreted as floating-point numbers of days since 01-01-0001. Might add a UTC converter at some point...
        Optional arguments for the plot are
        'yaxis' - to define a y axis label, log etc. Min/max/labels are ignored for this axis.
        'legend' - try and draw a legend.
        """
        xy = (input['width']/input.get('dpi',96),input['height']/input.get('dpi',96))
        fig = figure(figsize=xy)
    
        axes = fig.add_axes([0.1,0.1,0.8,0.8])
        axes.set_title(input.get('title',''))
        xaxis = input.get('xaxis',{})
        yaxis = input.get('yaxis',{})
    
        axes.set_xlabel(xaxis.get('label',''))
        axes.set_ylabel(yaxis.get('label',''))

        xtype = xaxis.get('type','num')
        series = input.get('series',[])
    
        logmin = 0
        if yaxis.get('log',False):
            axes.set_yscale('log')
            if len(series)>0:
                logmin = min(1,filter(lambda x: x>0, series[0]['values']))
    
    
        if xtype=='labels':
            names = {}
            labels = xaxis.get('labels',[])
            for i,n in enumerate(labels):
                names[n]=i
            values = {}
            colours = {}
            seennames = []
            for s in series:
                assert s['label'] in names
                seennames.append(s['label'])
                values[s['label']]=s['value']
                colours[s['label']]=s['colour']
            axes.set_xticklabels(labels)
            axes.set_xticks([i+0.5 for i in range(len(labels))])
            left = [names[n] for n in seennames]
            height = [values[n] for n in seennames]
            bottom = [logmin for n in seennames]
            colour = [colours[n] for n in seennames]
            axes.bar(left,height,width=1,bottom=bottom,color=colour)
        else:
            x_min = xaxis.get('min',0)
            x_max = xaxis.get('max',1)
            x_width = xaxis.get('width',1)
            x_range = x_max-x_min
            x_bins = int(x_range/x_width)

            bottom = [logmin for i in range(x_bins)]
            width = [x_width for i in range(x_bins)]
            left = [x_min+i*x_width for i in range(x_bins)]
            for s in series:
                height = s['values']
                assert len(height)==x_bins
                colour = s['colour']
                axes.bar(left,height,width,bottom,label=s['label'],facecolor=colour)
                bottom = [b+h for b,h in zip(bottom,height)]
            
            if input.get('legend',False):
                axes.legend(loc=0)
            if xtype=='time':
                axes.xaxis_date()        
            axes.set_xbound(x_min,x_max)
        return fig
   
    def cumulative(self,input):
        """
        Draw a cumulative plot. The argument and optional arguments for this are identical to the numerical bar chart case above.
        """
        xy = (input['width']/input.get('dpi',96),input['height']/input.get('dpi',96))
        fig = figure(figsize=xy)
        
        axes = fig.add_axes([0.1,0.1,0.8,0.8])
        axes.set_title(input.get('title',''))
        xaxis = input.get('xaxis',{})
        yaxis = input.get('yaxis',{})
        
        axes.set_xlabel(xaxis.get('label',''))
        axes.set_ylabel(yaxis.get('label',''))
        
        xtype = xaxis.get('type','num')
        series = input.get('series',[])
        
        logmin = 0
        if yaxis.get('log',False):
            axes.set_yscale('log')
            if len(series)>0:
                logmin = min(1,filter(lambda x: x>0, series[0]['values']))
        
        x_min = xaxis.get('min',0)
        x_max = xaxis.get('max',1)
        x_width = xaxis.get('width',1)
        x_range = x_max-x_min
        x_bins = int(x_range/x_width)
        
        y0 = [logmin for i in range(x_bins)]
        x = [x_min+(i+1)*x_width for i in range(x_bins)]
        
        for s in series:
            height = s['values']
            assert len(height)==x_bins
            colour = s['colour']
            y1 = [y+h for y,h in zip(y0,height)]
            axes.fill_between(x,y1,y0,label=s['label'],facecolor=colour)
            y0 = y1
        
        if xtype=='time':
            axes.xaxis_date()                
        axes.set_xbound(x_min,x_max)
        
        if input.get('legend',False):
            axes.legend([Rectangle((0,0),1,1,fc=s['colour']) for s in series],[s['label'] for s in series],loc=0)
        return fig
        
    def sparkline(self,input):
        """
        Draw an unlabelled line plot. Syntax is the same as for a pie chart.
        { width, height, title etc
          series: [
            {label: 'label0', values: [0,1,2,3,4,5,6,7,8,9], colour: '#ffffff'},
            {label: 'label1', values: [0,1,2,3,4,5,6,7,8,9], colour: '#ffffff'}, ...
          ]
        }
        Optional arguments are:
        'labelled': Add labels to the left of each plot.
        'overlay': Overlay the plots, instead of making a stack of separate plots.
        """
        xy = (input['width']/input.get('dpi',96),input['height']/input.get('dpi',96))
        fig = figure(figsize=xy)
    
        labelled = input.get('labelled',False)
        overlay = input.get('overlay',False)
    
        data = input['series']    
        for i,d in enumerate(data):
            if not 'colour' in d:
                d['colour'] = cm.Dark2(float(i)/len(data))
            if not 'label' in d:
                d['label'] = ''
            if not 'values' in d:
                d['values'] = [0]
    
        if len(data)>0:
            if overlay and len(data)>1:
                axes = fig.add_axes([0.1,0.1,0.9,0.9])
                miny=maxy=0
                maxlen=0
                for i,d in enumerate(data):
                    axes.plot(d['values'],color=d['colour'])
                    miny=min(miny,min(d['values']))
                    maxy=max(maxy,max(d['values']))
                    maxlen=max(maxlen,len(d['values']))
                axes.autoscale_view()
                if labelled:
                    for i,d in enumerate(data):
                        cy = miny + (maxy-miny)*((float(i)/len(data))+(0.5/len(data)))
                        axes.text(-1,cy,d['label'],horizontalalignment='right',verticalalignment='center',fontsize=10,color=d['colour'])
                    axes.set_xlim(xmin=-0.2*maxlen)
                axes.set_axis_off()
        
            
            else:
                for i,d in enumerate(data):
                    axes = fig.add_subplot(len(data),1,i+1)
                    axes.plot(d['values'],color=d['colour'])
                    cy = 0.5*(max(d['values'])-min(d['values']))+min(d['values'])
                    axes.set_axis_off()    
                    if labelled:
                        axes.text(-1,cy,d['label'],horizontalalignment='right',verticalalignment='center',fontsize=10,color=d['colour'])
                        axes.set_xlim(xmin=-0.2*len(d['values']))
        
        return fig

                
    def quality_map(self,input):
        """
        Draw a quality map as used for phedex transfer quality measurements, etc.
        
        Argument in the form:
        { width, height, title etc
          xaxis: {type: 'num', min:0, max:2, width: 1},
          yaxis: {type: 'labels', labels: ['A','B']},
          data: [
            [0.1,0.2],
            [0.3,0.4]
          ]
        }
        Both axis definitions can be either labels or numeric, and obey the rules described under bar charts.
        Data should be a list of rows, containing floats in the range 0>=x>=1.
        """
        xy = (input['width']/input.get('dpi',96),input['height']/input.get('dpi',96))
        fig = figure(figsize=xy)
        
        axes = fig.add_axes([0.1,0.1,0.8,0.8])
        axes.set_title(input.get('title',''))
        xaxis = input.get('xaxis',{})
        yaxis = input.get('yaxis',{})
        
        axes.set_xlabel(xaxis.get('label',''))
        axes.set_ylabel(yaxis.get('label',''))
        
        xtype = xaxis.get('type','num')
        ytype = yaxis.get('type','num')
        data = input.get('data',[[]])
        
        x_left = []
        x_width = []
        y_bottom = []
        y_height = []
        
        if xtype=='labels':
            xlabels = xaxis.get('labels',[])
            axes.set_xticklabels(xlabels)
            axes.set_xticks([i+0.5 for i in range(len(xlabels))])
            x_left = range(len(xlabels))
            x_width = 1
        else:
            x_min = xaxis.get('min',0)
            x_max = xaxis.get('max',1)
            x_width = xaxis.get('width',1)
            x_range = x_max-x_min
            x_bins = int(x_range/x_width)
            x_left = [x_min+i*x_width for i in range(x_bins)]
            if xtype=='time':
                axes.xaxis_date()        
                
        if ytype=='labels':
            ylabels = yaxis.get('labels',[])
            axes.set_yticklabels(ylabels)
            axes.set_yticks([i+0.5 for i in range(len(ylabels))])
            y_bottom = range(len(ylabels))
            y_height = 1
        else:
            y_min = yaxis.get('min',0)
            y_max = yaxis.get('max',1)
            y_height = yaxis.get('width',1)
            y_range = y_max-y_min
            y_bins = int(y_range/y_height)
            y_bottom = [y_min+i*y_height for i in range(y_bins)]
            if ytype=='time':
                axes.yaxis_date()        
        
        for y,row in enumerate(data):
            for x,col in enumerate(row):
                assert col>=0.
                assert col<=1.
                axes.bar(left=x_left[x],bottom=y_bottom[y],width=x_width,height=y_height,facecolor=[1-col,col,0])
        return fig

    def pie(self, input):
        """
        Produce an object representing a pie chart
                
        { width, height, title etc
          series:[
            {label:'Frogs',value:15,colour:'#ffffff'},
            {label:'Hogs',value:30,colour:'#ffffff'}
          ]
          explode:1  
        }
        """
        
        xy = (input['width']/input.get('dpi',96), input['height']/input.get('dpi',96))
        fig = figure(figsize = xy)
        axis = fig.add_axes([0.1, 0.1, 0.8, 0.8])
        
        labels = []
        fracs = []
        colours = []
        for s in input['series']:
            labels.append(s['label'])
            fracs.append(s['value'])
            colours.append(s['colour'])
            explode = [0] * len(fracs)
        if 'explode' in input:
            ind = input['explode'] - 1
            explode[ind] = 0.05
        axis.pie(fracs, explode=explode, labels=labels, autopct='%1.1f%%', shadow=True,colors=colours)
        
        return fig
    
    def scatter(self, input):
        """
        Produce an object representing a scatter chart
        Expects a json document like:
        
        {'data': { 
                'series': [
                    {'title': u'Series 1', 
                    'Std': [2, 3, 4, 1, 2], 
                    'Means': [20, 35, 30, 35, 27]}, 
                    {'title': u'Series 2', 
                    'Std': [3, 5, 2, 3, 3], 
                    'Means': [25, 32, 34, 20, 25]}
                ], # each series will be plotted
            'height': 400,
            'width': 400, 
            'x-name': 'Means', # what to plot on the x axis from the dicts in "series"
            'y-name': 'Std', # what to plot on the y axis from the dicts in "series" 
            'title': 'My plot title', # optional, fall back to the value of x-name
            'error': 'Std', # or x-error & y-error, values to plot as error bars - optional
            }
        'type': 'scatter'}
        
        returns a matplotlib Figure object
        """
        xy = (input['width']/input.get('dpi',96), input['height']/input.get('dpi',96))
        fig = figure(figsize = xy)
        axis = fig.add_axes([0.2, 0.1, 0.75, 0.75], 
                            xlabel=input['x-name'], 
                            ylabel=input['y-name'],
                            title=input.get('title',
                                                    input.get('x-name',
                                                                  'My Plot')))
        point_styles = ['rs', 'go', 'bt', 'y*']
        lines = ()
        titles = ()
        for series in input['series']:
            x = series[input['x-name']]
            y = series[input['y-name']]
            l = axis.plot(x, y, point_styles[input['series'].index(series)])
            lines += (l,)
            titles += (series['title'],)
        fig.legend(lines, titles, 'upper right')

        return fig
    
    def plot(self, *args, **kwargs):
        input = self.sanitise_input(args, kwargs)
        if not input['data']:
            # We have a URL for some json - we hope!
            jr = JSONRequests(input['url'])
            input['data'] = jr.get()
        return {'figure': self.plot_types[input['type']](input['data'])}
            
    def validate_input(self, input, verb, method):
        if not 'data' in input.keys():
            input['data'] = {'height': 600, 'width': 800}
            assert 'url' in input.keys()
            reg = "(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?"
            assert re.compile(reg).match(input['url']) != None , \
              "'%s' is not a valid URL (regexp: %s)" % (input['url'], reg)
        else:
            input['data'] = json.loads(urllib.unquote(input['data']))
            
        if not 'height' in input['data'].keys():
            input['data']['height'] = 600
        else:
            input['data']['height'] = int(input['data']['height'])
            
        if not 'width' in input['data'].keys():
            input['data']['width'] = 800
        else:
            input['data']['width'] = int(input['data']['width'])
            
        assert 'type' in input.keys(), \
                "no type provided - what kind of plot do you want? " +\
                "Choose one of %s" % self.plot_types.keys()
        assert input['type'] in self.plot_types.keys(), \
                "the plot you chose (%s) is not in the supported types (%s)" %\
                        (input['type'],self.plot_types.keys()) 
        return input