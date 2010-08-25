from Plot import Plot

class QualityMap(Plot):
    def plot(self,input):
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
