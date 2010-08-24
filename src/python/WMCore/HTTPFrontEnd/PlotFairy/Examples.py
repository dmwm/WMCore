import random, urllib, math
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json


WORDS = [w.strip().replace("'","") for w in open('/usr/share/dict/words').readlines()]

def rbool(trueprob=0.5):
    return random.random()<trueprob

def random_legend():
    return random.choice(['left','right','top','bottom','topleft','topright','bottomleft','bottomright']+['null']*4)

def random_sort():
    return random.choice(['label','label_reverse','value','value_reverse']+['null']*2)

def random_colour():
    return '#%02x%02x%02x'%(random.randint(0,255),random.randint(0,255),random.randint(0,255))
    
def random_baobab():
    def random_recurse(total,depth_left):
        if depth_left==0:
            return {'label':random.choice(WORDS), 'value':total}
        else:
            children = []
            use = (0.5+random.random()*0.5)*total
    	    for i in range(random.randint(1,8)):
                use_here = use*random.random()
                use -= use_here
                children.append(random_recurse(use_here,depth_left-1))
            return {'label':random.choice(WORDS), 'value':total, 'children':children}
	
    rand_dict = random_recurse(10**random.randint(1,6),random.randint(1,5))
    return {
            'data':rand_dict,
            'title':random.choice(WORDS),
            'minpixel':random.randint(5,50),
            'scale':rbool(),
            'unit':'B',
            'format':random.choice(('num','si','binary')),
            'dropped_colour':random_colour(),
            'external':rbool(),
            'scale_number':random.randint(1,10),
            'labelled':rbool(),
            'central_label':rbool(),
            'dropped_colour_size':random.random(),
            'text_truncate_inner':random.randint(1,50),
            'text_truncate_outer':random.randint(1,50),
            'text_size_min':random.randint(1,10)
            }

def random_data(length=10):
    functions = [
                 lambda x: x,
                 lambda x: 4+random.random(),
                 lambda x: 0.1*x**1.5,
                 lambda x: 5*math.cos(x)+10,
                 lambda x: 5*math.sin(x)+10,
                 lambda x: 30./x if not x==0 else 0,
                 lambda x: 40./x**2 if not x==0 else 0,
                 lambda x: math.sin(x)/x if not x==0 else 0,
                 ]
    func = random.choice(functions)
    return [func(i) for i in range(length)]

def random_series(count=5,length=10):
    return [{'label':random.choice(WORDS),'colour':random_colour(),'values':random_data(length)} for i in range(count)]

def random_labelled_series(count=5):
    return [{'label':random.choice(WORDS),'colour':random_colour(),'value':random.randint(0,256)} for i in range(count)]

def random_quality_map():
    x = random.randint(4,32)
    y = random.randint(2,8)
    return {
            'title':random.choice(WORDS),
            'colour0':random_colour(),
            'colour1':random_colour(),
            'xaxis':{'label':random.choice(WORDS),'min':random.randint(0,256),'width':random.randint(1,16),'bins':x},
            'yaxis':{'label':random.choice(WORDS),'labels':[random.choice(WORDS) for i in range(y)]},
            'data':[[random.random() for i in range(x)] for j in range(y)],
            }
    
def random_scatter():
    x = random.randint(4,32)
    return {
            'title':random.choice(WORDS),
            'xaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex'))},
            'yaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex'))},
            'draw_lines':rbool(),
            'series':[{'label':random.choice(WORDS),'colour':random_colour(),'x':random_data(x),'y':random_data(x),'marker':random.choice('*v^<>*o.')} for i in range(random.randint(1,5))],
            'legend':random_legend(),
            'sort':random_sort()
    }
    
def random_bar():
    x = random.randint(4,32)
    return {
            'title':random.choice(WORDS),
            'xaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex')),'min':random.randint(0,256),'width':random.randint(1,16),'bins':x},
            'yaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex')),'log':rbool(0.25),'logbase':random.randint(2,10)},
            'series':random_series(length=x,count=random.randint(1,5)),
            'legend':random_legend(),'sort':random_sort()
            }
    
def random_pie():
    return {
            'title':random.choice(WORDS),
            'series':random_labelled_series(count=random.randint(1,20)), #d20
            'shadow':rbool(),
            'percentage':rbool(),
            'sort':random_sort(),
            'legend':random_legend(),
            'labels':rbool()
            }

def random_sparkline():
    x = random.randint(16,64)
    return {
            'labelled':rbool(),
            'overlay':rbool(),
            'linewidth':random.random()*3,
            'text_fraction':random.random()*0.5,
            'series':random_series(length=x),
            'sort':random_sort(),
            'height':200,
            'width':400
            }    
def random_wave():
    x = random.randint(4,32)
    return {
            'title':random.choice(WORDS),
            'series':random_series(length=x,count=random.randint(1,10)),
            'text_size_min':random.randint(1,10),
            'truncate_text':random.randint(1,50),
            'labelled':rbool(),
            'text_span_bins':random.randint(3,10),
            'xaxis':{'label':random.choice(WORDS)},
            'sort':random_sort()
            }
def random_cumulative():
    x = random.randint(4,32)
    return {
            'title':random.choice(WORDS),
            'xaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex')),'min':random.randint(0,256),'width':random.randint(1,16),'bins':x},
            'yaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex')),'log':rbool(0.25),'logbase':random.randint(2,10)},
            'series':random_series(length=x+1,count=random.randint(1,5)),
            'legend':random_legend(),'sort':random_sort()
            }

plots = {
         'Baobab':random_baobab,
         'Scatter':random_scatter,
         'Bar':random_bar,
         'Pie':random_pie,
         'Sparkline':random_sparkline,
         'Wave':random_wave,
         'QualityMap':random_quality_map,
         'Cumulative':random_cumulative
         }

print "<html>\n<body>"
print "<h1>Example plots</h1>"
print "<script type='text/javascript'>function show(id){var el = document.getElementById(id);el.style.display = (el.style.display != 'none' ? 'none' : '' );};</script>"
print "<table>"
for t,f in plots.items():
    for i in range(3):
        d = f()
        print "<tr>"
        print "<td><h2><a href='http://localhost:8010/plotfairy/doc/?type=%s'>%s-%s</a></h2><br><a href='#' onclick='show(\"%s%s\")'>Show/Hide JSON</a><br><pre id='%s%s' style='display:none'>"%(t,t,i,t,i,t,i)
        print json.dumps(d,sort_keys=True,indent=4)
        print "</pre></td><td>"
        print "<img src='http://localhost:8010/plotfairy/plot/?type=%s&data=%s'>"%(t,urllib.quote(json.dumps(d,ensure_ascii=True)))
        print "</td></tr>"
print "</table>"
print "</body>\n</html>"	