import random, urllib, math
try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json


WORDS = [w.strip().replace("'","") for w in open('/usr/share/dict/words').readlines()]
rbool = lambda: random.random()>0.5    

def random_colour():
    return '#%02x%02x%02x'%(random.randint(0,255),random.randint(0,255),random.randint(0,255))
    
def random_baobab():
	def random_recurse(total,depth_left):
		if depth_left==0:
			return {'label':'random', 'value':total, 'children':[]}
		else:
			children = []
			use = random.random()*total
			for i in range(3):
				children.append(random_recurse(use/3,depth_left-1))
			return {'label':'random', 'value':total, 'children':children}
	
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
                 lambda x: random.random(),
                 lambda x: x**2,
                 lambda x: x**3,
                 math.sqrt,
                 math.exp,
                 math.cos,
                 math.sin
                 ]
    terms = [random.choice(functions) for i in range(random.randint(1,4))]
    return [sum([t(i) for t in terms]) for i in range(length)]

def random_series(count=2,length=10):
    return [{'label':random.choice(WORDS),'colour':random_colour(),'values':random_data(length)} for i in range(count)]

def random_labelled_series(count=5):
    return [{'label':random.choice(WORDS),'colour':random_colour(),'value':random.randint(0,256)} for i in range(count)]

def random_quality_map(x=10,y=5):
    return {
            'title':random.choice(WORDS),
            'colour0':random_colour(),
            'colour1':random_colour(),
            'xaxis':{'label':random.choice(WORDS),'min':random.randint(0,256),'width':random.randint(0,16),'bins':x},
            'yaxis':{'label':random.choice(WORDS),'labels':[random.choice(WORDS) for i in range(y)]},
            'data':[[random.random() for i in range(x)] for j in range(y)],
            }
    
def random_scatter(x=10):
    return {
            'title':random.choice(WORDS),
            'xaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex'))},
            'yaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex'))},
            'draw_lines':rbool(),
            'series':[{'label':random.choice(WORDS),'colour':random_colour(),'x':random_data(x),'y':random_data(x),'marker':random.choice('*v^<>*o.')} for i in range(random.randint(1,5))]
    }
    
def random_bar(x=10):
    return {
            'title':random.choice(WORDS),
            'xaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex')),'min':random.randint(0,256),'width':random.randint(0,16),'bins':x},
            'yaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex')),'log':rbool(),'logbase':random.randint(2,10)},
            'series':random_series(length=x)
            }
    
def random_pie():
    return {
            'title':random.choice(WORDS),
            'series':random_labelled_series(),
            'shadow':rbool(),
            'percentage':rbool()
            }

def random_sparkline():
    return {
            'labelled':rbool(),
            'overlay':rbool(),
            'linewidth':random.random()*3,
            'text_fraction':random.random()*0.5,
            'series':random_series(length=20)
            }    
def random_wave():
    return {
            'title':random.choice(WORDS),
            'series':random_series(length=10),
            'text_size_min':random.randint(1,10),
            'truncate_text':random.randint(1,50),
            'labelled':rbool(),
            'text_span_bins':random.randint(3,10),
            'xaxis':{'label':random.choice(WORDS)}
            }
def random_cumulative(x=10):
    return {
            'title':random.choice(WORDS),
            'xaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex')),'min':random.randint(0,256),'width':random.randint(0,16),'bins':x},
            'yaxis':{'label':random.choice(WORDS),'format':random.choice(('num','time','binary','si','hex')),'log':rbool(),'logbase':random.randint(2,10)},
            'series':random_series(length=x+1)
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
print "<table>"
for t,f in plots.items():
    for i in range(3):
        d = f()
        print "<tr>"
        print "<td><h2>%s-%s</h2><br><pre>"%(t,i)
        print json.dumps(d,sort_keys=True,indent=4)
        print "</pre></td><td>"
        print "<img src='http://localhost:8010/plotfairy/plot/?type=%s&data=%s'>"%(t,urllib.quote(json.dumps(d,ensure_ascii=True)))
        print "</td></tr>"
print "</table>"
print "</body>\n</html>"	