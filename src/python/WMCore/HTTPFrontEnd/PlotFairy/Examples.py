def generate_random_baobab():
	def random_recurse(total,depth_left):
		if depth_left==0:
			return {'label':'random', 'value':total, 'children':[]}
		else:
			children = []
			use = random.random()*total
			for i in range(3):
				children.append(random_recurse(use/3,depth_left-1))
			return {'label':'random', 'value':total, 'children':children}
	
	rand_dict = random_recurse(1000,3)
	return {'height':800,'width':800,'labelled':True,'title':'random baobab','data':rand_dict}

simple_sparkline = {
	'width':400, 'height':100,
	'series':[
		{'label':'series1','colour':'#ff0000','values':[random.random() for i in range(40)]},
		{'label':'series2','colour':'#00ff00','values':[random.random() for i in range(40)]},
		{'label':'series3','colour':'#0000ff','values':[random.random() for i in range(40)]}
	],
	'labelled':True,
	'overlay':False
}

simple_baobab = {
	'title':'simple baobab example',
	'width':800,'height':800,'labelled':True,
	'data':{
	'label':'root',
	'value':1000,
	"children":[
		{	'value':500,
			'label':'500',
			"children":[
				{'value':200,"children":[],'label':'200'},
				{'value':100,"children":[],'label':'100'},
			]},
		{	'value':250,
			"children":[],
			'label':'250'}]},
	'legend':True
}

simple_bar_labels = {
	'title':'simple labelled bar example',
	'width':800,'height':800,
	'xaxis':{'label':'Labelled axis','type':'labels','labels':['A','B','C','D']},
	'series':[
		{'label':'A', 'colour':'#ff0000','value':100},
		{'label':'B', 'colour':'#00ff00','value':200},
		{'label':'C', 'colour':'#0000ff','value':300}
	]
}

simple_bar_numeric = {
	'title':'simple numeric bar example',
	'width':800,'height':800,
	'xaxis':{'label':'Numeric axis','type':'num','min':0,'max':100,'width':10},
	'yaxis':{'label':'Log axis','log':False},
	'series':[
		{'label':'series1','colour':'#ff0000','values':range(10)},
		{'label':'series2','colour':'#00ff00','values':[x**2 for x in range(10)]},
		{'label':'series3','colour':'#0000ff','values':[x**3 for x in range(10)]}
	],
	'legend':True
}

simple_bar_time = {
	'title':'simple time bar example',
	'width':800,'height':800,
	'xaxis':{'label':'Numeric axis','type':'time','min':733280,'max':733290,'width':1},
	'yaxis':{'label':'Log axis','log':True},
	'series':[
		{'label':'series1','colour':'#ff0000','values':range(10)},
		{'label':'series2','colour':'#00ff00','values':[x**2 for x in range(10)]},
		{'label':'series3','colour':'#0000ff','values':[x**3 for x in range(10)]}
	],
	'legend':True
}

simple_quality_map = {
	'title':'simple quality map example',
	'width':800,'height':800,
	'xaxis':{'label':'numeric','type':'num','min':0,'max':100,'width':10},
	'yaxis':{'label':'labelled','type':'labels','labels':['xfer a','xfer b']},
	'data': [
		[.1,.2,.3,.4,.5,.6,.7,.8,.9,1.0],
		[1.0,.9,.8,.7,.6,.5,.4,.3,.2,.1]
	]
}