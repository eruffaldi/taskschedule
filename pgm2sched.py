import json

class Domain:
	def __init__(self,name,values):
		self.name = name
		self.values = values

class VariableNode:
	def __init__(self,name,id,role,xtype):
		self.name = name
		self.id = id
		self.role = role
		self.xtype = xtype
		self.domain = None
		self.ddim = 0
		self.gdim = 0
	def inmessagetype(other):
		return (self.ddim,self.gdim)
		# always same as node
	def __repr__(self):
		return "VariableNode(%s,%s,dim=%d)" % (self.name,self.xtype,self.dim)

class FactorNode:
	def __init__(self,name,id,xtype):
		self.name = name
		self.id = id
		self.xtype = xtype
		self.vars = []
		self.dvars = []
		self.gvars = []
		self.ddim = 0
		self.gdim = 0

	def inmessagetype(other):
		# always as the variable connected 
		return other.inmessagetype()
	def __repr__(self):
		return "FactorNode(%s,%s,dim=G%d/D%d)" % (self.name,self.xtype,self.gdim,self.ddim)

class Pgm:
	def __init__(self,f):
		j = json.load(type(f) and open(f,"rb") or f)
		self.vars = {}
		self.domains = {}
		self.nodeid = {}
		self.facs = {}
		self.sched = []
		self.loaddomains(j["domains"])
		self.loadvars(j["variables"])
		self.loadfactors(j["factors"])
		self.loadsched(j["schedule"])
	def loaddomains(self,d):
		for v in d:
			name = v["name"]
			values = v["values"]
			self.domains[name] = Domain(name,values)
	def loadvars(self,d):
		for v in d:
			id = int(v["id"])
			name = v["name"]
			role = v["role"]
			xtype = v["type"]
			x = VariableNode(name,id,role,xtype)
			self.vars[name] = x
			self.nodeid[id] = x
			if xtype == "discrete":
				domain = v["domain"]
				x.domain = self.domains[domain]
				x.ddim = len(x.domain.values)
			else:
				dim = int(v["dim"])
				x.gdim = dim
	def loadfactors(self,d):
		for v in d:
			id = int(v["id"])
			name = v["name"]
			xtype = v["type"]
			g = FactorNode(name,id,xtype)			
			if xtype == "gmm":
				g.dvars = [self.vars[x] for x in v["discreteVars"]]
				g.gvars = [self.vars[x] for x in v["gaussianVars"]]
				g.vars = g.dvars + g.gvars
				g.gdim = sum([x.dim for x in g.gvars])
				g.ddim = reduce(lambda x,y:x*y,[x.dim for x in g.dvars],1)
				pass
			elif xtype == "discrete":
				g.vars = [self.vars[x] for x in v["variables"]]
				g.dvars = g.vars[:]
				g.ddim = reduce(lambda x,y:x*y,[x.dim for x in g.vars],1)
				g.gdim = 0
				pass
			elif xtype == "gaussian":
				g.vars = [self.vars[x] for x in v["variables"]]
				g.gvars = g.vars[:]
				g.ddim = 0
				g.gdim = sum([x.dim for x in g.vars])
				pass
			self.facs[name] = g
			self.nodeid[id] = g
	def loadsched(self,s):
		self.sched = [(self.nodeid[int(a["src"])],self.nodeid[int(a["dst"])]) for a in s["list"]]



if __name__ == "__main__":
	import sys
	q = Pgm(sys.argv[1])
	for a,b in q.sched:
		print a,"->",b
	print q.vars
	print q.facs