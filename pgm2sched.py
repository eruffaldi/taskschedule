#TODO observations
#TODO paralel beliefs
import json
from collections import defaultdict
from functools import reduce as _reduce


# Taken from: https://pypi.python.org/pypi/toposort/1.0
def toposort(data):
    """Dependencies are expressed as a dictionary whose keys are items
and whose values are a set of dependent items. Output is a list of
sets in topological order. The first set consists of items with no
dependences, each subsequent set consists of items that depend upon
items in the preceeding sets.
"""
    # Special case empty input.
    if len(data) == 0:
        return

    # Copy the input so as to leave it unmodified.
    data = data.copy()

    # Ignore self dependencies REMOVED

    # Find all items that don't depend on anything.
    extra_items_in_deps = _reduce(set.union, data.values())- set(data.keys())
    # Add empty dependences where needed.
    data.update({item:set() for item in extra_items_in_deps})
    while True:
        ordered = set(item for item, dep in data.items() if len(dep) == 0)
        if not ordered:
            break
        yield ordered
        data = {item: (dep - ordered)
                for item, dep in data.items()
                    if item not in ordered}
    if len(data) != 0:
        raise ValueError('Cyclic dependencies exist among these items: {}'.format(', '.join(repr(x) for x in data.items())))


def toposort_flatten(data, sort=True):
    """Returns a single list of dependencies. For any set returned by
toposort(), those items are sorted and appended to the result (just to
make the results deterministic)."""
    result = []
    for d in toposort(data):
        result.extend((sorted if sort else list)(d))
    return result

# sorts tasks topologically
def toposorttasks(data,sort=True):
    qd = dict([(t.name,t) for t in data]) # build the dictionary for reconstruction
    q = dict([(t.name,set([p.name for p in t.parents])) for t in data]) # build dependency as list of id

    # back from id tho objects list
    return [qd[tid] for tid in toposort_flatten(q,sort=sort)]


class Task:
    def __init__(self,name,role,message):
        self.name = name
        self.role = role # aggregate messages, message, belief, observation
        self.node = None
        self.message = message
        self.parents = []
        self.schildren = set()
        self.children = []
        self.cost = 0
        self.items = 1
    def addchild(self,t):
        if not t in self.schildren:
            self.children.append(t)
            t.parents.append(self)
            self.schildren.add(t)
    def addparent(self,t):
        t.addchild(self)
    def __repr__(self):
        return "Task(%s,%s,#p%d,#c%d)" % (self.name,self.action,len(self.parents),len(self.children))

class Domain:
    def __init__(self,name,values):
        self.name = name
        self.values = values

class Node:
    def __init__(self,name,id):
        self.name = name
        self.id = id
        #for scheduling
        self.seenout = False
        self.seenouttask = None
        self.received = []
    def inmessagetype(self,other):
        # compute the size of the message exchanged, corresponds to the size of which is a variable of the two
        if isinstance(other,VariableNode):
            return (other.gdim,other.ddim)
        else:
            return (self.gdim,self.ddim)
    def restmessagetype(self,other):
        # the part of the message of the factor that is not covered by the message in other
        # difference of size in gaussian, division for discrete
        if isinstance(other,VariableNode):
            return (self.gdim-other.gdim,self.ddim/other.ddim)
        else:
            return (other.gdim-self.gdim,other.ddim/self.ddim)
    def costinmessage(self,src):
        # cost of the message from src to this: extract from src and just store here
        return 1 # just store
    def costaggregate(self,src):
        # cost aggregating the message from src (PRODUCT) to this
        return 1
    def costoutmessage(self,dst):
        # cost of message OUT from this to dst: subtract the message received from dst to the accumulated
        return 1

class VariableNode(Node):
    def __init__(self,name,id,role,xtype):
        Node.__init__(self,name,id)
        self.role = role
        self.xtype = xtype
        self.domain = None
        self.ddim = 0
        self.gdim = 0
        self.factors = [] 
    def adjacent(self):
        return self.factors
    def __repr__(self):
        return "VariableNode(%s,%s,dim=G%d/D%d)" % (self.name,self.xtype,self.gdim,self.ddim)

class FactorNode(Node):
    def __init__(self,name,id,xtype):
        Node.__init__(self,name,id)
        self.xtype = xtype
        self.vars = []
        self.dvars = []
        self.gvars = []
        self.prior = False
        self.ddim = 0
        self.gdim = 0
    def adjacent(self):
        return self.vars
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
                g.gdim = sum([x.gdim for x in g.gvars])
                g.ddim = reduce(lambda x,y:x*y,[x.ddim for x in g.dvars],1)
                pass
            elif xtype == "discrete":
                if "priorOf" in v:
                    g.prior = True
                    g.vars = [self.vars[v["priorOf"]]]
                else:
                    g.vars = [self.vars[x] for x in v["variables"]]
                g.dvars = g.vars[:]
                g.ddim = reduce(lambda x,y:x*y,[x.ddim for x in g.vars],1)
                g.gdim = 0
                pass
            elif xtype == "gaussian":
                if "priorOf" in v:
                    g.prior = True
                    g.vars = [self.vars[v["priorOf"]]]
                else:
                    g.vars = [self.vars[x] for x in v["variables"]]
                g.vars = [self.vars[x] for x in v["variables"]]
                g.gvars = g.vars[:]
                g.ddim = 0
                g.gdim = sum([x.gdim for x in g.vars])
                pass
            for v in g.vars:
                v.factors.append(g)
            self.facs[name] = g
            self.nodeid[id] = g
    def loadsched(self,s):
        self.sched = [(self.nodeid[int(a["src"])],self.nodeid[int(a["dst"])]) for a in s["list"]]

def emitgraph(q,o):
    import pydot
    g = pydot.Dot(graph_type="graph")
    for v in q.nodeid.values():
        # make pydot objects with minimal writings of attributes for readability
        n = pydot.Node(v.name,label="%s\n%s G%d/D%d" % (v.name,v.xtype,v.gdim,v.ddim),shape=isinstance(v,VariableNode) and "ellipse" or "box")
        # TODO cost
        g.add_node(n)
    for f in q.facs.values():
        # make pydot objects with minimal writings of attributes for readability
        for v in f.vars:
            e = pydot.Edge(f.name,v.name)
            g.add_edge(e)
    if o.endswith(".png"):
        g.write_png(o)
    else:
        fp = open(o,"wb")
        fp.write(g.to_string())
        fp.close()

def emitdotsched(tasks,o):
    # EMIT sched.py compatible graph
    import pydot
    g = pydot.Dot(graph_type="digraph")
    for t in tasks:
        # make pydot objects with minimal writings of attributes for readability
        n = pydot.Node(t.name)
        g.set("cost",str(t.cost))
        n.set("items",str(t.items))
        # TODO cost
        g.add_node(n)
    for t in tasks:
        for p in t.parents:
            e = pydot.Edge(p.name,t.name)
            g.add_edge(e)
    if o.endswith(".png"):
        g.write_png(o)
    else:
        fp = open(o,"wb")
        fp.write(g.to_string())
        fp.close()

def emitdot(tasks,o):
    import pydot
    g = pydot.Dot(graph_type="digraph")
    for t in tasks:
        # make pydot objects with minimal writings of attributes for readability
        if t.cost != 0:
            l = t.name+"\n%d" % t.cost
        else:
            l = t.name
        n = pydot.Node(t.name,label=l)
        g.set("cost",str(t.cost))
        # TODO cost
        g.add_node(n)
    for t in tasks:
        # make pydot objects with minimal writings of attributes for readability
        for p in t.parents:
            e = pydot.Edge(p.name,t.name)
            g.add_edge(e)
    if o.endswith(".png"):
        g.write_png(o)
    else:
        fp = open(o,"wb")
        fp.write(g.to_string())
        fp.close()


def transitivereduction(tasks):
    ancestors = dict()
    # build the ancestors of a node, being it a topologically sorted it is fine
    for t in tasks:
        q = set()
        for pa in t.parents:
            q = q | ancestors[pa.name]
            q.add(pa.name)
        ancestors[t.name] = q

    n = 0
    for t in tasks:
        qall = reduce(lambda x,y: x|y,[ancestors[p.name] for p in t.parents],set()) # ancestors not parents
        print "node pure ancestors",t.name,len(qall)
        before = len(t.parents)
        # remove a parent that is ancestor of some
        t.parents = [p for p in t.parents if not p.name in qall]
        after = len(t.parents)
        n += before-after
        if before != after:
            print "reduced",t.name,"of",before-after    
    return n
if __name__ == "__main__":
    import sys
    q = Pgm(sys.argv[1])
    for a,b in q.sched:
        print a,"->",b
    print q.vars
    print q.facs
    emitgraph(q,"graph.png")

    print "TODO: cleanup some messages"

    tasks = []
    lastof = dict()
    i = 1
    # add observations with enforcement of sequentiality in factors
    if False: # already done by schedule
        for v in q.vars.values():        
            if v.role == "observed":
                for f in v.adjacent():
                    t = Task("obs %s\nto %s" % (v.name,f.name),"observation",(v,f))
                    t.index = 1
                    w = lastof.get(f)
                    if w: 
                        t.addparent(w)
                    lastof[f] = t
                    tasks.append(t)
    for src,dst in q.sched:
        i = i + 1
        t = Task("","message",(src,dst))
        t.index = i
        if src == dst:
            print "UNSUPPORTED NL SELF MESSAGE"
            break
        if not src.seenout:
            # this is the first output of the factor, if it has received message we need to create the seenout task
            # that collects 
            if len(src.received) == 0:
                # no need to create seenout
                print src.name,dst.name,"sends without having received"
                pass
            elif len(src.received) == 1:
                src.seenouttask = src.received[0]
                print src.name,dst.name,"sends having received 1"
                src.seenout = True
            else:
                print src.name,dst.name,"sends needs collecting",len(src.received)
                tt = Task("%s" % src.name,"collect",None)
                tt.node = src
                tt.index = t.index
                tasks.append(tt)
                First = True
                for qq in src.received:
                    tt.addparent(qq)
                    if not First:
                        tt.cost += src.costaggregate(qq.message[0]) 
                    else:
                        First = False
                src.seenouttask = tt
                src.seenout = True
        dst.received.append(t)
        fobs = (isinstance(src,VariableNode) and src.role == "observed") and "OBS" or ""
        if src.seenouttask is not None:
            # src is an post message
            t.name = "%s*->%s %s #%d" % (src.name,dst.name,fobs,i)
            t.cost = src.costoutmessage(dst)
            t.addparent(src.seenouttask)
            lo = lastof.get(src)
            if lo and lo.index >= t.index:
                t.addparent(lo)
        else:
            t.name = "%s->%s %s #%d" % (src.name,dst.name,fobs,i)
            t.cost = dst.costinmessage(src)
            # chain the writing
            lo = lastof.get(src)
            if lo:
                t.addparent(lo)

        lastof[dst] = t
        tasks.append(t)

    for v in q.vars.values():
        if v.role == "regular":
            print "belief",v.name
            lo = lastof.get(v)
            t = Task("belief\n%s" % v.name,"belief",None)
            t.node = v
            t.index = i
            tasks.append(t)
            if lo:
                t.addparent(lo)
            lastof[v] = t

    toposorttasks(tasks)
    # NOTE: transitive reduction 
    n = transitivereduction(tasks)

    print "transitive reduction removed ",n

    emitdot(tasks,"out.dot")
    emitdot(tasks,"out.png")
    emitdotsched(tasks,"outsched.dot")

    #sort topologically
    for q in tasks:
        print q.name,q.role,
        if len(q.parents) != 0:
            print "parents:",[x.name for x in q.parents]
        else:
            print


