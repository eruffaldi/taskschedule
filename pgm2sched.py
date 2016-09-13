# TODO: prune graph/schedule of the leaf non ending in belief
#   e.g. in our case they are 
import json
from collections import defaultdict
from functools import reduce as _reduce
import re

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
    def __init__(self,name,role,node,index,message=None):
        self.name = name
        self.role = role # aggregate messages, message, belief, observation
        self.node = node # target 
        self.index = index
        self.message = message
        self.parents = set()
        self.children = set()
        self.cost = 0
        self.items = 1
        self.code = []
    def addchild(self,t):
        self.children.add(t)
        t.parents.add(self)
    def addparent(self,t):
        if t is not None:
            t.addchild(self)
    def __repr__(self):
        return "Task(%s,%s,#p%d,#c%d)" % (self.name,self.role,len(self.parents),len(self.children))

class Domain:
    def __init__(self,name,values):
        self.name = name
        self.values = values

class Node:
    def __init__(self,name,id):
        self.name = name
        self.id = id
        #for scheduling
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
    def isdiscrete(self):
        return self.xtype == "discrete"
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
        self.varlocation = {} # for each variable the location (offset or dimension)
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

def pgmtasks2code(pgm,tasks,outfile=None):
    if outfile is not None:
        out = open(outfile,"w")

    outs = []
    outm = set()
    outa = []
    for v in pgm.vars.values():
        # declare variables
        # declare the message stores
        d = max(v.gdim,v.ddim)
        outa.append(["variable",v.xtype,d,v.name])
    for f in pgm.facs.values():
        # declare factors, allocating indices of variables
        # declare the message stores
        outs.append(["factor",f.xtype,f.ddim,f.gdim,f.name])
    # then process based on tasks
    for t in tasks:   
        if t.role == "message":
            src,dst = t.message
            thevar = isinstance(src,VariableNode) and src or dst
            vad = max(thevar.ddim,thevar.gdim)
            fobs = (isinstance(src,VariableNode) and src.role == "observed")
            if fobs:
                outs.append(["T%03d" % t.index,"condition",thevar.xtype,vad,src.name,dst.name,"=>",dst.name])
            else:
                outm.add(("message",thevar.xtype,vad,src.name+"__"+dst.name))
                outm.add(("message",thevar.xtype,vad,dst.name+"__"+src.name))
                #make 1
                #1make msg if needed 
                outs.append(["T%03d" % t.index,"divide_marg_except",thevar.xtype,vad,src.name,src.name + "__" + dst.name,"=>",dst.name + "__" + src.name])

            #t.message = src,dst 
        elif t.role == "collect":
            #all parents are Tasks with messages 
            dst = t.node
            for p in t.parents:
                src = p.message[0]
                thevar = isinstance(src,VariableNode) and src or dst
                vad = max(thevar.ddim,thevar.gdim)
                outm.add(("message","thevar.xtype",vad,dst.name + "__" + src.name))
                outs.append(["T%03d" % t.index,"product_over",thevar.xtype,vad,dst.name,dst.name + "__" + src.name])

        elif t.role == "belief":
            outs.append(["T%03d" % t.index,"belief",t.node.name])
        else:
            print "unknonw task action"

    r = outa + list(outm) + outs
    if outfile is not None:
        out.write("\n".join([" ".join(str(y)) for y in r]))
    return r

def pgmsched2code(pgm,outfile=None):
    if outfile is not None:
        out = open(outfile,"w")

    outs = []
    outm = set()
    outa = []
    for v in pgm.vars.values():
        # declare variables
        # declare the message stores
        d = max(v.gdim,v.ddim)
        outa.append(["variable",v.xtype,d,v.name])
    for f in pgm.facs.values():
        # declare factors, allocating indices of variables
        # declare the message stores
        outs.append(["factor",f.xtype,f.ddim,f.gdim,f.name])
    # then process based on tasks
    i = 0;
    for src,dst in pgm.sched:
        i = i + 1;
        thevar = isinstance(src,VariableNode) and src or dst
        vad = max(thevar.ddim,thevar.gdim)
        fobs = (isinstance(src,VariableNode) and src.role == "observed")
        if fobs:
            outs.append(["T%03d" % i,"condition",thevar.xtype,vad,src.name,dst.name,"=>",dst.name])
        else:
            outm.add(("message",thevar.xtype,vad,src.name + "__" + dst.name))
            outm.add(("message",thevar.xtype,vad,dst.name + "__" + src.name))
            #make 1
            #1make msg if needed 
            outs.append(["T%03d" %i,"divide_marg_except",thevar.xtype,vad,src.name,src.name + "__" + dst.name,"=>",dst.name + "__" + src.name])
            i += 1
            outs.append(["T%03d" %i,"product_over",thevar.xtype,vad,dst.name,dst.name + "__" + src.name])
    for v in pgm.vars.values():
        # declare variables
        # declare the message stores
        if v.role == "regular":
            i += 1
            outs.append(["T%03d" %i,"belief",v.name])
    r = outa + list(outm) + outs
    if outfile is not None:
        out.write("\n".join([" ".join([str(w) for w in y]) for y in r]))
    return r

codebody = """
    #include "common.hpp"

    class $classname
    {
    public:
        $classname();
        void run();
    private:
$variables
$factors
$messages
$support
    };

    void $classname::run()
    {
$code
    }

    int main(int argc, char * argv[])
    {
        $classname p;
        $initsupport
        p.run();
        return  0;
    }
"""

def makevar(xtype,dim,name):
    if xtype == "gaussian":
        return "\t\tGaussianVariable var_%s = {\"%s\",%d};" % (name,name,dim)
    elif xtype == "discrete":
        return "\t\tDiscreteVariable var_%s = {\"%s\",DiscreteDomain(\"\",%d)};" % (name,name,dim);
def makefactor(xtype,dvars,gvars,name):
    #if gdim is None dim depends on ddim and type
    if gvars is None:
        gvars = dvars
    if xtype == "gaussian":
        return "\t\tLinearGaussianFactor %s = {\"%s\",{%s}};" % (name,name,",".join(gvars))
    elif xtype == "discrete":
        return "\t\tDiscreteFactor %s = {\"%s\",{%s}};" % (name,name,",".join(dvars));
    elif xtype == "gmm":
        return "\t\tMixtureGaussianFactor %s = {\"%s\",{%s},{%s}};" % (name,name,",".join(dvars),",".join(gvars));

def tasks2cpp(pgm,tasks,outfile):
    out = open(outfile,"w")
    # first process variables
    vars = []
    ovars =[]
    mvars = []
    messages = []
    factors = []
    code = []
    classname = "sched";
    outm = set()

    vars.append("// TODO DiscreteDomain declaration")
    for v in pgm.vars.values():
        d = max(v.gdim,v.ddim)
        vars.append(makevar(v.xtype,d,v.name))
        mvars.append(makefactor(v.xtype,["&var_"+v.name],None,"m_"+v.name))
        if v.role == "observed":
            if v.ddim != 0:
                ovars.append("\t\tint varobs_%s;" % v.name)
            else:
                ovars.append("\t\tEigen::VectorXd varobs_%s(%d);" % (v.name,d))
    vars.extend(mvars)
    vars.extend(ovars)
    for f in pgm.facs.values():
        factors.append(makefactor(f.xtype,["&var_"+x.name for x in f.dvars],["&var_"+x.name for x in f.gvars],"m_"+f.name))


    # TODO: associate code to task so we can decide scheduling
    # TODO scheduling code gen:
    #   - seq
    #   - openmp task (needs dependnecy var)
    #   - task runner
    #   - decoded task runner (needs barriers var)
    for t in tasks:
        if t.role == "message":
            src,dst = t.message
            thevar = isinstance(src,VariableNode) and src or dst
            vad = max(thevar.ddim,thevar.gdim)
            fobs = (isinstance(src,VariableNode) and src.role == "observed")
            if fobs:
                #outs.append(["T%03d" % t.index,"condition",thevar.xtype,vad,src.name,dst.name,"=>",dst.name])
                if src.xtype == "discrete":
                    t.code.append("\t\tm_%s.condition(varobs_%s,&var_%s);" % (dst.name,thevar.name,thevar.name))
                else:
                    t.code.append("\t\tm_%s.condition(varobs_%s,&var_%s);" % (dst.name,thevar.name,thevar.name))
            else:
                outm.add((thevar.xtype,vad,"m_"+src.name+"__"+dst.name,thevar))
                outm.add((thevar.xtype,vad,"m_"+dst.name+"__"+src.name,thevar))

                t.code.append("\t\tm_%s = (m_%s/m_%s).marg_except(&var_%s); // optimize me" % (dst.name+"__"+src.name,src.name,src.name+"__"+dst.name,thevar.name))
            #t.message = src,dst 
        elif t.role == "collect":
            #all parents are Tasks with messages 
            dst = t.node
            for p in t.parents:
                src = p.message[0]
                thevar = isinstance(src,VariableNode) and src or dst
                vad = max(thevar.ddim,thevar.gdim)
                outm.add((thevar.xtype,vad,"m_"+thevar.name,thevar))
                t.code.append("\t\tm_%s *= m_%s;" % (dst.name,dst.name + "__" + src.name))
        elif t.role == "belief":
            t.code.append("\t\tcompute_belief(v_%s,m_%s);" % (t.node.name,t.node.name))
        else:
            print "unknonw task action"

    for xtype,vad,name,thevar in outm:
        messages.append(makefactor(xtype,["&var_"+thevar.name],None,name))
        # TODO generate message

    # wrap: << for runner => add to support and then loading in initsupport
    # wrap: openmp task => define the dependency vars
    # wrap none


    #sequential
    for t in tasks:
        code.extend(t.code)
    rep = dict(classname="sched",messages="\n".join(messages),code="\n".join(code),factors="\n".join(factors),variables="\n".join(vars))
    rep = dict((re.escape("$"+k), v) for k, v in rep.iteritems())
    pattern = re.compile("|".join(rep.keys()))
    text = pattern.sub(lambda m: rep[re.escape(m.group(0))], codebody)
    out.write(text)
if __name__ == "__main__":
    import sys,argparse
    import argparse
    #parser = argparse.ArgumentParser(description='Task Scheduling for Multiprocessor - Emanuele Ruffaldi 2016 SSSA')
    #args = parser.parse_args()

    pgm = Pgm(sys.argv[1])
    for a,b in pgm.sched:
        print a,"->",b
    print pgm.vars
    print pgm.facs
    emitgraph(pgm,"graph.png")

    print "TODO: cleanup some messages"

    tasks = []
    lastof = dict()
    i = 0
    # add observations with enforcement of sequentiality in factors
    if False: # already done by schedule
        for v in pgm.vars.values():        
            if v.role == "observed":
                for f in v.adjacent():
                    i += 1
                    t = Task("obs %s\nto %s" % (v.name,f.name),"observation",message=(v,f),node=f,index=i)
                    t.addparent(lastof[f])
                    lastof[f] = t
                    tasks.append(t)
    collects = defaultdict(lambda:1)
    for src,dst in pgm.sched:
        if src == dst:
            print "UNSUPPORTED NL SELF MESSAGE"
            break
        if len(src.received) == 0:
            # no need to create seenout
            print src.name,dst.name,"sends without having received"
            pass        
        # TODO if there is just one then make the collect contextual to the previous: messageapply instead of message(make)
        else:
            print src.name,"sends needs collecting",len(src.received)
            cid = collects[src.name]
            collects[src.name] = cid + 1
            i += 1
            tt = Task("%s collect#%d" % (src.name,cid),"collect",node=src,index=i)
            tasks.append(tt)
            First = True
            for qq in src.received:
                tt.addparent(qq)
                if not First:
                    tt.cost += src.costaggregate(qq.message[0]) 
                else:
                    First = False
            tt.addparent(lastof.get(src)) # PROBABLY the one of the above addparent
            lastof[src] = tt
            src.received = []

        fobs = (isinstance(src,VariableNode) and src.role == "observed") and "OBS" or ""
        i += 1
        t = Task("%s->%s %s #%d" % (src.name,dst.name,fobs,i),"message",message=(src,dst),index=i,node=dst)
        t.cost = dst.costinmessage(src)
        t.addparent(lastof.get(dst)) # needed for combining the output of dst
        t.addparent(lastof.get(src)) # needed for using src (should be the just created collect above)
        tasks.append(t)

        dst.received.append(t)
        lastof[dst.name] = t

    for v in pgm.vars.values():
        if v.role == "regular":
            if len(v.received) > 0:
                print "post execution collect",v.name,len(v.received)
                i += 1
                tt = Task("%s collect final" % v.name,"collect",node=v,index=i)
                tasks.append(tt)
                tt.addparent(lastof.get(v))
                First = True
                for tre in v.received:
                    tt.addparent(tre)
                    if not First:
                        tt.cost += v.costaggregate(tre.message[0]) 
                    else:
                        First = False                
                lastof[v]  = tt
            i += 1
            t = Task("belief\n%s" % v.name,"belief",node=v,index=i)
            tasks.append(t)
            t.addparent(lastof.get(v))
            lastof[v] = t

    try:
        toposorttasks(tasks)
        # NOTE: transitive reduction 
        n = transitivereduction(tasks)
        print "transitive reduction removed ",n
    except:
        pass


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

    pgmtasks2code(pgm,tasks,"outtask.txt")
    pgmsched2code(pgm,"outsched.txt")
    tasks2cpp(pgm,tasks,"outtask.cpp")

