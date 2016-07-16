"""
CPR scheduling
Emanuele Ruffaldi, Scuola Superiore Sant'Anna 2016

TODO: edge cost not used for OPTIMIZING CPR (e.g. favouring same processor) BUT it is used in the final cost
TODO: find a case with earliest meaningful

TODO: compute ALAP The ALAP start-time of a node is a measure of how far the
nodes start-time can be delayed without increasing the schedule length: http://charm.cs.uiuc.edu/users/arya/docs/6.pdf

TODO: maybe static level = maximum path cost without the edge costs

"""
import sys
import heapq
import fractions
import argparse
import operator
from collections import OrderedDict
from functools import reduce as _reduce
import json

# global for controlling computation in fraction vs float
makenumbers = fractions.Fraction
# global for controlling number of cores (0=max)
defaultcore = 0 # ALL


class MTaskEdge:
    """Edge of a M-Task with edge cost"""
    def __init__(self,source,dest,cost):
        self.source = source # source MTask (parent of dest)
        self.dest = dest     # destination MTask (child of source)
        self.cost = makenumbers(cost)  # cost of the transfer

class MTask:
    """M-Task"""
    def __init__(self,id,cost,maxnp):
        self.id = id       # identifier
        self.parents = [] # inputs as MTaskEdge
        self.maxnp = maxnp # maximu number of processors (0=all)
        self.cost = makenumbers(cost)     # cost values

        # Computed FIXED propertirs
        self.sparents = set()  # tasks in parents
        self.children = []     # computed children
        self.top = 0        # t-level
        self.slevel = 0     # s-level (not counitng edge cost)
        self.bottom = 0     # b-level

        # Scheduling Results
        self.endtime = 0       # time of last proc running task
        self.earlieststart = 0 # time of effectiv first start among tasks in proc
        self.proc = set() # processors used
        self.Np = 0         # effective number of processor (request) == len(self.proc)
    def __repr__(self):
        return "MTask %s cost=%d top=%s bottom=%s slevel=%d parents=%s children=%s" % (self.id,self.cost,self.top,self.bottom,self.slevel,[t.id for t in self.sparents],[t.dest.id for t in self.children])

class Proc:
    """Processor allocation"""
    def __init__(self,index):
        self.index = index # number of the processor
        self.tasks = []    # (start,end,task) for the ones to be executed
        self.next = 0      # last task completed == self.tasks[-1][1]
        self.stasks = set() # task of which Proc contains all results
    def __repr__(self):
        if makenumbers == float:
            return "Proc(%d) ends %.2f tasks:\n%s" % (self.index,self.next,"\n".join(["\t%-6s [%.2f %.2f]" % (t.id,s,e,) for s,e,t in self.tasks]))
        else:
            return "Proc(%d) ends %s tasks:\n%s" % (self.index,self.next,"\n".join(["\t%-6s [%s %s]" % (t.id,s,e,) for s,e,t in self.tasks]))


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
    qd = dict([(t.id,t) for t in data]) # build the dictionary for reconstruction
    q = dict([(t.id,set([p.id for p in t.sparents])) for t in data]) # build dependency as list of id

    # back from id tho objects list
    return [qd[i] for i in toposort_flatten(q,sort=sort)]

def MLS(tasks,numCores,args):
    """Computes MLS"""
    proco = [Proc(i) for i in range(1,numCores+1)]
    procpq = []
    for p in proco:
        heapq.heappush(procpq,(0,p))

    ready = [] # list
    needed = set(tasks) # all needed
    done = set()
    running = [] # heap

    # clear schedule and add entry in ready list
    for t in tasks:
        t.proc = set()
        t.earlieststart = None
        t.endtime = None
        if len(t.sparents) == 0:
            heapq.heappush(ready,(0,t)) # for entries the earliest==bottom==0

    #Status of tasks:
    #   needed = tasks to be scheduled
    #   ready  = tasks ready to be schedule (parents OK)
    #   running = in execution (used for tracking running DEPENDENCY)
    #   done   = completed after running (used for DEPENDENCY)

    #print "!!starting with ready",len(ready),"and needed ",len(needed)
    while len(needed) > 0:        
        # if no more ready populate them picking from available in running
        while len(ready) == 0 and len(running) != 0:
            #print "need to fulfill some from running",len(running)
            # compute which is ready 
            justdonetime,justdone = heapq.heappop(running)
            done.add(justdone)
            for e in justdone.children:
                t = e.dest
                if len(set(t.sparents)-done) == 0: # TODO: improve efficiency of this
                    t.earlieststart = justdonetime # minimum time for this due to this LAST parent
                    # TODO: should we add the input costs?
                    #print "adding",t.id,"earliest",justdonetime
                    if args.earliest:
                        heapq.heappush(ready,(justdonetime,t)) # or priority, in any case i s0
                    else:
                        heapq.heappush(ready,(t.bottom,t)) # or priority, in any case i s0
                   
        # get highest priority
        pri,t = heapq.heappop(ready)
        needed.remove(t)

        # we cannot allocate due to the lack of available processors
        if t.Np > len(procpq):
            return 1e100,[]
        picked = [heapq.heappop(procpq) for i in range(0,t.Np)]
        
        # split cost due to parallelims, precompute all input transfers
        basecost = t.cost/makenumbers(len(picked))
        allinputcosts = sum([x.cost for x in t.parents])
        duration = allinputcosts + basecost

        for pnext,p in picked:
            if args.samezerocost:
                # per p duration depends on the edge transferts
                allinputcosts = 0
                for x in t.parents: # for all parents of t
                    if args.transitive and x.source in p.stasks:
                        if args.verbose:
                            print "transitive"
                        continue
                    if p in x.source.proc: # for all the k processors in which it has been split, if they contain p we can skip 1/k data transfer
                        if len(x.source.proc) > 1: #
                            allinputcosts += x.cost*makenumbers(len(x.source.proc)-1)/makenumbers(len(x.source.proc)) # all except p
                    else:
                        allinputcosts += x.cost # full cost being outside
                    p.stasks.add(x.source) # we are inglobating it for scheduling t
                duration = allinputcosts + basecost

            # compute execution range
            tstart = max(pnext,t.earlieststart)
            tend = tstart + duration
            
            p.tasks.append((tstart,tend,t)) # tstart-pnext IS flexibility
            p.next = tend # marks next available

            t.proc.add(p)
            heapq.heappush(procpq,(p.next,p))

        t.earlieststart = picked[0][1].tasks[-1][0]  # adjusted to reflect 
        t.endtime = picked[-1][1].tasks[-1][1]  

        heapq.heappush(running,(t.endtime,t)) # tend is the end of the last proc for task t

    # end time is max
    index, max_next = max(enumerate(proco),key=lambda p: p[1].next)
    return max_next.next,proco

def cpr(tasks,numCores,args):
    """Computes using CPR"""
    # clean assignments
    for t in tasks:
        t.Np = 1

    # build the ready pq using bottom (can be also earliest starting tiem)
    T,ta = MLS(tasks,numCores,args)
    Tchanged = True


    while Tchanged: # not modified
        Tchanged = False
        chi = [t for t in tasks if t.Np < numCores] # modifiable set
        while len(chi) > 0:   # PAPER: until T modified or chi empty
            index, max_value_ignored = max(enumerate([t.top+t.bottom for t in chi]), key=operator.itemgetter(1))
            t = chi[index]
            if t.Np == t.maxnp: #saturated
                del chi[index]
            else:
                t.Np += 1
                # try distribution using given unmber of cores for given processor
                Ti,tai = MLS(tasks,numCores,args)
                #print "tried upgrade of ",t.id," with ",t.Np," obtaining ",Ti,"vs previous",T
                if Ti < T:                
                    T = Ti
                    ta = tai
                    Tchanged = True
                else:
                    t.Np -= 1
                    del chi[index]
    return dict(schedule=ta,T=T)

def updatepriorities(schedule,tasks):
    """update priorities with effective schedule

    TODO use schedule
    TODO use change of processor cost
    TODO note that we need the decomposition level for the given task for affecting the edge cost
    """
    for t in tasks:
        if len(t.parents) == 0:
            t.top = 0
        else:
            t.top = max([p.source.top + p.source.cost + p.cost for p in t.parents])

    for t in tasks[::-1]:
        if len(t.children) == 0:
            t.bottom = t.cost
            t.slevel = t.cost
        else:
            t.bottom = max([p.dest.bottom + p.cost for p in t.children])+t.cost
            t.slevel = max([p.dest.slevel  for p in t.children])+t.cost


def annotatetasks(tasks):
    """compute children, top and bottom with no allocation"""
    for t in tasks:
        t.children = []
    for t in tasks:
        if len(t.parents) == 0:
            t.top = 0
        else:
            t.sparents = set([x.source for x in t.parents])
            t.top = max([p.source.top + p.source.cost + p.cost for p in t.parents])
            #t.top = max([p.top + p.cost for p in t.sparents])
        for p in t.parents:         
            p.source.children.append(p)

    for t in tasks[::-1]:
        if len(t.children) == 0:
            t.bottom = t.cost
            t.slevel = t.cost
        else:
            t.bottom = max([p.dest.bottom + p.cost  for p in t.children])+t.cost
            t.slevel = max([p.dest.slevel   for p in t.children])+t.cost


def loadtasksjson(fp):
    # array/dictionary of task with "id","cost","inputs"
    # inputs can be id of task or (id,cost)
    def makedge(x,d):
        if type(x) is list:
            return MTaskEdge(x[0],d,float(x[1]))
        else:
            return MTaskEdge(x,d,0)
    ts = []
    td = dict()
    j = json.load(fp)
    if type(j) == dict and "tasks" in j:
        j = j["tasks"]
    if type(j) == dict:
        # each a dictionary
        for id,ta in j.iteritems():
            t = MTask(id,ta.get("cost",1),ta.get("maxnp",defaultcore))
            ts.append(t)
            td[t.id] = t
        for id,ta in j.iteritems():
            me = ts[id]
            ts[id].parents = [makedge(td[x],me) for x in ta["inputs"]]
    else:
        # each is a list with 
        for ta in j:
            t = MTask(ta["id"],ta.get("cost",1),ta.get("maxnp",defaultcore))
            ts.append(t)
            td[t.id] = t
        for ta in j:
            if "inputs" in ta:
                me = td[ta["id"]]
                me.parents = [makedge(td[x],me) for x in ta["inputs"]]
    return ts

def loadtasksdot(fp):
    import pydot
    graphs = pydot.graph_from_dot_data(fp.read())
    (g2,) = graphs
    tasks = []
    tasksd = {}
    # if present use the attribute cost
    for n in g2.get_nodes():   
        ad =      n.get_attributes()
        #print n.get_name(),[a for a in ad]
        t = MTask(n.get_name(),float(ad.get("cost",1)),int(ad.get("maxnp",defaultcore)))
        tasks.append(t)
        tasksd[t.id] = t
    # if present use the attribute cost
    for e in g2.get_edges():
        #get_source
        #get_destination
        #get_attributes
        st = tasksd.get(e.get_source(),None)
        if st is None:
            st = MTask(e.get_source(),1,defaultcore)
            tasks.append(st)
            tasksd[st.id] = st
        dt = tasksd.get(e.get_destination(),None)
        if dt is None:
            dt = MTask(e.get_destination(),1,defaultcore)
            tasks.append(dt)
            tasksd[dt.id] = dt
        dt.parents.append(MTaskEdge(st,dt,float(e.get_attributes().get("cost",0))))

        #print e.get_source(),e.get_destination(),[a for a in e.get_attributes().iteritems()]
    return tasks


def analyzeschedule(schedule,tasks):
    """Analyzes Schedule for Errors"""
    avgs = []
    errors = 0
    #runs = []
    for p in schedule:
        last = 0
        slacks = []
        for b,e,t in p.tasks:
            slacks.append(b-last)
            last = e
            #heapq.heappush(runs,(b,(t,e,p.index)))
        if len(slacks) == 0: # unused
            continue
        avgs.append(sum(slacks)/len(slacks))

    for t in tasks:
        for s in t.sparents:
            if t.earlieststart < s.endtime:
                print "inversion for ",t.id," starts ",t.earlieststart," against ",s.id," ends ",s.endtime
                errors += 1

    return dict(avgslack=float(sum(avgs)/len(schedule)),used=len(avgs),errors=errors)
    
if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description='Task Scheduling for Multiprocessor - Emanuele Ruffaldi 2016 SSSA')
    parser.add_argument('--algorithm',default="cpr",help='chosen algorithm: cpr none')
    parser.add_argument('input',help="input file")  
    parser.add_argument('--cores',type=int,default=4,help="number of cores for the scheduling")
    parser.add_argument('--verbose',action="store_true")
    parser.add_argument('--earliest',action="store_true",help="uses earliest instead of bottom-level for the MLS")
    parser.add_argument('--usefloats',action="store_true",help="compute using floats instead of fractions")
    parser.add_argument('--allunicore',action="store_true",help="all tasks cannot be split")
    parser.add_argument('--samezerocost',action="store_true",help="skip edge cost for same processor edges")
    parser.add_argument('--transitive',action="store_true",help="transitive reduction (activates --samezerocost)")
    parser.add_argument('--output',help="JSON output of scheduling")

    args = parser.parse_args()

    if args.usefloats:
        makenumbers = float

    if args.transitive:
        args.samezerocost = True

    if args.allunicore:
        defaultcore = 1

    if args.input.endswith(".json"):
        tasks = loadtasksjson(open(args.input,"rb"))
    else:
        tasks = loadtasksdot(open(args.input,"rb"))

    # topological sort
    tasks = toposorttasks(tasks)

    # update structures and compute 
    annotatetasks(tasks)
    if args.verbose:
        for t in tasks:
            print t
    if args.algorithm == "cpr":
        r = cpr(tasks,args.cores,args)  
        e = analyzeschedule(r["schedule"],tasks)
        for p in r["schedule"]:
            print p
        print e
        print "Total",float(r["T"])
        if args.output:
            # TODO store in the output flags for the synchronization
            s = []
            for p in r["schedule"]:
                pp = []
                for b,e,t in p.tasks:
                    pp.append(dict(task=t.id,span=[float(b),float(e)],split=len(t.proc)))
                s.append(pp)
            j = dict(maxspan=float(r["T"]),schedule=s)
            if args.output == "-":
                fp = sys.stdout
            else:
                fp = open(args.output,"wb")
            json.dump(j,fp,sort_keys=True,indent=4, separators=(',', ': '))
    elif args.algorithm == "none":
        print "Tasks",len(tasks)
        for t in tasks:
            print t
    else:
        print "unknown algorithm",args.algorithm

