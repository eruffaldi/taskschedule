"""
CPR scheduling
Emanuele Ruffaldi, Scuola Superiore Sant'Anna 2016

TODO: edge cost not used for OPTIMIZING CPR (e.g. favouring same processor) BUT it is used in the final cost
TODO: find a case with earliest meaningful

TODO: compute ALAP The ALAP start-time of a node is a measure of how far the
nodes start-time can be delayed without increasing the schedule length: http://charm.cs.uiuc.edu/users/arya/docs/6.pdf

TODO: maybe static level = maximum path cost without the edge costs

TODO: use NetworkX for transitive reduction http://www.boost.org/doc/libs/1_52_0/libs/graph/doc/transitive_closure.html
TODO: use NetworkX for topological sort https://networkx.readthedocs.io/en/stable/reference/generated/networkx.algorithms.dag.topological_sort.html
"""
import sys,math
import heapq
import fractions
import argparse
import operator
import itertools
from collections import OrderedDict
from functools import reduce as _reduce
import json
import operator

try:
    from pulp import *
    haspulp = True
except:
    haspulp = False
def forall(u,op):
    for x in u:
        op(x)


# global for controlling computation in fraction vs float
makenumbers = fractions.Fraction

defaultcore = 0

class MTaskEdge:
    """Edge of a M-Task with edge cost

    Is reduction means that the destination task is a reduction task of a data-parallel map-reduction task. This means that if the map-reduction task has
    Np == 1 then we can consider the reduction task as 0 cost and skippable

    When instead we split the map-reduction we need to 
    """
    def __init__(self,source,dest,delay,isreduction=False):
        self.source = source # source MTask (parent of dest)
        self.dest = dest     # destination MTask (child of source)
        self.delay = makenumbers(delay)  # cost of the transfer
        self.isreduction = isreduction

class MTask:
    deadlinemaxtime = 10000000
    """M-Task"""
    def __init__(self,id,cost,items,maxnp,deadline=deadlinemaxtime,reductioncost=0,evennp=False):
        self.id = id       # identifier
        self.parents = [] # inputs as MTaskEdge
        self.items = items
        self.maxnp = maxnp # maximu number of splits (0=all means infinitely splittable!)
        if self.maxnp == 0 or self.maxnp > self.items:
            self.maxnp = self.items
        self.cost = makenumbers(cost)     # cost values (should be evenly divisible by maxnp if maxnp is not null)
        self.deadline = makenumbers(deadline)
        self.reductioncost = reductioncost
        self.doesreduction = False
        self.evennp = evennp

        # Computed FIXED propertirs
        self.sparents = set()  # tasks in parents
        self.children = []     # computed children
        self.top = 0        # t-level
        self.slevel = 0     # s-level (not counitng edge cost)
        self.bottom = 0     # b-level
        self.cdeadline = deadline  # children earliest deadline

        # Scheduling Results
        self.endtime = 0       # time of last proc running task
        self.earlieststart = 0 # time of effective first start among tasks in proc
        self.lateststart = 0
        self.proc = set() # processors used
        self.ucost = self.cost # self.cost/self.Np
        self.Np = 1         # effective number of processor (request) == len(self.proc)
    def allproc(self):
		return set([p.proc for p in self.proc])
		
    def __repr__(self):
        return "MTask %s cost=%s top=%s bottom=%s slevel=%d items=%d maxnp=%d early/late=%s %s deadline/c=%s %s parents=%s children=%s" % (self.id,str(self.cost),self.top,self.bottom,self.slevel,self.items,self.maxnp,self.earlieststart,self.lateststart,self.deadline,self.cdeadline,[t.id for t in self.sparents],[t.dest.id for t in self.children])
    def updateNp(self,n):
        print "updateNp for ",self.id,self.Np,n
        self.Np = n
        self.ucost = self.cost / self.Np
        for c in self.children:
            if c.isreduction:
                c.dest.maxnp = 1 # 1 processor
                if n == 1:
                    # remove it
                    print "Deselected Reduction",c.dest.id,"with cost",c.dest.cost
                    c.dest.items = 0
                    c.dest.cost = 0
                    c.dest.ucost = 0
                    c.dest.doesreduction = False
                else:
                    # does n but without parallelism
                    c.dest.items = n # n items
                    unitaryreductioncost = self.reductioncost / makenumbers(self.items)
                    print "Selected Reduction",c.dest.id,"with cost",c.dest.cost
                    c.dest.cost = unitaryreductioncost * n
                    c.dest.ucost = c.dest.cost
                    c.dest.doesreduction = True
    def setOneRun(self,tstart):
        self.earlieststart = tstart # time of effectiv first start among tasks in proc
        self.lateststart = tstart
        self.endtime = self.cost + tstart      # time of last proc running task
        self.Np = 1
        self.ucost = self.cost
        self.proc = set()

# use of task by Processor, it was (s,e,t) but we need also slicing
class ProcTask:
    def __init__(self,proc,task,begin,end):
        self.proc = proc
        self.task = task
        self.begin = begin
        self.end = end
        self.rangesplit = (0,0,0) # (istart,iend,target) for parallel
class Proc:
    """Processor allocation"""
    def __init__(self,index):
        self.index = index # number of the processor
        self.tasks = []    # ProcTask for the ones to be executed
        
        # Temporary
        self.next = 0      # last task completed == self.tasks[-1][1]
        self.stasks = set() # task of which Proc contains all results (having executed or received)
    def addTask(self,task,begin,end):
        pt = ProcTask(self,task,begin,end)
        task.proc.add(pt)
        self.tasks.append(pt)
        self.next = max(task.endtime,self.next)
        self.stasks.add(task)
    def clear(self):
        self.tasks = []
        self.next = 0
        self.stasks = set()
    def __repr__(self):
        if makenumbers == float:
            return "Proc(%d) ends %.2f tasks:\n%s" % (self.index,self.next,"\n".join(["\t%-6s [%.2f %.2f, %d-%d %d]" % (q.task.id,q.begin,q.end,q.rangesplit[0],q.rangesplit[1],q.rangesplit[2]) for q in self.tasks]))
        else:
            return "Proc(%d) ends %s tasks:\n%s" % (self.index,self.next,"\n".join(["\t%-6s [%s %s, %d-%d %d]" % (q.task.id,q.begin,q.end,q.rangesplit[0],q.rangesplit[1],q.rangesplit[2]) for q in self.tasks]))


def transitivereduction(tasks,updatechildren):
    ancestors = dict()
    # build the ancestors of a node, being it a topologically sorted it is fine
    for t in tasks:
        q = set()
        for pa in t.parents:
            q = q | ancestors[pa.source.id]
            q.add(pa.source.id)
        ancestors[t.id] = q
        if updatechildren:
            t.children = []

    for t in tasks:
        qall = reduce(lambda x,y: x|y,[ancestors[p.source.id] for p in t.parents],set()) # ancestors not parents
        before = len(t.parents)
        # remove a parent that is ancestor of some
        t.parents = [p for p in t.parents if not p.source.id in qall]
        if updatechildren:
            for q in t.parents:
                q.source.children.append(t)



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
    q = dict([(t.id,set([p.source.id for p in t.parents])) for t in data]) # build dependency as list of id

    # back from id tho objects list
    return [qd[tid] for tid in toposort_flatten(q,sort=sort)]

def recomputetaskproc(schedule,tasks):
    """Recomputes some values per-task after the approval of a schedule"""
    # first build the correct list of ProcTask per task
    for t in tasks:
        t.proc = set()
    for p in schedule:
        for q in p.tasks:
            q.task.proc.add(q)

    # then update the activation times and distribute the slices in the case of paralle tasks
    for t in tasks:
        t.Np = len(t.proc)
        if t.Np == 0:
            # strange no allocation
            t.earlieststart = 0
            t.endtime = 0
            t.lateststart = 0
        else:
            t.earlieststart = min([q.begin for q in t.proc])
            t.lateststart   = max([q.begin for q in t.proc])
            t.endtime = max([q.end for q in t.proc])
            if t.Np == 1:
                # just one for all
                list(t.proc)[0].rangesplit = (0,t.items,0)
            elif t.items > 0:                
                # we have a data-parallel task split over t.proc
                k = int(math.ceil(t.items/t.Np))
                z = list(t.proc)
                i0 = 0
                for i in range(0,len(t.proc)):
                    z[i].rangesplit = (i0,i0+k,i)
                    i0 += k
                # NOTE: with k we overstimate, so we could have some excess in the last. We do not take this into account
                z[-1].rangesplit = (z[-1].rangesplit[0],t.items,z[-1].rangesplit[2])
            else:
                # unbounded paralle tasks assign 1 slot to every processor REMOVED
                sys.exit(0)
                pass

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
        t.lateststart = None
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
                    # minimum time for this due to this LAST parent
                    # BUT we can start on ANY processor
                    t.earlieststart = justdonetime 
                    t.lateststart = justdonetime
                    if args.earliest:
                        heapq.heappush(ready,((t.cdeadline,justdonetime),t)) # or priority, in any case i s0
                    else:
                        heapq.heappush(ready,((t.cdeadline,t.bottom),t)) # or priority, in any case i s0
                   
        # get highest priority
        pri,t = heapq.heappop(ready)
        needed.remove(t)




        # AVOID load balancing in multiprocessor FAVOR affinity
        if args.notfavoraffinity:
            if t.Np > len(procpq):
                return None,[]
            picked = [heapq.heappop(procpq) for i in range(0,t.Np)]
        else:
            if t.Np > len(proco):
                return None,[]
            allp = []

            
            for p in proco:
                ee = max(p.next,t.earlieststart)
                ees = [ee]
                epp = set() # number of depending processors
                for te in t.parents: # TaskEdge
                    if te.source in p.stasks:
                        continue # result already transferred, NO DELAY
                    else:
                        pap = set([q.proc for q in te.source.proc])
                        parentNp = makenumbers(len(pap))

                        # we need to WAIT arrival
                        k = 1/parentNp
                        ees.append([q.next + te.delay*k for q in pap])

                        # epp contains all the list of predecessors
                        epp = epp | pap
                ee = max(ees)
                if p in epp:
                    epp.remove(p)
                allp.append((ee,p,len(epp)))

            allp.sort(key=lambda x: (x[0],x[2]))
            picked = [x[0:2] for x in allp[0:t.Np]]

        for ignored,p in picked:
            tstart = max(p.next,t.earlieststart)

            # we need to adjust tstart depending on delays
            for te in t.parents: # TaskEdge
                pa = te.source
                if pa in p.stasks: # result of source already in stasks
                    continue
                else:
                    # tstart depends on the maximum availability
                    w = [po.proc.next + te.delay/pa.Np for po in pa.proc if po.proc != p]
                    if len(w) > 0:
                        print "adjust tstart",tstart,w
                        tstart = max(tstart,max(w))

                p.stasks.add(pa) # now this processor has the result of parent

            # compute execution range
            tend = tstart + t.ucost
            
            # create allocation
            q = ProcTask(p,t,tstart,tend)
            p.tasks.append(q) # tstart-p.next IS flexibility
            p.next = tend # marks next available
            t.proc.add(q)
            if t.Np == 1:
                p.stasks.add(t) # contains all the result having it computed

            if args.notfavoraffinity:
                heapq.heappush(procpq,(p.next,p))

        t.earlieststart = picked[0][1].tasks[-1].begin  # adjusted to reflect 
        t.lateststart = picked[-1][1].tasks[-1].begin   # adjusted to reflect 
        t.endtime = picked[-1][1].tasks[-1].end

        # fail
        if t.deadline < MTask.deadlinemaxtime and t.lateststart > t.deadline:
            return None,[]

        heapq.heappush(running,(t.endtime,t)) # tend is the end of the last proc for task t

    # end time is max
    index, max_next = max(enumerate(proco),key=lambda p: p[1].next)
    return max_next.next,proco

#ftp://ftp.keldysh.ru/K_student/AUTO_PARALLELIZATION/TASK/a-low-cost-approach.pdf
def cpa(tasks,numCores,args):
    """Computes using CPR"""
    # clean assignments
    for t in tasks:
        t.updateNp(1)

    # build the ready pq using bottom (can be also earliest starting tiem)
    T,ta = MLS(tasks,numCores,args)
    Tchanged = True

    if T is None:
        return dict(schedule=[],T=0)


    while Tchanged: 
        Tchanged = False
        chi = [t for t in tasks if t.Np < min(this.maxnp,numCores)]
        #TCP = max TB(v)
        #TA = 1/P sum Tcost(v,Np(v))*Np(v)
        while TCP > TA:
            # optimal t in CP with Tc(t,Np(t))/Np-Tc(t,Np(t)+1)/(Np+1)
            #   this is a t in CP that maximizes the gain
            # schedule anyway
            # update TL TB
            index, max_value_ignored = max(enumerate([t.top+t.bottom for t in chi]), key=operator.itemgetter(1))
            t = chi[index]
            n = t.Np
            if t.evennp:
                # 1,2,... all even
                if t.Np == 1:
                    n = 2
                else:
                    n = t.Np + 2 
            else:
                n = t.Np+1
            if n > t.maxnp: #saturated
                del chi[index]
            else:
                oldn = t.Np
                t.updateNp(n)
                # try distribution using given unmber of cores for given processor
                Ti,tai = MLS(tasks,numCores,args)
                #print "tried upgrade of ",t.id," with ",t.Np," obtaining ",Ti,"vs previous",T
                if Ti is not None and Ti < T:     # NOTE: None < x for any x != None           
                    T = Ti
                    ta = tai
                    Tchanged = True
                    # TODO update TA
                    # TODO update TB and TL => new critical path => new TCP
                else:
                    # failed
                    t.updateNp(oldn)
                    del chi[index]
    # remove processors
    ta = [p for p in ta if len(p.tasks) > 0]
    recomputetaskproc(ta,tasks)
    return dict(schedule=ta,T=T)

def cpr(tasks,numCores,args):
    """Computes using CPR"""
    # clean assignments
    for t in tasks:
        t.updateNp(1)

    # build the ready pq using bottom (can be also earliest starting tiem)
    T,ta = MLS(tasks,numCores,args)
    Tchanged = True

    if T is None:
        return dict(schedule=[],T=0)


    # CPR restarts the LOOP when 
    while Tchanged: # not modified
        Tchanged = False
        chi = [t for t in tasks if t.Np < min(t.maxnp,numCores)] # extensible processors
        while len(chi) > 0: #and not Tchanged:    # PAPER says not Tchanged
            # TODO: make this a priority queue
            index, max_value_ignored = max(enumerate([t.top+t.bottom for t in chi]), key=operator.itemgetter(1))
            t = chi[index]
            n = t.Np
            if t.evennp:
                # 1,2,... all even
                if t.Np == 1:
                    n = 2
                else:
                    n = t.Np + 2 
            else:
                n = t.Np+1
            if n > t.maxnp: #saturated
                del chi[index]
            else:
                oldn = t.Np
                t.updateNp(n)
                # try distribution using given unmber of cores for given processor
                Ti,tai = MLS(tasks,numCores,args)
                #print "tried upgrade of ",t.id," with ",t.Np," obtaining ",Ti,"vs previous",T
                if Ti is not None and Ti < T:     # NOTE: None < x for any x != None           
                    T = Ti
                    ta = tai
                    Tchanged = True

                    # TODO update TB and TL
                else:
                    # failed
                    t.updateNp(oldn)
                    del chi[index]
    # remove processors
    ta = [p for p in ta if len(p.tasks) > 0]
    recomputetaskproc(ta,tasks)
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
            t.top = max([p.source.top + p.source.ucost + p.delay for p in t.parents])

    for t in tasks[::-1]:
        if len(t.children) == 0:
            t.bottom = t.ucost
            t.slevel = t.ucost
            t.cdeadline = t.deadline
        else:
            t.bottom = max([p.dest.bottom + p.delay for p in t.children])+t.ucost
            t.slevel = max([p.dest.slevel  for p in t.children])+t.ucost            
            # move back the cdeadline (if any) of each child by the transfer cost and sum up my cost
            cd = [p.dest.cdeadline-p.delay for p in t.children if p.dest.cdeadline < MTask.deadlinemaxtime]
            if len(cd) == 0:
                t.cdeadline = t.deadline
            else:
                # move it back using my current cost, and anyway later than my deadline (if any)
                t.cdeadline = min(t.deadline,min(cd)-t.ucost)


def annotatetasks(tasks):
    """compute children, top and bottom with no allocation"""
    for t in tasks:
        t.children = []
    for t in tasks:
        if len(t.parents) == 0:
            t.top = 0
        else:
            t.sparents = set([x.source for x in t.parents])
            t.top = max([p.source.top + p.source.ucost + p.delay for p in t.parents])
            #t.top = max([p.top + p.cost for p in t.sparents])
        for p in t.parents:         
            p.source.children.append(p)

    for t in tasks[::-1]:
        if len(t.children) == 0:
            t.bottom = t.ucost
            t.slevel = t.ucost
            t.cdeadline = t.deadline
        else:
            t.bottom = max([p.dest.bottom + p.delay  for p in t.children])+t.ucost
            t.slevel = max([p.dest.slevel   for p in t.children])+t.ucost
            # move back the cdeadline (if any) of each child by the transfer cost and sum up my cost
            cd = [p.dest.cdeadline-p.ucost for p in t.children if p.dest.cdeadline < MTask.deadlinemaxtime]
            if len(cd) == 0:
                t.cdeadline = t.deadline
            else:
                # move it back using my current cost, and anyway later than my deadline (if any)
                t.cdeadline = min(t.deadline,min(cd)-t.ucost)

def savetasksjson(tasks,fp):
    ot = []
    for t in tasks:
        oti = dict(id=t.id,cost=str(t.cost),inputs=[])
        if t.items != 1:
            oti["items"] = t.items
        if t.maxnp != defaultcore:
            oti["maxnp"] = t.maxnp
        if t.deadline != MTask.deadlinemaxtime:
            oti["deadline"] = t.deadline
        if t.reductioncost != 0:
            oti["reductioncost"] = t.reductioncost
        if t.evennp:
            oti["evennp"] = True
        for p in t.parents:
            if p.delay == 0 and not p.isreduction:
                oti["inputs"].append(p.source.id)
            else:
                oti["inputs"].append(dict(source=p.source.id,delay=str(p.delay),reduction=p.isreduction and 1 or 0))
        ot.append(oti)
    json.dump(dict(tasks=ot),fp,sort_keys=True,indent=4, separators=(',', ': '))


def loadtasksjson(fp):
    # array/dictionary of task with "id","cost","inputs"
    # inputs can be id of task or (id,cost)
    def makedge(x,d):
        if type(x) is list:
            # (source,delay,[isreduction])
            return MTaskEdge(x[0],d,float(x[1]),len(x) > 2 and int(x) != 0 or 0)
        elif type(x) is dict:
            return MTaskEdge(x["source"],d,x["delay"],x.get("reduction",False))
        else:
            return MTaskEdge(x,d,0)
    ts = []
    td = dict()
    j = json.load(fp)
    if type(j) == dict and "tasks" in j:
        j = j["tasks"]

    alli = {}
    if type(j) == dict:        
        for id,ta in j.iteritems():
            alli[id] = ta
    else:
        for ta in j:
            alli[ta["id"]] = ta

    for id,ta in alli.iteritems():
        t = MTask(id,ta.get("cost",1),ta.get("items",1),ta.get("maxnp",defaultcore),ta.get("deadline",MTask.deadlinemaxtime),ta.get("reductioncost",0),evennp=ta.get("evennp"))
        ts.append(t)
        td[t.id] = t

    # edges solved to objects (second pass)
    for id,ta in alli.iteritems():
        if "inputs" in ta:
            me = td[id]
            me.parents = [makedge(td[x],me) for x in ta["inputs"]]

    return ts

def savetasksdot(tasks,fp):
    import pydot
    g = pydot.Dot(graph_type="digraph")
    for t in tasks:
        # make pydot objects with minimal writings of attributes for readability
        n = pydot.Node(t.id)
        n.set("cost",str(t.cost))
        if t.items != 1:
            n.set("items",t.items)
        if t.maxnp != defaultcore:
            n.set("maxnp",t.maxnp)
        if t.deadline != MTask.deadlinemaxtime:
            n.set("deadline",t.deadline)
        if t.reductioncost != 0:
            n.set("reductioncost",t.reductioncost)
        if t.evennp:
            n.set("evennp",1)
        g.add_node(n)
    for t in tasks:
        for te in t.parents:
            # make pydot objects for edges
            e = pydot.Edge(te.source.id,t.id)
            if te.delay != 0 or te.isreduction:
                e.set("delay",te.delay)
                e.set("reduction",te.isreduction and 1 or 0)
            g.add_edge(e)
    fp.write(unicode(g.to_string()))

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
        t = MTask(n.get_name(),float(ad.get("cost",1)),int(ad.get("items",1)),int(ad.get("maxnp",defaultcore)),int(ad.get("deadline",MTask.deadlinemaxtime)),float(ad.get("reductioncost",0)),int(ad.get("evennp",0)) != 0)
        tasks.append(t)
        tasksd[t.id] = t
    # if present use the attribute cost
    for e in g2.get_edges():
        #get_source
        #get_destination
        #get_attributes
        st = tasksd.get(e.get_source(),None)
        if st is None:
            st = MTask(e.get_source(),1,1,defaultcore)
            tasks.append(st)
            tasksd[st.id] = st
        dt = tasksd.get(e.get_destination(),None)
        if dt is None:
            dt = MTask(e.get_destination(),1,1,defaultcore)
            tasks.append(dt)
            tasksd[dt.id] = dt
        dt.parents.append(MTaskEdge(st,dt,float(e.get_attributes().get("delay",0)),float(e.get_attributes().get("reduction",0))))

        #print e.get_source(),e.get_destination(),[a for a in e.get_attributes().iteritems()]
    return tasks

def drawsched(name,schedule,tasks,sembegin,semcount,nolabels):
    # draw the make span horizontally (#proc < #tasks)
    import cairo
    nproc = len(schedule)
    if nproc > 0:
        maxspan = max([x.next for x in schedule])
        minspan = min([len(p.tasks) > 0 and min([q.end-q.begin for q in p.tasks if q.task.ucost > 0])  or 100000000 for p in schedule])
        maxtext = max([len(p.tasks) > 0 and max([nolabels and 1 or len(str(q.task.id)) for q in p.tasks if q.task.ucost > 0]) or 0  for p in schedule])
    else:
        maxspan = 0
        minspan = 0
        maxtext = 0
    pheight = 20
    pymargin = 2
    pxmargin = 2
    fontsize = 12
    height = int((pheight+pymargin)*(nproc+1))
    # minimum (for text) should be 50 pixels
    if minspan != 0:
        timescale = int((maxtext*fontsize+20)/minspan)
    else:
        timescale =1 
    timescale = max(timescale,1)
    width = int(maxspan*timescale+pxmargin*2)

    print "draw nproc",nproc," timescale",timescale,"maxtext",maxtext,"minspan",minspan

    svgmode = name.endswith(".svg")
    if svgmode:
        surface = cairo.SVGSurface (name,width, height)
    else:
        surface = cairo.ImageSurface (cairo.FORMAT_ARGB32, width, height)
    cr = cairo.Context (surface)
    #ctx.scale (WIDTH, HEIGHT) # Normalizing the canvas

    cr.set_line_width(pxmargin)
    cr.select_font_face("Georgia", cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
    cr.set_font_size(fontsize)
    #(x, y, w, h, dx, dy) = cr.text_extents(maxtext*"O")
    #print w

    py = pheight/2
    taskbeginend = dict()
    deps = []
    for p in schedule:
        for q in p.tasks:
            e = q.end
            b = q.begin
            t = q.task
            if t.ucost == 0:
                continue
            durwidth = float((e-b)*timescale)
            durwidth -= pxmargin
            bx = float(b*timescale+pxmargin)
            cr.rectangle(bx, py, durwidth, pheight)
            cr.set_source_rgb(0, 0, 0)
            cr.stroke_preserve()
            cr.set_source_rgb(1.0, 1.0, 1.0)
            cr.fill()
            cr.set_source_rgb(0, 0, 0)
            if not nolabels:
                s = str(t.id)
                (x, y, w, h, dx, dy) = cr.text_extents(s)
                cr.move_to(bx+durwidth/2-w/2, py+pheight/2-h/2-y)
                cr.show_text(s)
            # all processors to be waited except this processor
            #allp = _reduce(operator.or_,[set([q.proc for q in x.source.proc]) for x in t.parents],set())
            #allp = allp - set([p])
            si = sembegin.get(t.id)
            if si is None:
                nsem = 0
            else:
                nsem = semcount[si]

            # nothing to be wait for
            if nsem == 0:
                mode = 1
            # one single semafore
            elif nsem == 1:
                mode = 2
                c = (1.0,1.0,0)
            # more than one
            else:
                mode = 3
                c = (1.0,0,0)
            if mode > 1:
                cr.rectangle(bx, py+pheight/2-pheight/8.0, pheight/4.0,pheight/4.0)
                cr.set_source_rgb(0, 0, 0)
                cr.stroke_preserve()
                cr.set_source_rgb(*c)
                cr.fill()

            taskbeginend[t.id] = (bx,bx+durwidth,py+pheight/2) # startx,endx,centerheight
            if t.deadline < MTask.deadlinemaxtime and t.lateststart > t.deadline:
                cr.arc(bx+pheight/8.0, py+pheight/8.0, pheight/8.0,0,2*math.pi)
                cr.set_source_rgb(0, 0, 0)
                cr.stroke_preserve()
                cr.set_source_rgb(1.0,0,0)
                cr.fill()
            if args.drawedges:
                for pa in t.parents:
                    deps.append((pa.source,t))
        py += pheight+pymargin
    if len(deps) > 0:
        print "generating deps"
    cr.set_line_width (0.5)
    cr.set_source_rgb(0,0.0,0.0)
    for source,target in deps:
        psource = taskbeginend.get(source.id)
        ptarget = taskbeginend.get(target.id)
        if psource is None:
            print "missing source",source
            continue
        # exclude same processor
        if(psource[2] != ptarget[2]):
            cr.move_to(psource[1],psource[2])
            cr.line_to(ptarget[0],ptarget[2])
        cr.stroke()
    if not svgmode:
        surface.write_to_png (name) # Output to PNG



def analyzeschedule(schedule,tasks):
    """Analyzes Schedule for Errors"""
    if len(schedule) == 0:
        return dict(avgslack=0,used=0,errors=0,deadlineerrors=0)

    avgs = []
    deadlineerrors = 0
    errors = 0
    #runs = []
    for p in schedule:
        last = 0
        slacks = []
        for q in p.tasks:
            slacks.append(q.begin-last)
            last = q.end
            #heapq.heappush(runs,(b,(t,e,p.index)))
        if len(slacks) == 0: # unused
            continue
        avgs.append(sum(slacks)/len(slacks))

    for t in tasks:
        if len(t.proc) == 0:
            print "not computed",t.id
        if t.deadline < MTask.deadlinemaxtime and t.deadline-t.lateststart < 0: # NOT t.deadline < t.lateststart
            print "missed deadline"
            deadlineerrors += 1
        for s in t.sparents:
            if t.earlieststart < s.endtime:
                print "inversion for ",t.id," starts ",t.earlieststart," against ",s.id," ends ",s.endtime
                errors += 1

    return dict(avgslack=float(sum(avgs)/len(schedule)),used=len(avgs),errors=errors,deadlineerrors=deadlineerrors)

def defmax(a,default):
    if len(a) == 0:
        return default
    else:
        return max(a)
def loadsched(f):
    pass

def loadtasks(f,dotransitive=False):
    if f.endswith(".json"):
        tasks = loadtasksjson(open(f,"rb"))
    else:
        tasks = loadtasksdot(open(f,"rb"))
    tasks = toposorttasks(tasks)
    #OPTIONAL
    if dotransitive:
        transitivereduction(tasks,updatechildren=False)
    # update structures and compute 
    annotatetasks(tasks)
    return tasks

# dynamic mode is a representation of the pure graph with dependencies
# essentially it is a breadth first visit of the graph from the toplevel
# the number of processors is the largest size of the BFS
def dyn(tasks,P,args):
    print "dyn not implemented"
    aset = queue([t for t in tasks if len(t.parents) == 0])
    pass

def xpulp(tasks,P,args):
    """
    Towards the Optimal Solution of the Multiprocessor Scheduling Problem with Communication Delays, Davidovic
    Using: Uang Cheung (2004), The berth allocation problem: models and solution methods. 
    """
    if not haspulp:
        print "missing pulp"
        return None

    print "pulp has no DATA-PARALLELISM or REDUCTION"

    N = len(tasks)
    alli  = range(1,N+1)
    allp  = range(1,P+1)
    allij = list(itertools.product(range(1,N+1),range(1,N+1))) # product 1-based with i==j
    allhk = list(itertools.product(range(1,P+1),range(1,P+1))) # product 1-based with h==k
    allih = list(itertools.product(range(1,N+1),range(1,P+1))) # product 1-based
    #allijhk = itertools.product(range(1,N+1),range(1,N+1),range(1,P+1),range(1,P+1)) # product 1-based by 4

    print "allij",allij
    print "allhk",allhk
    print "allih",allih

    def makebin(name):
        return LpVariable(name,cat='Binary')
    def makelinpos(name,maxv):
        return LpVariable(name,lowBound=0,upBound=maxv,cat='Continuous')
    def makeint(name,maxv):
        return LpVariable(name,lowBound=1,upBound=maxv,cat='Integer')
    Wmax = sum([t.cost + defmax([te.delay for te in t.parents],0) for t in tasks])
    print "Wmax is",Wmax
    makenormalized = lambda name: makelinpos(name,1)
    makeproc = lambda name: makeint(name,P)
    maketask = lambda name: makeint(name,N)
    maketime = lambda name: makelinpos(name,Wmax)

    # we embed the constraint in the variable type definition for the time
    t = [makelinpos("t_%d" % i,min(Wmax,tasks[i-1].deadline)) for i in alli] #t[i:task]:time TODO add here the timelimit

    # ALTERNATIVELY use dicts of LpVariable
    p = [makeproc("p_%d" % i) for i in alli] # p[i:task]:proc
    x = dict([(ih,makebin("x_%d_%d" % ih)) for ih in allih]) # x[i:task,h:proc]:binary if i runs on h <=> p[i:task]

    # in the following we create but not use i== j
    si = dict([(ij,makebin("si_%d_%d" % ij)) for ij in allij]) # si[i,j:task]:binary
    eps = dict([(ij,makebin("eps_%d_%d" % ij)) for ij in allij]) # eps[i,j:task]:binary

    # we avoid creating z explicitly
    #z = dict([(ijhk,makebin("z_%d_%d_%d" % ijhk)) for allijhk]) # z[i,j:task,h,k:proc]:normalized
    z = dict()
    W = maketime("W") # the objective time function 0..Wmax

    prob = LpProblem("The fantastic scheduler",LpMinimize)

    #Not needed
    prob += W >= 0, "Enforce positivity"
    for tt in t:
        prob += tt >= 0, "Enforce positivity of " + tt.name

    prob += W, "Span"    

    # build task object to index 1-based
    # build cost array
    inv = {}
    L = []
    for i,tt in enumerate(tasks):
        inv[tt] = i+1
        L.append(tt.cost)
    print "costs",L

    # constraints
    for i in range(1,N+1):
        prob += t[i-1] + L[i-1] <= W # execution smaller than maxspan
        prob += sum([k*x[(i,k)] for k in allp]) == p[i-1]  # match task of processor wiht processor of task
        prob += sum([x[(i,k)] for k in allp]) == 1 # only one processor

    # this holds for all i != j
    for i in range(1,N+1):
        for j in range(1,N+1):
            if i == j:
                continue
            sij = si[(i,j)]
            epsij = eps[(i,j)]
            prob += t[j-1] - t[i-1] - L[i-1] - (sij-1) * Wmax >= 0  # ?
            prob += p[j-1] - p[i-1] - 1 - (epsij-1) * P >= 0 # ?
            if i > j:
                # symmetric enforcement
                sji = si[(j,i)]
                epsji = eps[(j,i)]
                prob += sij+sji+epsij+epsji >= 1
                prob += sij + sji <= 1
                prob += epsij + epsji <= 1

    for j in range(1,N+1): # all task 1-based
        tt = tasks[j-1]
        for pt in tt.parents: # all parent tasks objects
            i = inv[pt.source] # 1-based
            prob += si[(i,j)] == 1 # dependency

            if args.quadratic:
                prob += t[i-1] + L[i-1] + sum([ (h != k and pt.delay or 0) * x[(i,h)] * x[(j,k)] for h,k in allhk]) <= t[j-1] # time
            else:
                # z[ij hk] := x[ij hk] x[ji kh]
                for h,k in allhk:
                    q = (i,j,h,k)
                    z[q] = makebin("z_%d_%d_%d_%d" % q)
                    # NOTE z[(j,i,k,h)] = z[q]

                if not args.usecompact:
                    for h,k in allhk:
                        q = (i,j,h,k)
                        prob += x[(i,h)] >= z[q]
                        prob += x[(j,k)] >= z[q]
                        prob += x[(i,h)]+x[(j,k)]-1 <= z[q]
                else:
                    # eq. 32 
                    for k in allp:
                        prob += sum([z[(i,j,h,k)] for h in allp]) == x[(j,k)]   
                    # eq. 33 symmetric: z[ijhk] == z[jikh] BUT we never create z[ji**] being DAG
                    # BUT we do not USE z[]
                    # prob += z[(i,j,h,k)] == z[(j,i,k,h)]

                # the model employed is DELAY, this is a heavy constraint noting that z(i,j,h,k) is mutually exclusive in (h,k)
                prob += t[i-1] + L[i-1] + sum([ (h != k and pt.delay or 0) * z[(i,j,h,k)] for h,k in allhk]) <= t[j-1] # time


    prob.writeLP("pulp.lp")
    prob.solve()#pulp.GLPK_CMD())
    print("Status:", LpStatus[prob.status])
    print("Objective:", value(prob.objective))
    print("maxspan",W.varValue)
    for i,v in enumerate(t):
        print(v.name, "=", v.varValue)
        tasks[i].setOneRun(v.varValue)
    for i,v in enumerate(p):
        print(v.name, "=", v.varValue)
    for i,v in enumerate(z.values()):
        print(v.name, "=", v.varValue)

    stasks = tasks[:]
    stasks.sort(key=lambda k: k.earlieststart)
    schedule = [Proc(h) for h in range(1,P+1)]

    # build the ProcTask by scanning the tasks ordered
    for tt in stasks:
        i = inv[tt]
        proc = schedule[int(p[i-1].varValue)-1]
        proc.addTask(tt,tt.earlieststart,tt.endtime)

    schedule = [p for p in schedule if len(p.tasks) > 0]

    return dict(T=W.varValue,schedule=schedule)

if __name__ == "__main__":

    import argparse
    parser = argparse.ArgumentParser(description='Task Scheduling for Multiprocessor - Emanuele Ruffaldi 2016 SSSA')
    parser.add_argument('--algorithm',default="cpr",help='chosen algorithm: cpr none pulp cpa dyn')
    parser.add_argument('input',help="input file")  
    parser.add_argument('--cores',type=int,default=4,help="number of cores for the scheduling")
    parser.add_argument('--verbose',action="store_true")
    parser.add_argument('--nolabels',action="store_true")
    parser.add_argument('--earliest',action="store_true",help="uses earliest instead of bottom-level for the MLS")
    parser.add_argument('--usefloats',action="store_true",help="compute using floats instead of fractions")
    parser.add_argument('--usecompact',action="store_true",help="compact z constraint for pulp model")
    parser.add_argument('--quadratic',action="store_true",help="quadratic model for pulp (not supported so far by pulp)")
    parser.add_argument('--notfavoraffinity',action="store_true",help="favor affinity favoraffinity (NOT in pulp)")
    #parser.add_argument('--transitive',action="store_true",help="transitive reduction")
    #parser.add_argument('--nextproc',action="store_true",help="uses just the next available proc")
    parser.add_argument('--output',help="JSON output of scheduling")
    parser.add_argument('--drawedges',help="emit edges in the schedule savepng/savesvg",action="store_true")
    parser.add_argument('--savejson',help="emit JSON of the input graph")
    parser.add_argument('--savedot',help="emit DOT of the input graph")
    parser.add_argument('--savepng',help="emit PNG")
    parser.add_argument('--savesvg',help="emit SVG")
    parser.add_argument('--transitive',action="store_true",help="apply transitive closure removing dependency (GOOD only for some TASK models)")
    parser.add_argument('--saverun',help="emit RUN for taskrunner")
    parser.add_argument('--keepancestorsinrun',help="ignore ancestor removal",action="store_true")

    args = parser.parse_args()

    if args.usefloats or args.algorithm == "pulp":
        makenumbers = float

    tasks = loadtasks(args.input)

    if args.savejson:
        savetasksjson(tasks,args.savejson == "-" and sys.stdout or open(args.savejson,"wb"))
        sys.exit(0)
    if args.savedot:
        savetasksdot(tasks,args.savedot == "-" and sys.stdout or open(args.savedot,"wb"))
        sys.exit(0)
        print "\n".join([str(x.id) for x in tasks])
    
    if args.verbose:
        for t in tasks:
            print t
    if args.algorithm == "cpr":
        r = cpr(tasks,args.cores,args)  
    elif args.algorithm == "cpa":
        r = cpa(tasks,args.cores,args)  
    elif args.algorithm == "pulp":
        r = xpulp(tasks,args.cores,args)  
    elif args.algorithm == "dyn":
        r = dyn(tasks,args.cores,args)  
    elif args.algorithm == "none":
        print "Tasks",len(tasks)
        for t in tasks:
            print t
        sys.exit(0)
    else:
        print "unknown algorithm",args.algorithm
        sys.exit(0)

    #Regular Run
    r["tasks"] = tasks
    updatepriorities(r["schedule"],tasks)
    e = analyzeschedule(r["schedule"],tasks)
    for p in r["schedule"]:
        print p
    print e
    for t in tasks:
        print t
    print "Total",float(r["T"])
    if args.output:
        # TODO store in the output flags for the synchronization
        s = []
        for p in r["schedule"]:
            pp = []
            for q in p.tasks:
                pp.append(dict(task=q.task.id,span=[float(q.begin),float(q.end)],split=len(q.task.proc)))
            s.append(pp)
        j = dict(maxspan=float(r["T"]),schedule=s)
        if args.output == "-":
            fp = sys.stdout
        else:
            fp = open(args.output,"wb")
        json.dump(j,fp,sort_keys=True,indent=4, separators=(',', ': '))
    if args.savepng or args.savesvg:
        import sched2run
        oo = sched2run.sched2run(args,r["schedule"],r["tasks"],verbose=args.verbose)
        print "Commands %d - Semaphores %d - Notifies %d" % (oo["ncommands"],len(oo["semcount"]),sum(oo["semcount"]))
        drawsched(args.savepng or args.savesvg,r["schedule"],tasks,oo["sembegin"],oo["semcount"],args.nolabels)
    if args.saverun:
        import sched2run
        oo = sched2run.sched2run(args,r["schedule"],r["tasks"],verbose=args.verbose)
        print "Commands %d - Semaphores %d - Notifies %d" % (oo["ncommands"],len(oo["semcount"]),sum(oo["semcount"]))
        open(args.saverun,"wb").write("\n".join([" ".join([str(z) for z in y]) for y in oo["commands"]]))


