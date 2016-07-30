"""
CPR scheduling
Emanuele Ruffaldi, Scuola Superiore Sant'Anna 2016

TODO: edge cost not used for OPTIMIZING CPR (e.g. favouring same processor) BUT it is used in the final cost
TODO: find a case with earliest meaningful

TODO: compute ALAP The ALAP start-time of a node is a measure of how far the
nodes start-time can be delayed without increasing the schedule length: http://charm.cs.uiuc.edu/users/arya/docs/6.pdf

TODO: maybe static level = maximum path cost without the edge costs

"""
import sys,math
import heapq
import fractions
import argparse
import operator
from collections import OrderedDict
from functools import reduce as _reduce
import json
import operator

def forall(u,op):
    for x in u:
        op(x)


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
    def __init__(self,id,cost,maxnp,deadline=None):
        self.id = id       # identifier
        self.parents = [] # inputs as MTaskEdge
        self.maxnp = maxnp # maximu number of processors (0=all means infinitely splittable!)
        self.cost = makenumbers(cost)     # cost values (should be evenly divisible by maxnp if maxnp is not null)
        if deadline is not None:
            if deadline == 0:
                self.deadline = None
            else:
                self.deadline = makenumbers(deadline)
        else:
            self.deadline = deadline

        # Computed FIXED propertirs
        self.sparents = set()  # tasks in parents
        self.children = []     # computed children
        self.top = 0        # t-level
        self.slevel = 0     # s-level (not counitng edge cost)
        self.bottom = 0     # b-level
        self.cdeadline = deadline  # children earliest deadline

        # Scheduling Results
        self.endtime = 0       # time of last proc running task
        self.earlieststart = 0 # time of effectiv first start among tasks in proc
        self.lateststart = 0
        self.proc = set() # processors used
        self.ucost = self.cost # self.cost/self.Np
        self.Np = 1         # effective number of processor (request) == len(self.proc)
    def __repr__(self):
        return "MTask %s cost=%d top=%s bottom=%s slevel=%d maxnp=%d early/late=%s %s deadline/c=%s %s parents=%s children=%s" % (self.id,self.cost,self.top,self.bottom,self.slevel,self.maxnp,self.earlieststart,self.lateststart,self.deadline,self.cdeadline,[t.id for t in self.sparents],[t.dest.id for t in self.children])
    def updateNp(self,n):
        self.Np = n
        self.ucost = self.cost / self.Np
# use of task by Processor, it was (s,e,t) but we need also slicing
class ProcTask:
    def __init__(self,proc,task,begin,end):
        self.proc = proc
        self.task = task
        self.begin = begin
        self.end = end
        self.rangesplit = (0,0) # (istart,iend)
class Proc:
    """Processor allocation"""
    def __init__(self,index):
        self.index = index # number of the processor
        self.tasks = []    # (start,end,task) for the ones to be executed
        self.next = 0      # last task completed == self.tasks[-1][1]
        self.stasks = set() # task of which Proc contains all results
    def __repr__(self):
        if makenumbers == float:
            return "Proc(%d) ends %.2f tasks:\n%s" % (self.index,self.next,"\n".join(["\t%-6s [%.2f %.2f, %d-%d]" % (q.task.id,q.begin,q.end,q.rangesplit[0],q.rangesplit[1]) for q in self.tasks]))
        else:
            return "Proc(%d) ends %s tasks:\n%s" % (self.index,self.next,"\n".join(["\t%-6s [%s %s, %d-%d]" % (q.task.id,q.begin,q.end,q.rangesplit[0],q.rangesplit[1]) for q in self.tasks]))


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
                list(t.proc)[0].rangesplit = (0,t.maxnp)
            elif t.maxnp > 0:
                # we have a boundex task so we can split it
                k = int(math.ceil(t.maxnp/t.Np))
                z = list(t.proc)
                i0 = 0
                for i in range(0,len(t.proc)):
                    z[i].rangesplit = (i0,i0+k)
                    i0 += k
                # NOTE: with k we overstimate, so we could have some excess in the last. We do not take this into account
                z[-1].rangesplit = (z[-1].rangesplit[0],t.maxnp)
            else:
                # unbounded paralle tasks assign 1 slot to every processor
                z = list(t.proc)
                for i in range(0,len(t.proc)):
                    z[i].rangesplit = (i,i)

def MLS(tasks,numCores,args):
    """Computes MLS"""
    proco = [Proc(i) for i in range(1,numCores+1)]
    procpq = []
    if args.nextproc:
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
                    t.earlieststart = justdonetime # minimum time for this due to this LAST parent
                    t.lateststart = justdonetime
                    # TODO: should we add the input costs?
                    #print "adding",t.id,"earliest",justdonetime
                    if args.earliest:
                        heapq.heappush(ready,((t.cdeadline,justdonetime),t)) # or priority, in any case i s0
                    else:
                        heapq.heappush(ready,((t.cdeadline,t.bottom),t)) # or priority, in any case i s0
                   
        # get highest priority
        pri,t = heapq.heappop(ready)
        needed.remove(t)


        # We should pick all the Np processors p that satisfy the following
        #   next[p] + cost[t,p] + sum([cost[ta,t,proc[ta],p] for ta in parents[t]])
        #
        #   note: if proc[ta] > 1 we can take max or mean
        #
        # The above formulation is in the line of heterogeneous systems, and also deals with affinity: cost[ta,t,p,p] = 0
        # In the classic formulation we simply order by: next[p]
        #
        # We would like also to 
        # split cost due to parallelims, precompute all input transfers
        basecost = t.ucost

        if args.nextproc:
            # we cannot allocate due to the lack of available processors
            if t.Np > len(procpq):
                return None,[]
            picked = [heapq.heappop(procpq) for i in range(0,t.Np)]
        else:
            if t.Np > len(proco):
                return None,[]
            allp = []
            # compute the end-time of the choice
            optionminimizedeps = True # given same cost optimize for minimize affinity
            for p in proco:
                # for heterogeneouse modify this with cost[t,p]
                ee = max(p.next,t.earlieststart) + basecost
                if optionminimizedeps:
                    epp = set()
                for x in t.parents: # x is TaskEdge
                    if args.transitive and x.source in p.stasks:
                        continue # result already transferred
                    else:
                        n = makenumbers(len(x.source.proc))
                        pap = set([q.proc for q in x.source.proc])
                        # for heterogeneouse modify this with cost[ta.source,t,ta.proc,p]
                        if args.samezerocost and p in pap:
                            ee += x.cost*((n-1)/n)
                        else:
                            ee += x.cost
                        if optionminimizedeps:
                            epp = epp | pap
                if optionminimizedeps:
                    if p in epp:
                        epp.remove(p)
                    allp.append((ee,p,len(epp)))
                else:
                    allp.append((ee,p))
            if optionminimizedeps:
                allp.sort(key=lambda x: (x[0],x[2]))
                picked = [x[0:2] for x in allp[0:t.Np]]
            else:
                allp.sort(key=lambda x: x[0])
                picked = allp[0:t.Np]

        # we load the transfer cost into the RECEIVER and we DON'T charge the SENDER
        allinputcosts0 = sum([x.cost for x in t.parents])

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
                        n = makenumbers(len(x.source.proc))
                        allinputcosts += x.cost*(n-1)/n
                    else:
                        allinputcosts += x.cost # full cost being outside
                    p.stasks.add(x.source) # we are inglobating it for scheduling t
                duration = allinputcosts + basecost
            else:
                duration = allinputcosts0 + basecost

            # compute execution range
            tstart = max(p.next,t.earlieststart)
            tend = tstart + duration
            
            # create allocation
            q = ProcTask(p,t,tstart,tend)
            p.tasks.append(q) # tstart-p.next IS flexibility
            p.next = tend # marks next available
            t.proc.add(q)

            if args.nextproc:
                heapq.heappush(procpq,(p.next,p))

        t.earlieststart = picked[0][1].tasks[-1].begin  # adjusted to reflect 
        t.lateststart = picked[-1][1].tasks[-1].begin   # adjusted to reflect 
        t.endtime = picked[-1][1].tasks[-1].end

        # fail
        if t.deadline is not None and t.lateststart > t.deadline:
            return None,[]

        heapq.heappush(running,(t.endtime,t)) # tend is the end of the last proc for task t

    # end time is max
    index, max_next = max(enumerate(proco),key=lambda p: p[1].next)
    return max_next.next,proco

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


    while Tchanged: # not modified
        Tchanged = False
        chi = [t for t in tasks if t.Np < numCores] # modifiable set
        while len(chi) > 0:   # PAPER: until T modified or chi empty
            index, max_value_ignored = max(enumerate([t.top+t.bottom for t in chi]), key=operator.itemgetter(1))
            t = chi[index]
            if t.Np == t.maxnp: #saturated
                del chi[index]
            else:
                t.updateNp(t.Np+1)
                # try distribution using given unmber of cores for given processor
                Ti,tai = MLS(tasks,numCores,args)
                #print "tried upgrade of ",t.id," with ",t.Np," obtaining ",Ti,"vs previous",T
                if Ti is not None and Ti < T:     # NOTE: None < x for any x != None           
                    T = Ti
                    ta = tai
                    Tchanged = True
                else:
                    # failed
                    t.updateNp(t.Np-1)
                    del chi[index]
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
            t.top = max([p.source.top + p.source.cost + p.cost for p in t.parents])

    for t in tasks[::-1]:
        if len(t.children) == 0:
            t.bottom = t.ucost
            t.slevel = t.ucost
            t.cdeadline = t.deadline
        else:
            t.bottom = max([p.dest.bottom + p.cost for p in t.children])+t.ucost
            t.slevel = max([p.dest.slevel  for p in t.children])+t.ucost            
            # move back the cdeadline (if any) of each child by the transfer cost and sum up my cost
            cd = [p.dest.cdeadline-p.cost for p in t.children if p.dest.cdeadline is not None]
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
            t.top = max([p.source.top + p.source.cost + p.cost for p in t.parents])
            #t.top = max([p.top + p.cost for p in t.sparents])
        for p in t.parents:         
            p.source.children.append(p)

    for t in tasks[::-1]:
        if len(t.children) == 0:
            t.bottom = t.cost
            t.slevel = t.cost
            t.cdeadline = t.deadline
        else:
            t.bottom = max([p.dest.bottom + p.cost  for p in t.children])+t.cost
            t.slevel = max([p.dest.slevel   for p in t.children])+t.cost
            # move back the cdeadline (if any) of each child by the transfer cost and sum up my cost
            cd = [p.dest.cdeadline-p.cost for p in t.children if p.dest.cdeadline is not None]
            if len(cd) == 0:
                t.cdeadline = t.deadline
            else:
                # move it back using my current cost, and anyway later than my deadline (if any)
                t.cdeadline = min(t.deadline,min(cd)-t.ucost)


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
            t = MTask(id,ta.get("cost",1),ta.get("maxnp",defaultcore),ta.get("deadline"))
            ts.append(t)
            td[t.id] = t
        for id,ta in j.iteritems():
            me = ts[id]
            ts[id].parents = [makedge(td[x],me) for x in ta["inputs"]]
    else:
        # each is a list with 
        for ta in j:
            t = MTask(ta["id"],ta.get("cost",1),ta.get("maxnp",defaultcore),ta.get("deadline"))
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
        t = MTask(n.get_name(),float(ad.get("cost",1)),int(ad.get("maxnp",defaultcore)),int(ad.get("deadline",0)))
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

def drawsched(name,schedule,tasks):
    # draw the make span horizontally (#proc < #tasks)
    import cairo
    nproc = len(schedule)
    if nproc > 0:
        maxspan = max([x.next for x in schedule])
        minspan = min([min([q.end-q.begin for q in p.tasks]) for p in schedule])
        maxtext = max([max([len(str(q.task.id)) for q in p.tasks]) for p in schedule])
    else:
        maxspan = 0
        minspan = 0
        maxtext = 0
    pheight = 20
    pymargin = 2
    pxmargin = 2
    fontsize = 12
    height = (pheight+pymargin)*(nproc+1)
    # minimum (for text) should be 50 pixels
    if minspan != 0:
        timescale = (maxtext*fontsize+20)/minspan
    else:
        timescale =1 
    width = int(maxspan*timescale+pxmargin*2)

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
    for p in schedule:
        for q in p.tasks:
            e = q.end
            b = q.begin
            t = q.task
            durwidth = (e-b)*timescale
            durwidth -= pxmargin
            bx = b*timescale+pxmargin
            cr.rectangle(bx, py, durwidth, pheight)
            cr.set_source_rgb(0, 0, 0)
            cr.stroke_preserve()
            cr.set_source_rgb(255, 255, 255)
            cr.fill()
            cr.set_source_rgb(0, 0, 0)
            s = str(t.id)
            (x, y, w, h, dx, dy) = cr.text_extents(s)
            cr.move_to(bx+durwidth/2-w/2, py+pheight/2-h/2-y)
            cr.show_text(s)
            # all processors to be waited except this processor
            allp = _reduce(operator.or_,[set([q.proc for q in x.source.proc]) for x in t.parents],set())
            allp = allp - set([p])

            # nothing to be wait for
            if len(allp) == 0:
                mode = 1
            # one single semafore
            elif len(allp) == 1:
                mode = 2
                c = (255,255,0)
            # more than one
            else:
                mode = 3
                c = (255,0,0)
            if mode > 1:
                cr.rectangle(bx, py+pheight/2-pheight/8.0, pheight/4.0,pheight/4.0)
                cr.set_source_rgb(0, 0, 0)
                cr.stroke_preserve()
                cr.set_source_rgb(*c)
                cr.fill()
            if t.deadline is not None and t.lateststart > t.deadline:
                cr.arc(bx+pheight/8.0, py+pheight/8.0, pheight/8.0,0,2*math.pi)
                cr.set_source_rgb(0, 0, 0)
                cr.stroke_preserve()
                cr.set_source_rgb(255,0,0)
                cr.fill()


        py += pheight+pymargin
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
        if t.deadline is not None and t.deadline-t.lateststart < 0: # NOT t.deadline < t.lateststart
            print "missed deadline"
            deadlineerrors += 1
        for s in t.sparents:
            if t.earlieststart < s.endtime:
                print "inversion for ",t.id," starts ",t.earlieststart," against ",s.id," ends ",s.endtime
                errors += 1

    return dict(avgslack=float(sum(avgs)/len(schedule)),used=len(avgs),errors=errors,deadlineerrors=deadlineerrors)

def loadsched(f):
    pass

def loadtasks(f):
    if f.endswith(".json"):
        tasks = loadtasksjson(open(f,"rb"))
    else:
        tasks = loadtasksdot(open(f,"rb"))
    tasks = toposorttasks(tasks)
    # update structures and compute 
    annotatetasks(tasks)
    return tasks

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
    parser.add_argument('--nextproc',action="store_true",help="uses just the next available proc")
    parser.add_argument('--output',help="JSON output of scheduling")
    parser.add_argument('--savepng',help="emit PNG")
    parser.add_argument('--savesvg',help="emit SVG")
    parser.add_argument('--saverun',help="emit RUN for taskrunner")

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
            drawsched(args.savepng or args.savesvg,r["schedule"],tasks)
        if args.saverun:
            import sched2run
            oo = sched2run.sched2run(r["schedule"],r["tasks"])
            open(args.saverun,"wb").write("\n".join([" ".join([str(z) for z in y]) for y in oo]))

    elif args.algorithm == "none":
        print "Tasks",len(tasks)
        for t in tasks:
            print t
    else:
        print "unknown algorithm",args.algorithm

