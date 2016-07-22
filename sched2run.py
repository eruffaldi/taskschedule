import sched

def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

Actions = enum(*("NUMTHREADS,NUMSEMAPHORES,SEMAPHORE,NUMACTIONS,RUNTASK,RUNTASKPAR,WAIT,NOTIFY,SLEEP".split(",")))
#action tid id p0 p1 p2 p3

#NUMTHREADS p0
#NUMSEAPHORES p0
#NUMACTIONS p0
#SEMAPHORE id=sem p0=count
#RUNTASK tid id=task
#RUNTASKPAR tid id=task p0-p1 is rang
#WAIT id=sem
#NOTIFY id=sem
#SLEEP not used

def makeTHREADS(n):
	return [Actions.NUMTHREADS,0,0,n,0]

def makeSEMAPHORES(n):
	return [Actions.NUMSEMAPHORES,0,0,n,0]

def makeACTIONS(tid,n):
	return [Actions.ACTIONS,tid,0,n,0]

def makeSEMAPHORE(sid,n):
	return [Actions.SEMAPHORE,0,sid,n,0]

def makeRUNTASK(tid,id):
	return [Actions.RUNTASK,tid,id,0,0]

def makeRUNTASKPAR(tid,id,f,e):
	return [Actions.RUNTASK,tid,id,f,e,0]

def makeWAIT(tid,sid):
	return [Actions.WAIT,tid,sid,0,0,0]

def makeNOTIFY(tid,sid):
	return [Actions.NOTIFY,tid,sid,0,0,0]

def makeSLEEP(tid,tms):
	return [Actions.SLEEP,tid,0,tms,0,0]


def sched2run(schedule,tasks):
	o = []
	o.append(makeTHREADS(len(schedule)))

	#map task id to numbers: first all integers
	taskid2id = dict()
	for t in tasks:
		if type(t.id) == int:
			taskid2id[t.id] = t.id
	if len(taskid2id) == 0:
		z = 1
	else:
		z = max([x for x in taskid2id.keys()])+1
	for t in tasks:
		if type(t.id) != int:
			taskid2id[t.id] = z
			z = z + 1

	sems = []
	osched = []
	tasksem = {}

	# compute semaphore of tasks
	for t in tasks:
		if len(t.procs) == 1:
			thisproc = list(t.procs)[0].proc
			# assume no need
			needed = False
			# check if all parents are single-proc in the same proc of this
			for tp in t.parents:
				if len(tp.procs) > 1 or list(tp.procs)[0].proc != thisproc:
					needed = True 
		else:
			needed = True
		if needed:
			si = len(sems)
			sems.append(len(t.procs))
			tasksem[t.id] = si

	for p in schedule:
		ss = []
		for q in p.tasks:
			t = q.task
			# wait for ANY parent if they have an associated semaphore
			for pt in t.parents:
				si = tasksem.get(pt.id)
				if si is not None:
					ss.append(makeWAIT(si))

			if len(t.procs) == 1: # single
				ss.append(makeRUNTASK(p.index,taskid2id[t.id]))				
			else: # par
				ss.append(makeRUNTASKPAR(p.index,taskid2id[t.id],q.first,q.last))

			# notify if needed by THIS task instance
			si = tasksem.get(t.id)
			if si is not None:
				ss.append(makeNOTIFY(si))
		osched.append(ss)					

	# add closings semaphore
	if len(schedule) > 1:
		si = len(sems)
		sems.append(len(schedule)-1)
		for i in range(1,len(schedule)):
			osched[i].append(makeNOTIFY(si))
		osched[0].append(makeWAIT(si))

	# out semaphores
	o.append(makeSEMAPHORES(len(sems)))
	# out semaphor init
	for i in range(0,len(sems)):
		o.append(makeSEMAPHORE(i,sems[i]))
	# out action counts
	for i in range(0,len(osched)):
		o.append(makeACTIONS(i,len(osched[i])))
	# emit the execution
	for p in osched:
		o.extend(p)
	return o

	