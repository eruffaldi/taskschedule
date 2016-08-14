import sched,math

def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

Actions = enum(*("NUMTHREADS,NUMSEMAPHORES,SEMAPHORE,NUMACTIONS,RUNTASK,RUNTASKPAR,WAIT,NOTIFY,SLEEP,NUMREDUCETARGET".split(",")))
#action tid id p0 p1 p2 p3

#NUMTHREADS p0
#NUMSEAPHORES p0
#NUMACTIONS p0
#SEMAPHORE id=sem p0=count
#RUNTASK tid id=task
#RUNTASKPAR tid id=task p0-p1 p2=target
#WAIT id=sem
#NOTIFY id=sem
#SLEEP not used
#NUMREDUCETARGET tid=thread(or ALL) id=task n=size (where task is the mapreduce)

def makeTHREADS(n,implicitjoin=0):
	return [Actions.NUMTHREADS,0,0,n,implicitjoin]

def makeNUMREDUCETARGET(tid,sid,n):
	return [Actions.NUMREDUCETARGET,tid,sid,n,0]

def makeSEMAPHORES(n):
	return [Actions.NUMSEMAPHORES,0,0,n,0]

def makeACTIONS(tid,n):
	return [Actions.NUMACTIONS,tid,0,n,0]

def makeSEMAPHORE(sid,n):
	return [Actions.SEMAPHORE,0,sid,n,0]

def makeRUNTASK(tid,id):
	return [Actions.RUNTASK,tid,id,0,0]

def makeRUNTASKPAR(tid,id,f,e,target=0):
	return [Actions.RUNTASKPAR,tid,id,f,e,target]

def makeWAIT(tid,sid):
	return [Actions.WAIT,tid,sid,0,0,0]

def makeNOTIFY(tid,sid):
	return [Actions.NOTIFY,tid,sid,0,0,0]

def makeSLEEP(tid,tms):
	return [Actions.SLEEP,tid,0,tms,0,0]


def sched2run(schedule,tasks):
	implicitjoint = 0
	o = []
	o.append(["# op tid id p1 p2 p3"])

	#map task id to numbers: first all integers
	taskid2id = dict()
	sems = []
	osched = []
	tasksem = {}
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

	# emit task map for later association of id to task.id
	for k,v in taskid2id.iteritems():
		o.append(["#","task",v,k])

	o.append(makeTHREADS(len(schedule),implicitjoint))
	
	# compute semaphore of tasks by checking for AFFINITY
	for t in tasks:
		if len(t.proc) == 1:
			thisproc = list(t.proc)[0].proc
			# assume no need
			needed = False
			# check if all parents are single-proc in the same proc of this
			for tp in t.parents:
				if len(tp.source.proc) > 1 or list(tp.source.proc)[0].proc != thisproc:
					needed = True 
		else:
			needed = True
		if needed:
			si = len(sems)
			sems.append(len(t.proc))
			tasksem[t.id] = si

	for index,p in enumerate(schedule):
		ss = []
		for q in p.tasks:
			t = q.task
			# wait for ANY parent if they have an associated semaphore
			for pt in t.parents:
				si = tasksem.get(pt.source.id)
				if si is not None:
					ss.append(makeWAIT(index,si))

			if t.items > 1:
				# mapreduce or data-parallel in any case it runs 
				ss.append(makeRUNTASKPAR(index,taskid2id[t.id],q.rangesplit[0],q.rangesplit[1],q.rangesplit[2]))
			elif t.items == 0:
				# we support semaphores but no tasks or action
				pass
			else:
				ss.append(makeRUNTASK(index,taskid2id[t.id]))				

			# notify if needed by THIS task instance
			si = tasksem.get(t.id)
			if si is not None:
				ss.append(makeNOTIFY(index,si))
		osched.append(ss)					

	# add closings semaphore: emitted by each proc 1..n, wait by main
	if implicitjoint != 0 and len(schedule) > 1:
			si = len(sems)
			sems.append(len(schedule)-1)
			for i in range(1,len(schedule)):
				osched[i].append(makeNOTIFY(i,si))
			osched[0].append(makeWAIT(0,si))

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

	