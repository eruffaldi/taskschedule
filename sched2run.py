import sched,math
from collections import defaultdict
def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

Actions = enum(*("NUMTHREADS,NUMSEMAPHORES,SEMAPHORE,NUMACTIONS,RUNTASK,RUNTASKPAR,WAIT,NOTIFY,SLEEP,NUMREDUCETARGET,COMMENT".split(",")))
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

class RunAction:
	def __init__(self,*args):
		self.action = args[0]
		self.threadid = args[1]
		self.id = args[2]
		self.params = args[3:]
		self.taskid = None #

	@staticmethod
	def fromfile(inf):
		runid2id = dict()
		aa = []
		for x in inf:
			x = x.strip()
			if x.startswith("#"):
				if x.startswith("# task"):
					ig1,ig2,runid,id = x.split(" ",3)
					runid2id[int(runid)] = id
			else:
				a = [int(y) for y in x.split(" ")]
				q = RunAction(*a)
				if q.action == Actions.RUNTASK or q.action == Actions.RUNTASKPAR or q.action == Actions.NUMREDUCETARGET:
					q.taskid = runid2id.get(q.id,None)
				aa.append(q)
		return aa


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


def sched2run(args,schedule,tasks,verbose=False):
	implicitjoint = 0
	o = []
	o.append(["# op tid id p1 p2 p3"])

	#map task id to numbers: first all integers
	taskid2id = dict()
	sems = []
	osched = []
	ancestors = dict()

	# build the ancestors of a node, being it a topologically sorted it is fine
	for t in tasks:
		q = set()
		for pa in t.parents:
			q = q | ancestors[pa.source.id]
			q.add(pa.source.id)
		ancestors[t.id] = q
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
		if str(v) != str(k):
			o.append(["#","task",v,k])

	o.append(makeTHREADS(len(schedule),implicitjoint))
	
	# enforce the fact that new TASK starts after parent tasks has been completed. We use a semaphore that is decremented
	# and WHEN zero is reached all waiters are notified (notifyall)
	#
	# In our scheme each PARENT has a single WAIT task
	# 
	# compute need of semaphore. A new task WAIT for another IF
	#	- source has multiple
	#	- target has multiple
	#	- source and target are in the different processors
	#
	# semaphore is on START
	# notificatio is on end
	#
	tasksembegin = {}
	tasksemend = defaultdict(list)
	#tasksemnotify = defaultdict(set)

	#TODO missing: we handle transitive inside the same processor, BUT NOT in different processors

	for t in tasks:
		# extract the single processor
		if len(t.proc) == 1:
			thisproc = list(t.proc)[0].proc 
		else:
			thisproc = None # no affinity if multiple

		# we'll pick the last
		actionproc = defaultdict(list)
		
		# if we wait for parent we assume waiting for all the ancestors
		if not args.keepancestorsinrun:
			qall = ancestors[t.id] - set([p.source.id for p in t.parents])
		else:
			qall = set()

		for tp in t.parents:
			# if the parent is an ancestor of another parent it has been already processed
			if tp.source.id in qall:
				print "ignored",tp.id,"parent of",t.id,"due to ancestor"
				continue	
			# processors of parent
			allproc = tp.source.allproc()

			# this parent accounts for all its allocated processors except if we are from the same
			if thisproc in allproc:
				count += len(allproc)-1
				for p in allproc:
					if p != thisproc:
						actionproc[p].append(tp.source)
			else:
				count += len(allproc)	
				for p in allproc:
					actionproc[p].append(tp.source)

 		count = len(actionproc)

 		# if we have to wait SOME processor then count is not zero
		if count > 0:
			# new semaphorses
			si = len(sems)
			sems.append(count) # initialization number
			tasksembegin[t.id] = si # for EMITTING wait

			# for all the task last in the processors append a NOTIFY
			for k,v in actionproc.iteritems():			
				v.sort(key=lambda x:x.endtime) # sorted by end-time
				last = v[-1]  # last of given processor
				tasksemend[last.id].append((t,si)) # notify to task t and semaphore si

	for index,p in enumerate(schedule):
		ss = []
		for q in p.tasks:
			t = q.task

			si = tasksembegin.get(t.id)
			if si is not None:
				print "taskid %s waits sem %d" % (t.id,si)
				ss.append(["#","taskid %s waits sem %d" % (t.id,si)])
				ss.append(makeWAIT(index,si))

			if t.items > 1:
				# mapreduce or data-parallel in any case it runs 
				ss.append(makeRUNTASKPAR(index,taskid2id[t.id],q.rangesplit[0],q.rangesplit[1],q.rangesplit[2]))
			elif t.items == 0:
				# we support semaphores but no tasks or action
				pass
			else:
				ss.append(makeRUNTASK(index,taskid2id[t.id]))				

			# wait for ANY parent if they have an associated semaphore
			for ct,si in tasksemend[t.id]:
				#print "taskid %s notifies sem %d of %s" % (t.id,si,ct.id)
				ss.append(["#","taskid %s notifies sem %d of %s" % (t.id,si,ct.id)])
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
	return dict(commands=o,sembegin=tasksembegin,semcount=sems,ncommands=len([x for x in o if x[0] != "#"]))

if __name__ == '__main__':
	import sys
	for a in sys.argv[1:]:
		print "Doing",a
		aa = RunAction.fromfile(open(a,"rb"))
		for x in aa:
			print x.action,x.threadid,x.id,x.taskid,x.params

	