import sched
import sched2run
import os
#graph (dot or json) with taskid


#RUN = action threadid runtaskid|semid p0..p2
# space sep values with # comments
# special comment to associate runtaskid (int) with taskid: "#","task",runtaskid,taskid
# relevant actions: TASK=4 TASKPAR=5 WAIT=6

#Actions
# annotate log with runtaskid taskid and section of data in par task
# extract average/worst duration for task and taskpar (multiple log files)
# update graph with costs from the averafe rotation (multiple log files or average/worst annotated)

class RunActionLog:
	def __init__(self):
		# stored
		self.index = 0
		self.action = 0
		self.endtime = 0
		self.duration = 0
		# computed
		self.runaction = None

class TaskStat:
	def __init__(self,id,rid):
		self.taskid = id
		self.runtaskid = rid
		self.worst = 0
		self.average = 0
		self.samples = 0


def loadlog(logfile,actionlist):
	"""Loads the log file into a RunActionLog list"""
	#LOG = action index endtime(float) duration(float)
	# space sep values with # comments
	#onf << (int)(p.action) << " " << i << " " << relend.count() << " " << duration.count() << std::endl;;

	for x in logfile:
		x = x.strip()
		if x.startswith("#"):
			continue
		else:
			a = x.split(" ")
			va = int(a[0])
			vi = int(a[1])
			vendtime = float(a[2])
			vduration = float(a[3])

			t = RunActionLog()
			t.index = vi
			t.action = va
			t.runaction = actionlist[t.index]
			t.endtime = vendtime
			t.duration = vduration
			r.append(t)
	return r

def makestats(lf):
	tasks = {}
	for f in lf:
		for t in lf:
			if t.action == sched2run.Actions.TASK:
				n = t.runaction.params[1]-t.runaction.params[0]
				d = t.duration/n
			elif t.action == sched2run.Actions.TASKPAR:
				n = 1
			l = tasks.get(t.runaction.taskid)
			if l is None:
				l = TaskStat(t.runaction.taskid,t.runaction.id)
				tasks[t.taskid] = l
			l.samples += 1
			l.worst = max(d,l.worst)
			# or running mean
			l.average += d
	for v in tasks.values():
		v.average /= v.samples
	return tasks

				
if __name__ == "__main__":
	import argparse

	parser = argparse.ArgumentParser(description='Process log files of Task Runner')
	parser.add_argument("logfile",nargs="+",dest="logfiles",help="list of log files to be used")
	parser.add_argument("--run",required=True,help="run filename")
	parser.add_argument("--graph",help="graph filename for updating the costs")
	parser.add_argument("--info",action="store_true",help="details of log")
	parser.add_argument("--stats",action="store_true",help="compute task stat from logs")
	parser.add_argument("--update",action="store_true",help="update graph from stats")
	parser.add_argument("--outgraph",help="output graph",default="-")
	args = parser.parse_args()

	lr = sched2run.RunAction.fromfile(args.run)
	lf = [loadlog(n,lr) for n in args.logfiles]

	if args.graph.endswith(".json"):
		gg = sched.loadtasksjson(open(args.graph,"rb"))
	elif args.graph.endswith(".dot"):
		gg = sched.loadtasksdot(open(args.graph,"rb"))
	elif args.graph != "":
		print "unknown graph file",args.graph
		sys.exit(-1)
	else:
		gg = None

	if args.stats or args.update:
		ts = makestats(lf)
	else:
		ts = {}

	if args.update:
		if gg is None:
			print "graph needed for update"
			sys.exit(-1)
		for t in gg:
			tis = ts.get(t.id)
			if tis is None:
				print "no stats for task",t.id
			else:
				t.cost = tis.worst * t.items

	if args.info:
		print "# action taskid duration items",
		for i,f in enumerate(lf):
			print "#",args.logfile[i]
			for t in f:
				if t.runaction.action == sched2run.Actions.TASK or t.runaction.action == sched2run.Actions.TASKPAR:
					n = t.runaction.params[1]-t.runaction.params[0]
				else:
					n = 0
				print t.runaction.action,t.runaction.id,t.duration,n

	if args.stats:
		print "taskid,runtaskid,worst,avg,samples"
		for v in ts.values():
			print v.taskid,v.runtaskid,v.worst,v.average,v.samples
		
	if args.update:
		if args.outgraph == "-":
			e = os.path.splitext(args.graph)[1]
			of = sys.stdout
		else:
			e = os.path.splitext(args.outgraph)[1] 
			of = open(args.outgraph,"wb")

		if e == ".dot":
			savetasksdot(gg,of)
		elif e == ".json":
			savetasksjson(gg,of)
		else:
			print "unknown output extension for graph",e

		



