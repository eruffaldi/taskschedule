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
	r = []
	for x in logfile:
		x = x.strip()
		if x.startswith("#"):
			continue
		else:
			a = x.split(" ")
			va = int(a[0])
			vi = int(a[1])
			if vi < 0:
				# special log entry
				continue
			vendtime = float(a[2])
			vduration = float(a[3])
			aa = actionlist[vi]
			if va != aa.action:
				print "log and run action mismatch for",va,aa.action,"line",x
				continue

			t = RunActionLog()
			t.index = vi
			t.action = va
			t.runaction = aa
			t.endtime = vendtime
			t.duration = vduration
			r.append(t)
	return r

def makestats(lf):
	tasks = {}
	for f in lf:
		for t in f:
			if t.action == sched2run.Actions.RUNTASK:
				n = 1
				d = t.duration / n
			elif t.action == sched2run.Actions.RUNTASKPAR:
				n = t.runaction.params[1]-t.runaction.params[0]
				d = t.duration
			else:
				continue
			if t.runaction.taskid is None:
				print "not associated taskid for runtask",t.runaction.id
				continue
			l = tasks.get(t.runaction.taskid)
			if l is None:
				l = TaskStat(t.runaction.taskid,t.runaction.id)
				tasks[t.runaction.taskid] = l
			l.samples += 1
			l.worst = max(d,l.worst)
			# or running mean
			l.average += d

	for v in tasks.values():
		v.average /= v.samples
	return tasks

				
if __name__ == "__main__":
	import argparse
	sched.makenumbers = float


	parser = argparse.ArgumentParser(description='Process log files of Task Runner')
	parser.add_argument("logfiles",nargs="+",help="list of log files to be used")
	parser.add_argument("--run",required=True,help="run filename")
	parser.add_argument("--graph",help="graph filename for updating the costs")
	parser.add_argument("--info",action="store_true",help="details of log")
	parser.add_argument("--stats",action="store_true",help="compute task stat from logs")
	parser.add_argument("--update",action="store_true",help="update graph from stats")
	parser.add_argument("--compare",action="store_true",help="compare times from logs with graph times")
	parser.add_argument("--outgraph",help="output graph",default="-")
	args = parser.parse_args()

	lr = sched2run.RunAction.fromfile(open(args.run,"rb"))
	lf = [loadlog(open(n,"rb"),lr) for n in args.logfiles]

	if args.graph is not None:
		if args.graph.endswith(".json"):
			gg = sched.loadtasksjson(open(args.graph,"rb"))
		elif args.graph.endswith(".dot"):
			gg = sched.loadtasksdot(open(args.graph,"rb"))
		elif args.graph != "":
			print "unknown graph file",args.graph
			sys.exit(-1)
	else:
		gg = None

	if args.stats or args.update or args.compare:
		ts = makestats(lf)
	else:
		ts = {}

	if args.update or args.compare:
		if gg is None:
			print "graph needed for update and compare"
			sys.exit(-1)
		if args.update:
			for t in gg:
				tis = ts.get(t.id)
				if tis is None:
					print "no stats for task",t.id
				else:
					t.cost = tis.worst * t.items
		if args.compare:
			print "# runtaskid originalcost worst avg samples taskid"
			for t in gg:
				v = ts.get(t.id)
				if tis is None:
					print "#no stats for task",t.id
				print v.runtaskid,t.cost,v.worst,v.average,v.samples,v.taskid

	if args.info:
		for i,f in enumerate(lf):
			print "##",args.logfiles[i]
			print "# runtaskid action duration items taskid"
			for t in f:
				if t.runaction.action == sched2run.Actions.RUNTASK:
					n = 1
				elif t.runaction.action == sched2run.Actions.RUNTASKPAR:
					n = t.runaction.params[1]-t.runaction.params[0]
				else:
					continue
				print t.runaction.id,t.runaction.action,t.duration,n,t.runaction.taskid


	if args.stats:
		print "# taskid runtaskid worst avg samples"
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

		



