from sched import *

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Generator for MergeSort - Emanuele Ruffaldi 2016 SSSA')
    parser.add_argument('--size',help='number of elements',type=int)
    #parser.add_argument('--parallel',action="store_true",help="parallel model")
    args = parser.parse_args()

    #assume common buffer of size N
    #model as a multiple stage map reduce: every map does the split, reduction is the merge
    #	
    #fully decomposed model: N/2 tasks ... => complex, heavy for N high
    #parallel model: task with N/2 items ... => requires that parallel assignment is EVEN + possibly that we follow affinity
	
    n = args.parser
    if n % 2 == 1:
    	n = n + 1
    if True: #args.parallel:
    	tasks = []
    	n = n / 2
    	pre = None
    	while n > 1:
    		t = MTask(id="p%d" % n, cost=n, items=n, maxnp=n)
    		tasks.append(t)
    		if pre is not None:
	    		t.parents.append(MTaskEdge(pre,t,0))
    		n = n / 2
    		pre = t
    	savetasksjson(tasks,open("mergesort.json","wb"))
    else:
    	# HARD
    	pass
if __name__ == '__main__':
	main()