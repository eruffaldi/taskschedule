import sched
import argparse

class ToggleAction(argparse.Action):
    def __call__(self, parser, ns, values, option):
        setattr(ns, self.dest, bool("-+".index(option[0])))

def main():
    parser = argparse.ArgumentParser(description='Generator for MergeSort - Emanuele Ruffaldi 2016 SSSA',prefix_chars='-+')
    parser.add_argument('--size',help='number of elements',type=int,default=16)
    parser.add_argument('-parallel', '+parallel', default=False,action=ToggleAction, nargs=0)
    args = parser.parse_args()

    #assume common buffer of size N
    #model as a multiple stage map reduce: every map does the split, reduction is the merge
    #   
    #fully decomposed model: N/2 tasks ... => complex, heavy for N high
    #parallel model: task with N/2 items ... => requires that parallel assignment is EVEN + possibly that we follow affinity
    
    n = args.size
    if args.parallel:
        tasks = []
        remainder = n & 1
        pre = None
        while n > 0:
            s = remainder and n+1 or n
            t = sched.MTask(id="p%d" % s, cost=s, items=s, maxnp=s)
            tasks.append(t)
            if pre is not None:
                t.parents.append(sched.MTaskEdge(pre,t,0))
            remainder = n & 1
            n = n / 2
            pre = t
        sched.savetasksjson(tasks,open("mergesort.json","wb"))
    else:
        # first produce n/2 tasks
        tasks = []
        pre = []
        size = 2
        remainder = n & 1
        while n > 0:
            ptasks = []
            print "doing",n,len(pre)
            s = remainder and n+1 or n 
            for i in range(0,s,2):
                t = sched.MTask("s%dp%d" % (size,i/2),cost=size,items=size,maxnp=1)
                if len(pre) > 0:
                    print " n=",n," i=",(i,i+1)," size=",size
                    t.parents.append(sched.MTaskEdge(pre[i],t,0))
                    if i+1 < len(pre):
                        t.parents.append(sched.MTaskEdge(pre[i+1],t,0))
                ptasks.append(t)
                tasks.append(t)
            pre = ptasks
            size = size*2
            remainder = n & 1
            n = n / 2
        print "\n".join([t.id for t in tasks])
        sched.savetasksjson(tasks,open("mergesort.json","wb"))
if __name__ == '__main__':
    main()