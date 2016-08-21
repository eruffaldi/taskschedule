#http://ocw.mit.edu/courses/mathematics/18-337j-parallel-computing-fall-2011/projects/MIT18_337JF11_FFT_pres.pdf
#       ==> https://courses.engr.illinois.edu/cs554/fa2015/notes/13_fft_8up.pdf
# - Binary Exchange Algorithm: 
#       http://parallelcomp.uw.hu/ch13lev1sec2.html
#       paper: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.43.1823&rep=rep1&type=pdf
# - Transpose Algorithm 
#
# DFT of n-point sequence can be computed by breaking it into two DFTs of half length, provided n is even
#   at level n:
#       - distribute
#       - fft odd/even
#       - combine
#
# Properties
# - log(n) levels of computing with Theta(n log n)
# - in-place
# - complex ASSUMPTION
# - need to reorder final stage in logn
# - iterative formulation
#
# Binary Exchange
# Transpose Algorithm: 
#
# n = pow2, p = pow2
# - More focus on minimizing communication overhead versus computational optimizations 
from sched import *

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Generator for FFT - Emanuele Ruffaldi 2016 SSSA')
    parser.add_argument('--size',help='log of elements n',type=int)
    parser.add_argument('--groupsize',help='log p of group size p',type=int)
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