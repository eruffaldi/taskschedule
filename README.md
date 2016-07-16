
Scheduling of task with dependencies (M-Task model with DAG)
=========

This program takes as input a Graphviz file or a JSON specifying tasks with their dependencies and it produces a scheduling.

So far it supports Critical Path Reduction (CPR) only method by Radulescu 2001.

The M-Task model comprises:
- identifier of task
- input task from which it depends
- cost
- maximum number of processors (extended wrt CPR)
- cost of edges between nodes (accounting for data transfer)

See examples for testing

Command Line
===========	
	usage: sched.py [-h] [--algorithm ALGORITHM] [--cores CORES] [--verbose]
	                [--earliest] [--usefloats] [--allunicore] [--samezerocost]
	                [--transitive] [--output OUTPUT]
	                input

	positional arguments:
	  input                 input file

	optional arguments:
	  -h, --help            show this help message and exit
	  --algorithm ALGORITHM
	                        chosen algorithm: cpr none
	  --cores CORES         number of cores for the scheduling
	  --verbose
	  --earliest            uses earliest instead of bottom-level for the MLS
	  --usefloats           compute using floats instead of fractions
	  --allunicore          all tasks cannot be split
	  --samezerocost        skip edge cost for same processor edges
	  --transitive          transitive reduction (activates --samezerocost)
	  --output OUTPUT       JSON output of scheduling


References
=========
Dumler et al., A Scheduling Toolkit for Multiprocessor-Task Programming with Dependencies, EUROPAR (2007)

Radulescu, A., Nicolescu, C., van Gemund, A., Jonker, P.: CPR: Mixed Task and Data Parallel Scheduling for Distributed Systems. In: IPDPS ’01: Proc. of the 15th Int. Par. & Distr. Processing Symp., IEEE Computer Society (2001) 39