
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
	                [--earliest] [--usefloats] [--allunicore]
	                input

	Scheduling Tester

	positional arguments:
	  input                 input file

	optional arguments:
	  -h, --help            show this help message and exit
	  --algorithm ALGORITHM
	                        chosen algorithm: cpr none
	  --cores CORES         number of cores
	  --verbose
	  --earliest            uses earliest instead of bottom for the MLS
	  --usefloats
	  --allunicore


References
=========
Dumler et al., A Scheduling Toolkit for Multiprocessor-Task Programming with Dependencies, EUROPAR (2007)

Radulescu, A., Nicolescu, C., van Gemund, A., Jonker, P.: CPR: Mixed Task and Data Parallel Scheduling for Distributed Systems. In: IPDPS ’01: Proc. of the 15th Int. Par. & Distr. Processing Symp., IEEE Computer Society (2001) 39