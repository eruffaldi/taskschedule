#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>

class MTaskEdge;
class MTask;
class MProcTask;
class MProc;

struct MTaskEdge:
{
    /*Edge of a M-Task with edge cost

    Is reduction means that the destination task is a reduction task of a data-parallel map-reduction task. This means that if the map-reduction task has
    Np == 1 then we can consider the reduction task as 0 cost and skippable

    When instead we split the map-reduction we need to 
    */
    MTask * source = 0;
    MTask * dest = 0;
    float delay = 0;
    bool isreduction = false;
};

struct MRangeSplit
{
	int istart = 0,iend = 0,target = 0;
} ;

struct MProcTask
{
	MProc * proc = 0;
	MTask * task = 0;
	float begin=0,end = 0;
	MRangeSplit rangesplit=0;
};

struct MTask
{
	std::string id;
	std::vector<MTaskEdge> parents;
	int items = 1;
	int maxnp = 0;
	float cost = 1;
	float deadline = 0;
	float reductioncost = 0;
	bool doesreduction = 0;
	bool evennp = 0;
	float ucost = 0;
	int Np; = 1;

	unordered_set<MTaskEdge> sparents;
	std::vector<MTask> children;
	int top,slevel,botom,cdeadline;
	float endtime,earlieststart,lateststart;
	unordered_set<MProcTask> proc;

	void updateNp(int n);
	void setOneRun(float tstart);
};

struct MProc
{
	int index = 0;
	std::vector<MProcTask> tasks;
	unordered_set<MTask> stasks;
	float next = 0;

	void addTask(MTask *, float begin, float end);
	void clear();
};

