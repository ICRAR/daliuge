#include <sys/time.h>
#include <string.h>
#include <stdio.h>

#define TICTAK_MAX_POINTS 256
#define TICTAK_MAX_GROUPS 32
#define TICTAK_MAX_TAGCHAR 256 

struct CheckPoint{
	struct timeval time;
	char tag[TICTAK_MAX_TAGCHAR];
};

struct CheckPoint points[TICTAK_MAX_GROUPS][TICTAK_MAX_POINTS];
int cindex[TICTAK_MAX_GROUPS]={0};

float tictak_total(int group){

	if (group >= TICTAK_MAX_GROUPS)
		return -1;

	int index= cindex[group];
	struct CheckPoint *pointsp= points[group];

	float total = pointsp[index-1].time.tv_sec - pointsp[0].time.tv_sec + 1.0/1000000.0 * (pointsp[index-1].time.tv_usec - pointsp[0].time.tv_usec);

	return total;

}

int tictak_add(char tag[],int group){

	if (group >= TICTAK_MAX_GROUPS && cindex[group] >= TICTAK_MAX_POINTS)
		return -1;

	int index= cindex[group];
	struct CheckPoint *pointsp= points[group];
	strcpy(pointsp[index].tag, tag);

	gettimeofday(&pointsp[index].time,0);

	cindex[group]=cindex[group]+1;
	return 0;
}

int tictak_dump(int group){

	if (group >= TICTAK_MAX_GROUPS)
		return -1;

	int index= cindex[group];
	struct CheckPoint *pointsp= points[group];

	int i;
	float total = pointsp[index-1].time.tv_sec - pointsp[0].time.tv_sec + 1.0/1000000.0 * (pointsp[index-1].time.tv_usec - pointsp[0].time.tv_usec);

	printf("group=%d, total, %f seconds\n",group,total);
	for(i=0; i<index-1; i++)
	{
		float sub=pointsp[i+1].time.tv_sec - pointsp[i].time.tv_sec + 1.0/1000000.0 * (pointsp[i+1].time.tv_usec - pointsp[i].time.tv_usec);
		printf("group=%d, tag=%s, %f seconds, per=%f%%\n", group, pointsp[i].tag, sub, sub/total*100.0);
	}

	printf("\n\n");
	cindex[group]=0;

	return 0;
}
