#ifndef __mapreduce_h__
#define __mapreduce_h__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define MAX_SIZE 10


// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);


// Key/List struct and methods
typedef struct{
	char *key, **Valuelist;
	int size, curIdx;
} KeyListPair;


// Partitions Implementation
typedef struct{
	KeyListPair *Keylist;
	int size, CurIdx;
} Partitions;

// Global Variables
int num_partitions;
Partitions *part;

void initPartitions(int num_partitions){
	part->CurIdx = -1;
	part->size = MAX_SIZE;
	part->Keylist = (KeyListPair *)malloc(sizeof(KeyListPair) * part->size);
}

void initKeyListPair(int num_partition, int KeyIdx){
	part[num_partition].Keylist[KeyIdx].curIdx = -1;
	part[num_partition].Keylist[KeyIdx].size = MAX_SIZE;
	part[num_partition].Keylist[KeyIdx].Valuelist = (char **)malloc(sizeof(char *) * part[num_partition].Keylist[KeyIdx].size);
}


void Increase_Key_Value_List_Size(int num_partition, int KeyIdx){
	char **NewValuelist = (char **)malloc((part[num_partition].Keylist[KeyIdx].size + MAX_SIZE) * sizeof(char *));
	for(int i = 0; i <= part[num_partition].Keylist[KeyIdx].curIdx; i++){
		NewValuelist[i] = part[num_partition].Keylist[KeyIdx].Valuelist[i];
	}
	free(part[num_partition].Keylist[KeyIdx].Valuelist);
	part[num_partition].Keylist[KeyIdx].Valuelist = NewValuelist;
	part[num_partition].Keylist[KeyIdx].size += MAX_SIZE;
}

void Increase_Partition_Key_List_Size(int num_partition){
	KeyListPair *NewKeylist = (KeyListPair *)malloc((part[num_partition].size + MAX_SIZE) * sizeof(KeyListPair));
	for(int i = 0; i <= part[num_partition].CurIdx; i++){
		NewKeylist[i] = part[num_partition].Keylist[i];
	}
	free(part[num_partition].Keylist);
	part[num_partition].Keylist = NewKeylist;
	part[num_partition].size += MAX_SIZE;
}

void push_back_NewKey(int num_partition, char *Key){
	//Check If there is space
	if (part[num_partition].CurIdx == part[num_partition].size - 1){
		Increase_Partition_Key_List_Size(num_partition);
	}

	part[num_partition].Keylist[++part[num_partition].CurIdx].key = Key;
	initKeyListPair(num_partition, part[num_partition].CurIdx);
}


void push_back_NewValue(int num_partition, int KeyIdx, char *value){
	int *CurValueListIdx = &part[num_partition].Keylist[KeyIdx].curIdx,
		*CurValueListSize = &part[num_partition].Keylist[KeyIdx].size;
	
	//Check If there is space	
	if (*CurValueListIdx == *CurValueListSize - 1){
		Increase_Key_Value_List_Size(num_partition, KeyIdx);
	}

	part[num_partition].Keylist[KeyIdx].Valuelist[++*CurValueListIdx] = value;	
}

// -----------------------------------------------------------------------------------------------

// TBD
// Partitions Key Sorting
int compare(const void *a, const void *b) {
    const KeyValuePair *pairA = (const KeyValuePair *)a;
    const KeyValuePair *pairB = (const KeyValuePair *)b;

    // Compare the Keys first
    int keyCompare = strcmp(pairA->key, pairB->key);

    // If keys are different, return the result immediately
    if (keyCompare != 0) {
        return keyCompare;
    }

    //If keys are equal, compare Values
    return strcmp(pairA->value, pairB->value);
}

void SortDynamicArray(dynamicArray *da){
	qsort(da->list, da->CurIdx + 1, sizeof(KeyValuePair), compare);
}
// --------------------------------------------------------------






// External functions: these are what you must define
unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
	unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Emit(char *key, char *value){
	int num_partition = MR_DefaultHashPartition(key, num_partitions);

	int Keyidx = -1;
	for(int i = 0; i <= part[num_partition].CurIdx; i++){
		if (strcmp(part[num_partition].Keylist[i].key, key) == 0){
			Keyidx = i;
			break;
		}
	}

	// Not Found the Key
	if (Keyidx == -1){
		push_back_NewKey(num_partition, key);
		Keyidx = part[num_partition].CurIdx;
	}
	push_back_NewValue(num_partition, Keyidx, value);
}



void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition)
{
	if (argc < 2){
		perror("Please enter input files\n");
	}

	
	// Process Map function


	SortDynamicArray(da);
	// Convert Key/Value to Key/List
	int num_partitions = num_reducers;
	KeyListPair *partitions[num_partitions];
	for(int i = 0; i <= da->CurIdx; i++){
		unsigned long num_partition = MR_DefaultHashPartition(da->list[i].key, num_partitions);
	}
}

#endif // __mapreduce_h__
