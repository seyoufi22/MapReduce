#ifndef __mapreduce_h__
#define __mapreduce_h__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define MAX_SIZE 100

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);


// Key/Value Struct
typedef struct {
    char *key, *value;
} KeyValuePair;



// Dynamic Array Implementation
typedef struct {
    KeyValuePair *list;
    int size, CurIdx;
} dynamicArray;


void initDynamicArray(dynamicArray *da){
	da->CurIdx = -1;
	da->size = MAX_SIZE;
	da->list = (KeyValuePair *)malloc(sizeof(KeyValuePair) * da->size);
}

void IncreaseSize(dynamicArray *da){
	KeyValuePair *New = (KeyValuePair *)malloc(sizeof(KeyValuePair) * (da->size + MAX_SIZE));
	
	for(int i = 0; i < da->size; i++){
		New[i] = da->list[i];
	}
	
	free(da->list);
	da->list = New;

	da->size += MAX_SIZE;
}

void push_back(dynamicArray *da, KeyValuePair *node){
	if (da->CurIdx == da->size - 1)IncreaseSize(da);
	da->list[++da->CurIdx] = *node;
}
// --------------------------------------------------------------


// Dynamic Array Sorting
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
void MR_Emit(char *key, char *value){
	KeyValuePair *NewNode = (KeyValuePair *)malloc(sizeof(KeyValuePair));
	NewNode->key = key;
	NewNode->value = value;
	free(NewNode);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
	unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition){
	
	
		
}

#endif // __mapreduce_h__
