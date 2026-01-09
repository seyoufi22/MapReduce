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
	int size, curIdx, GetterIdx;
} KeyListPair;


// Partitions Implementation
typedef struct{
	KeyListPair *Keylist;
	int size, CurIdx;
	pthread_mutex_t lock;
} Partitions;

// Global Variables
int num_partitions;
Partitions *part;
Partitioner partition_func;

// Thread pool structures
typedef struct {
    char **files;
    int num_files;
    int current_file;
    pthread_mutex_t file_lock;
    Mapper map_func;
} MapperArgs;

typedef struct {
    int partition_id;
    Reducer reduce_func;
    Getter get_func;
} ReducerArgs;

void initPartitions(int num_partition){
	part[num_partition].CurIdx = -1;
	part[num_partition].size = MAX_SIZE;
	part[num_partition].Keylist = (KeyListPair *)malloc(sizeof(KeyListPair) * part[num_partition].size);
	pthread_mutex_init(&part[num_partition].lock, NULL);
}

void initKeyListPair(int num_partition, int KeyIdx){
	part[num_partition].Keylist[KeyIdx].curIdx = -1;
	part[num_partition].Keylist[KeyIdx].GetterIdx = 0;
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


// Sorting
int compareKey(const void *a, const void *b) {
    const KeyListPair *pairA = (const KeyListPair *)a;
    const KeyListPair *pairB = (const KeyListPair *)b;

	return strcmp(pairA->key, pairB->key);
}

int compareValue(const void *a, const void *b) {
    const char *valueA = *(const char **)a;
    const char *valueB = *(const char **)b;

	return strcmp(valueA, valueB);
}

void SortPartitionKeyList(int num_partition){
	qsort(part[num_partition].Keylist, part[num_partition].CurIdx + 1, sizeof(KeyListPair), compareKey);
}

void SortValueList(int num_partition, int KeyIdx){
	int size = part[num_partition].Keylist[KeyIdx].curIdx + 1;
	qsort(part[num_partition].Keylist[KeyIdx].Valuelist, size, sizeof(char *), compareValue);
}

// --------------------------------------------------------------




char *get_next(char *key, int partition_number) {
    Partitions *p = &part[partition_number];
    for (int i = 0; i <= p->CurIdx; i++) {
        if (strcmp(p->Keylist[i].key, key) == 0) {
            KeyListPair *pair = &p->Keylist[i];
            if (pair->GetterIdx <= pair->curIdx) {
                return pair->Valuelist[pair->GetterIdx++];
            } else {
                pair->GetterIdx = 0;  // Reset for potential future calls
                return NULL;
            }
        }
    }
    return NULL;
}


// External functions: these are what you must define
unsigned long MR_DefaultHashPartition(char *key, int num_partitions){
	unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void MR_Emit(char *key, char *value){
	char *new_key = (char *)malloc(strlen(key) + 1);
	strcpy(new_key, key);
	char *new_value = (char *)malloc(strlen(value) + 1);
	strcpy(new_value, value);

	int num_partition = partition_func(new_key, num_partitions);

	pthread_mutex_lock(&part[num_partition].lock);

	int Keyidx = -1;
	for(int i = 0; i <= part[num_partition].CurIdx; i++){
		if (strcmp(part[num_partition].Keylist[i].key, new_key) == 0){
			Keyidx = i;
			break;
		}
	}

	// Not Found the Key
	if (Keyidx == -1){
		push_back_NewKey(num_partition, new_key);
		Keyidx = part[num_partition].CurIdx;
	} else {
		free(new_key);  // Key already exists, free the duplicate
	}
	push_back_NewValue(num_partition, Keyidx, new_value);

	pthread_mutex_unlock(&part[num_partition].lock);
}

// Mapper thread function
void *mapper_thread(void *arg) {
    MapperArgs *args = (MapperArgs *)arg;
    
    while (1) {
        pthread_mutex_lock(&args->file_lock);
        if (args->current_file >= args->num_files) {
            pthread_mutex_unlock(&args->file_lock);
            break;
        }
        char *file = args->files[args->current_file++];
        pthread_mutex_unlock(&args->file_lock);
        
        args->map_func(file);
    }
    
    return NULL;
}

// Reducer thread function
void *reducer_thread(void *arg) {
    ReducerArgs *args = (ReducerArgs *)arg;
    int partition_id = args->partition_id;
    
    for (int j = 0; j <= part[partition_id].CurIdx; j++) {
        args->reduce_func(part[partition_id].Keylist[j].key, args->get_func, partition_id);
    }
    
    return NULL;
}


void MR_Run(int argc, char *argv[], 
	    Mapper map, int num_mappers, 
	    Reducer reduce, int num_reducers, 
	    Partitioner partition)
{
	partition_func = partition;

	//Process Map Phase
	num_partitions = num_reducers;
	part = (Partitions *)malloc(sizeof(Partitions) * num_partitions);
	for(int i = 0; i < num_partitions; i++){
		initPartitions(i);
	}

	// Create mapper threads
	pthread_t *mapper_threads = (pthread_t *)malloc(sizeof(pthread_t) * num_mappers);
	MapperArgs mapper_args;
	mapper_args.files = &argv[1];
	mapper_args.num_files = argc - 1;
	mapper_args.current_file = 0;
	mapper_args.map_func = map;
	pthread_mutex_init(&mapper_args.file_lock, NULL);

	for (int i = 0; i < num_mappers; i++) {
		pthread_create(&mapper_threads[i], NULL, mapper_thread, &mapper_args);
	}

	// Wait for all mapper threads to complete
	for (int i = 0; i < num_mappers; i++) {
		pthread_join(mapper_threads[i], NULL);
	}

	pthread_mutex_destroy(&mapper_args.file_lock);
	free(mapper_threads);

	//Intermediate Sorting Phase
	for(int i = 0; i < num_partitions; i++){
		SortPartitionKeyList(i);
		for(int j = 0; j <= part[i].CurIdx; j++){
			SortValueList(i, j);
		}
	}

	//Process Reduce Phase
	Getter get_func = &get_next;
	
	pthread_t *reducer_threads = (pthread_t *)malloc(sizeof(pthread_t) * num_reducers);
	ReducerArgs *reducer_args = (ReducerArgs *)malloc(sizeof(ReducerArgs) * num_reducers);

	for (int i = 0; i < num_reducers; i++) {
		reducer_args[i].partition_id = i;
		reducer_args[i].reduce_func = reduce;
		reducer_args[i].get_func = get_func;
		pthread_create(&reducer_threads[i], NULL, reducer_thread, &reducer_args[i]);
	}

	// Wait for all reducer threads to complete
	for (int i = 0; i < num_reducers; i++) {
		pthread_join(reducer_threads[i], NULL);
	}

	free(reducer_threads);
	free(reducer_args);

	// Cleanup memory
	for (int i = 0; i < num_partitions; i++) {
		for (int j = 0; j <= part[i].CurIdx; j++) {
			// Free all values in the value list
			for (int k = 0; k <= part[i].Keylist[j].curIdx; k++) {
				free(part[i].Keylist[j].Valuelist[k]);
			}
			// Free the value list array
			free(part[i].Keylist[j].Valuelist);
			// Free the key
			free(part[i].Keylist[j].key);
		}
		// Free the key list array
		free(part[i].Keylist);
		// Destroy the mutex
		pthread_mutex_destroy(&part[i].lock);
	}
	// Free the partitions array
	free(part);
	
}

#endif // __mapreduce_h__