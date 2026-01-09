# MapReduce Library

A simple, thread-safe library for parallel data processing on a single machine.

## What is MapReduce?

MapReduce processes large amounts of data in parallel using two simple functions:
- **Map**: Process input files and emit key-value pairs
- **Reduce**: Combine all values for each key

The library handles all threading, synchronization, and data management automatically.

## Why Use This?

- **Easy**: Write 2 functions instead of managing threads yourself
- **Fast**: Automatically uses multiple CPU cores
- **Safe**: Thread-safe with no race conditions

## Quick Start

### 1. Write Your Code

```c
#include "mapreduce.h"

// Process one file
void Map(char *file_name) {
    FILE *fp = fopen(file_name, "r");
    char *line = NULL;
    size_t size = 0;
    
    while (getline(&line, &size, fp) != -1) {
        char *word, *str = line;
        while ((word = strsep(&str, " \t\n\r")) != NULL) {
            if (*word != '\0') {
                MR_Emit(word, "1");  // Emit word
            }
        }
    }
    free(line);
    fclose(fp);
}

// Count all values for one key
void Reduce(char *key, Getter get_next, int partition_number) {
    int count = 0;
    char *value;
    while ((value = get_next(key, partition_number)) != NULL) {
        count++;
    }
    printf("%s %d\n", key, count);
}

int main(int argc, char *argv[]) {
    MR_Run(argc, argv, Map, 10, Reduce, 10, MR_DefaultHashPartition);
    return 0;
}
```

### 2. Compile and Run

```bash
gcc -o wordcount wordcount.c -pthread -O2
./wordcount file1.txt file2.txt file3.txt
```

## API

### `MR_Run(argc, argv, Map, num_mappers, Reduce, num_reducers, MR_DefaultHashPartition)`
Starts the MapReduce job.
- `Map`: Your function to process files
- `num_mappers`: Number of mapper threads (typically 4-8)
- `Reduce`: Your function to combine values
- `num_reducers`: Number of reducer threads (typically 4-8)

### `MR_Emit(key, value)`
Call from Map to emit a key-value pair.
- Both key and value are strings
- Can be called from multiple threads safely

### `get_next(key, partition_number)`
Call from Reduce to get the next value for a key.
- Returns value string or NULL when done

## How It Works

```
Files → [Map Threads] → Group by Key → Sort → [Reduce Threads] → Output
```

1. **Map Phase**: Multiple threads process files in parallel
2. **Shuffle**: Library groups all values by key automatically
3. **Reduce Phase**: Multiple threads process keys in parallel

## Examples

### Word Count
```c
Map: Emit each word with value "1"
Reduce: Count how many "1"s for each word
```

### Find Pattern
```c
Map: Emit filename if line contains pattern
Reduce: Print all matching line numbers per file
```

### Count URLs
```c
Map: Emit URL from log line
Reduce: Count occurrences of each URL
```

## Tips

**Choose thread count:**
- Use 4-8 mappers and reducers for most cases
- Match your CPU core count for best performance

**In Map function:**
- Always check if file opened successfully
- Free any memory you allocate
- Use `MR_Emit()` to send data to reducers

**In Reduce function:**
- Call `get_next()` in a loop until it returns NULL
- Process all values for the key

## Common Issues

**Segmentation fault?**
- Check if file opened: `if (fp == NULL) return;`
- Free memory you allocated

**Wrong counts?**
- Filter empty strings: `if (*word != '\0')`

**Slow performance?**
- Try different thread counts (4, 8, 16)
- Make sure Map/Reduce functions are efficient

## Thread Safety

✓ Multiple mappers can run simultaneously  
✓ `MR_Emit()` is thread-safe  
✓ Memory is cleaned up automatically  
✓ No race conditions

## Requirements

- C compiler (gcc)
- POSIX threads library

Compile with: `gcc -pthread -O2`
