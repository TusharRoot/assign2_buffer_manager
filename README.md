# Assignment 2: Buffer Manager

- Yousef Suleiman A20463895

## Building

To build the buffer manager as well the first set of test cases in `test_assign2_1.c`, use

```sh
make
./test_assign2_1.o
```

To build the second set of test cases in `test_assign2_2.c`, use

```sh
make test_assign2_2
./test_assign2_2.o
```

There is a third test case made to test some simple functionality. To build this set of test cases in `test_assign2_3.c`, use

```sh
make test_assign2_3
./test_assign2_3.o
```

There is also another set of tests for a hash table coded for this assignment in `hash_table.c`. To build this set of test cases in `test_hash_table.c`, use

```sh
make test_hash_table
./test_hash_table.o
```

To clean the solution use

```sh
make clean
```

## Explanation of Solution

### Hash Table

A hash table was created (header file `hash_table.h` and implement in `hash_table.c`) to implement the page table. It maps `int -> int` using modulo to index into a set of flexible array lists.

### Additional Definitions

These definitions are internal and will not be exposed to the user.

```c
typedef unsigned int TimeStamp;
```

- instead of using something like `time.h`,  we simply used an `int` counter to keep track of timestamps

```c
typedef struct BM_PageFrame;
```

- this internal `struct` is used to store a frame's
  - `char*` data pointer
  - the page number associated with the frame
  - the frame index in the array of frames
  - the `fixCount` or number of pins
  - dirty and occupied bools
  - as well as a time stamp

```c
typedef struct BM_Metadata;
```

- this internal `struct`'s data is stored into the `BM_BufferPool`'s `mgmtData`. It holds
  - the page frame array of `BM_PageFrame`
  - a page table (using `hash_table.h`)
  - the page file handle 
  - a global time stamp (this will be used to assign time stamps to individual frames)
  - an index `queueIndex` that is used to treat the page frame array as a queue by cycling over its unpinned pages
  - as well as statistical counters for number of reads and writes

### Buffer Manager Interface Pool Handling

```c
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
```

- initializes a buffer pool by setting up metadata and data structures
- allocates memory for page frames and initializes them with default values

```c
RC shutdownBufferPool(BM_BufferPool *const bm)
```

- shuts down a buffer pool checking that there are no pinned pages
- writes dirty pages back to disk (using `forceFlushPool`), frees memory, and closes the page file

```c
RC forceFlushPool(BM_BufferPool *const bm)
```

- writes all dirty & unpinned pages to disk
- it also increments the timestamp for all pages in the order it flushes

### Buffer Manager Interface Access Pages

```c
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
```

- marks page as dirty and increments timestamp

```c
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
```

- unpins the page by decrementing `fixCount` and increments timestamp

```c
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
```

- writes the page to disk (regardless if dirty) only if the page is not pinned (i.e. `fixCount == 0`)
- increments timestamp
- clears dirty bool

```c
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
```

- checks if the page is already in memory using the page table and updates the page handle
- if its not there, it will use either the FIFO or LRU replacement policy
  - these are defined in `replacementFIFO` and `replacementLRU`
- if the policies return `NULL` pointers it indicates no pages could be evicted as they are all occupied & pinned
- otherwise, if it is not `NULL`, the page was successfully evicted & written to disk (if dirty) such that it can be used

### Statistics Interface

```c
PageNumber *getFrameContents (BM_BufferPool *const bm)
bool *getDirtyFlags (BM_BufferPool *const bm)
int *getFixCounts (BM_BufferPool *const bm)
```

- these functions simply iterate over the page frame array and assign values to an dynamically allocated array for statistics
- the user is responsible for calling free on the returned array

```c
int getNumReadIO (BM_BufferPool *const bm)
int getNumWriteIO (BM_BufferPool *const bm)
```

- these functions simply return the associated counters
- note that the counters are incremented after `writeBlock` or `readBlock` is called

### Replacement Policies

These 2 functions will use the specified replacement policy to find an available frame or evict the frame's page (writing to disk if needed). They will will also deal with incrementing the timestamp for an evicted frame. The caller will be responsible for setting the metadata (i.e. in `pinPage`).

```c
BM_PageFrame *replacementFIFO(BM_BufferPool *const bm)
```

- keep cycling the `queueIndex` in FIFO order until a frame is found that is not pinned
- if the cycle comes back to where it began, it means that all pages are pinned and `NULL` is returned
- otherwise, the index is used to evict the page in the frame (and write to disk if needed)

```c
BM_PageFrame *replacementLRU(BM_BufferPool *const bm)
```

- iterates over the page frames looking the the unpinned page with the smallest timestamp
- if all the time stamps are pinned, then `NULL` is returned