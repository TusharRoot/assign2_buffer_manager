
# CS_525 Assignment 2 - Buffer Manager


The buffer manager manages a fixed number of
pages in memory that represent pages from a page file managed by the storage manager implemented in assignment 1. The memory pages managed by the buffer manager are called page frames or frames for short.

Aim - Creating a basic buffer manager pool. It has caches file counts into memory from disk. In this buffer manager will look for a certain page asked from user into its cache and if its not found in cache it will load from disk. Cache will be of pre defined size and buffer pool will decide which page to replace. 
## Running Tests

To build the buffer manager as well the first set of test cases in test_assign2_1.c, use

```bash
make
./test_assign2_1.o
```
To build the second set of test cases in test_assign2_2.c, use run the following command -
```bash
make test_assign2_2
./test_assign2_2.o
```
There is also another set of tests for a hash table coded for this assignment in hash_table.c. To build this set of test cases in test_hash_table.c, use run the following command - 

```bash
make test_hash_table
./test_hash_table.o
```
To clean the solution use
```bash
make clean
```

## Solution Description
1] Data Structure
```bash
BM_BufferPool
```
This stores information about buffer pool: pageFileName
, page frames count, page replacement stategy, pointer to bookkeeping data

```bash
BM PageHandle
```
position of page in file is stored in -pageNum-. Page number of first data page is 0. 

2] Buffer Pool Functions
```bash
initBufferPool
```
This will create a new buffer pool with -numPages- page frames using the page replacement
strategy. The pool is used to cache pages from the page file with name -pageFileName-.
This methode will not generate new page file. The page file should initially exist and all page frames empty.


```bash
shutdownBufferPool
```
This frees all resources associated with buffer pool. If there are dirty pages it will be written to disk before destroying the buffer pool. It is an error to shutdown a buffer pool that has pinned pages.


```bash
forceFlushPool 
```
all dirty pages from the buffer pool to be written to disk.

3] Page Management Functions
```bash
pinPage
```
The buffer manager is responsible to set the
-pageNum- field of the page handle passed to the method. The data field should point to the area in memory storing the content of the page.

```bash
unpinPage
```
The -pageNum- field of page should be used to figure out which -page- to unpin.
```bash
markDirty
```
This will mark page as dirty.
```bash
forcePage 
```
This will write the current content of page back to page file on disk.

4] Statistics Functions
```bash
getFrameContents
```
function returns an array of PageNumbers. For an empty page frame NO PAGE is used.
```bash
getDirtyFlags
```
This returns array of bools as TRUE where page stored at that index is dirty. Empty pages are clean.
```bash
getFixCounts
```
 It will give array of ints of fix count of the page stored in current index page frame. It will give 0 for empty page frames.
```bash
getNumReadIO
```
It will give number of pages read from disk since initialization of buffer pool.
```bash
getNumWriteIO
```
returns the number of pages written to the page file since the buffer pool has been
initialized.


## Authors



Akshar Patel - A20563554

Ankita Chirame - A20543083

Tushar Patel - A20561012