#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
	RS_LRU = 1,
	RS_CLOCK = 2,
	RS_LFU = 3,
	RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
	char *pageFiles;
	int numberOfPages;
	ReplacementStrategy strategies;
	void *mgmtData; // use this one to store the bookkeeping info your buffer
	// manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
	PageNumber pageNumbering;
	char *datas;
} BM_PageHandle;

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bufferManage, const char *const pageFileName, 
		const int numberOfPages, ReplacementStrategy strategies,
		void *strategyData);
RC shutdownBufferPool(BM_BufferPool *const bufferManage);
RC forceFlushPool(BM_BufferPool *const bufferManage);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bufferManage, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bufferManage, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bufferManage, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bufferManage, BM_PageHandle *const page, 
		const PageNumber pageNumbering);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bufferManage);
bool *getDirtyFlags (BM_BufferPool *const bufferManage);
int *getFixCounts (BM_BufferPool *const bufferManage);
int getNumReadIO (BM_BufferPool *const bufferManage);
int getNumWriteIO (BM_BufferPool *const bufferManage);

#endif
