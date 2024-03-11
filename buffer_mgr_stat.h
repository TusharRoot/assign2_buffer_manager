#ifndef BUFFER_MGR_STAT_H
#define BUFFER_MGR_STAT_H

#include "buffer_mgr.h"

// debug functions
void printPoolContent (BM_BufferPool *const bufferManage);
void printPageContent (BM_PageHandle *const page);
char *sprintPoolContent (BM_BufferPool *const bufferManage);
char *sprintPageContent (BM_PageHandle *const page);

#endif
