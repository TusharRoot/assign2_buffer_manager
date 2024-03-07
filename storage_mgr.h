#ifndef STORAGE_MGR_H
#define STORAGE_MGR_H

#include "dberror.h"

/************************************************************
 *                    handle data structures                *
 ************************************************************/
typedef struct SM_FileHandle {
	char *fileName;
	int totalNumPages;
	int curPagePos;
	void *mgmtInfo;
} SM_FileHandle;

typedef char* SM_PageHandle;

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */
extern void initStorageManager (void);
extern RC createPageFile (char *fileName);
extern RC openPageFile (char *fileName, SM_FileHandle *fileHandle);
extern RC closePageFile (SM_FileHandle *fileHandle);
extern RC destroyPageFile (char *fileName);

/* reading blocks from disc */
extern RC readBlock (int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage);
extern int getBlockPos (SM_FileHandle *fileHandle);
extern RC readFirstBlock (SM_FileHandle *fileHandle, SM_PageHandle memPage);
extern RC readPreviousBlock (SM_FileHandle *fileHandle, SM_PageHandle memPage);
extern RC readCurrentBlock (SM_FileHandle *fileHandle, SM_PageHandle memPage);
extern RC readNextBlock (SM_FileHandle *fileHandle, SM_PageHandle memPage);
extern RC readLastBlock (SM_FileHandle *fileHandle, SM_PageHandle memPage);

/* writing blocks to a page file */
extern RC writeBlock (int pageNum, SM_FileHandle *fileHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock (SM_FileHandle *fileHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock (SM_FileHandle *fileHandle);
extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fileHandle);

#endif
