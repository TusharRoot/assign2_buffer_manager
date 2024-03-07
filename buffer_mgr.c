#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "hash_table.h"
#include <stdlib.h>
#include <limits.h>

/* Additional Definitions */

#define PAGE_TABLE_SIZE 256

typedef unsigned int TimeStamp;

typedef struct BM_PageFrame {
    // the frame's buffer
    char* data;
    // the page currently occupying it
    PageNumber pageNum;
    // management data on the page frame
    int indexOfFrame;
    int fixedCount;
    bool dirty;
    bool filled;
    TimeStamp timeStamp;
} BM_PageFrame;

typedef struct BM_Metadata {
    // an array of frames
    BM_PageFrame *pageFrames;
    // a page table that associates the a page ID with an index in pageFrames
    HT_TableHandle pageTables;
    // the file handle
    SM_FileHandle pageFile;
    // increments everytime a page is accessed (used for frame's timeStamp)
    TimeStamp timeStamp;
    // used to treat *pageFrames as a queue
    int indexOfAQueue;
    // statistics
    int readNum;
    int writeNum;
} BM_Metadata;

/* Declarations */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm);

BM_PageFrame *replacementLRU(BM_BufferPool *const bm);

// use this helper to increment the pool's global timestamp and return it
TimeStamp getTimeStamp(BM_Metadata *metadata);

// use this help to evict the frame at indexOfFrame (write if filled and dirty) and return the new empty frame
BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int indexOfFrame);

/* Buffer Manager Interface Pool Handling */

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData)
{
    // initialize the metadata
    BM_Metadata *metadata = (BM_Metadata *)malloc(sizeof(BM_Metadata));
    HT_TableHandle *pageTabe = &(metadata->pageTables);
    metadata->timeStamp = 0;

    // start the queue from the last element as it gets incremented by one and modded 
    // at the start of each call of replacementFIFO
    metadata->indexOfAQueue = bm->numPages - 1;
    metadata->readNum = 0;
    metadata->writeNum = 0;
    RC result = openPageFile((char *)pageFileName, &(metadata->pageFile));
    if (result == RC_OK)
    {
        initHashTable(pageTabe, PAGE_TABLE_SIZE);
        metadata->pageFrames = (BM_PageFrame *)malloc(sizeof(BM_PageFrame) * numPages);
        for (int i = 0; i < numPages; i++)
        {
            metadata->pageFrames[i].indexOfFrame = i;
            metadata->pageFrames[i].data = (char *)malloc(PAGE_SIZE);
            metadata->pageFrames[i].fixedCount = 0;
            metadata->pageFrames[i].dirty = false;
            metadata->pageFrames[i].filled = false;
            metadata->pageFrames[i].timeStamp = getTimeStamp(metadata);
        }
        bm->mgmtData = (void *)metadata;
        bm->numPages = numPages;
        bm->pageFile = (char *)&(metadata->pageFile);
        bm->strategy = strategy;
        return RC_OK;
    }
    else
    {
        // in case the file can't be open, set the metadata to NULL
        bm->mgmtData = NULL;
        return result;
    }
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTables);
        
        // "It is an error to shutdown a buffer pool that has pinned pages."
        for (int i = 0; i < bm->numPages; i++)
        {
            if (pageFrames[i].fixedCount > 0) return RC_WRITE_FAILED;
        }
        forceFlushPool(bm);
        for (int i = 0; i < bm->numPages; i++)
        {
            // free each page frame's data
            free(pageFrames[i].data);
        }
        closePageFile(&(metadata->pageFile));

        // free the pageFrames array and metadata
        freeHashTable(pageTabe);
        free(pageFrames);
        free(metadata);
        return RC_OK;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        for (int i = 0; i < bm->numPages; i++)
        {
            // write the filled, dirty, and unpinned pages to disk
            if (pageFrames[i].filled && pageFrames[i].dirty && pageFrames[i].fixedCount == 0)
            {
                writeBlock(pageFrames[i].pageNum, &(metadata->pageFile), pageFrames[i].data);
                metadata->writeNum++;
                pageFrames[i].timeStamp = getTimeStamp(metadata);

                // clear the dirty bool
                pageFrames[i].dirty = false;
            }
        }
        return RC_OK;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

/* Buffer Manager Interface Access Pages */

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTables);
        int indexOfFrame;

        // get the mapped indexOfFrame from pageNum
        if (getValue(pageTabe, page->pageNum, &indexOfFrame) == 0)
        {
            pageFrames[indexOfFrame].timeStamp = getTimeStamp(metadata);

            // set dirty bool
            pageFrames[indexOfFrame].dirty = true;
            return RC_OK;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTables);
        int indexOfFrame;

        // get the mapped indexOfFrame from pageNum
        if (getValue(pageTabe, page->pageNum, &indexOfFrame) == 0)
        {
            pageFrames[indexOfFrame].timeStamp = getTimeStamp(metadata);

            // decrement (not below 0)
            pageFrames[indexOfFrame].fixedCount--;
            if (pageFrames[indexOfFrame].fixedCount < 0)
                pageFrames[indexOfFrame].fixedCount = 0;
            return RC_OK;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTables);
        int indexOfFrame;

        // get the mapped indexOfFrame from pageNum
        if (getValue(pageTabe, page->pageNum, &indexOfFrame) == 0)
        {
            pageFrames[indexOfFrame].timeStamp = getTimeStamp(metadata);

            // only force the page if it is not pinned
            if (pageFrames[indexOfFrame].fixedCount == 0)
            {
                writeBlock(page->pageNum, &(metadata->pageFile), pageFrames[indexOfFrame].data);
                metadata->writeNum++;

                // clear dirty bool
                pageFrames[indexOfFrame].dirty = false;
                return RC_OK;
            }
            else return RC_WRITE_FAILED;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTables);
        int indexOfFrame;

        // make sure the pageNum is not negative
        if (pageNum >= 0) 
        {
            // check if page is already in a frame and get the mapped indexOfFrame from pageNum
            if (getValue(pageTabe, pageNum, &indexOfFrame) == 0)
            {
                pageFrames[indexOfFrame].timeStamp = getTimeStamp(metadata);
                pageFrames[indexOfFrame].fixedCount++;
                page->data = pageFrames[indexOfFrame].data;
                page->pageNum = pageNum;
                return RC_OK;
            }
            else 
            {
                // use specified replacement strategy
                BM_PageFrame *pageFrame;
                if (bm->strategy == RS_FIFO)
                    pageFrame = replacementFIFO(bm);
                else // if (bm->strategy == RS_LRU)
                    pageFrame = replacementLRU(bm);

                // if the strategy failed (i.e. all frames are pinned) return error
                if (pageFrame == NULL)
                    return RC_WRITE_FAILED;
                else 
                {
                    // set the mapping from pageNum to indexOfFrame
                    setValue(pageTabe, pageNum, pageFrame->indexOfFrame);

                    // grow the file if needed
                    ensureCapacity(pageNum + 1, &(metadata->pageFile));

                    // read data from disk
                    readBlock(pageNum, &(metadata->pageFile), pageFrame->data);
                    metadata->readNum++;

                    // set frame's metadata
                    pageFrame->dirty = false;
                    pageFrame->fixedCount = 1;
                    pageFrame->filled = true;
                    pageFrame->pageNum = pageNum;
                    page->data = pageFrame->data;
                    page->pageNum = pageNum;
                    return RC_OK;
                }
            }
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

/* Statistics Interface */

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        PageNumber *array = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);
        for (int i = 0; i < bm->numPages; i++)
        {
            if (pageFrames[i].filled)
                array[i] = pageFrames[i].pageNum;
            else array[i] = NO_PAGE;
        }
        return array;
    }
    else return NULL;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        bool *array = (bool *)malloc(sizeof(bool) * bm->numPages);
        for (int i = 0; i < bm->numPages; i++)
        {
            if (pageFrames[i].filled)
                array[i] = pageFrames[i].dirty;
            else array[i] = false;
        }
        return array;
    }
    else return NULL;
}

int *getFixCounts (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        int *array = (int *)malloc(sizeof(int) * bm->numPages);
        for (int i = 0; i < bm->numPages; i++)
        {
            if (pageFrames[i].filled)
                array[i] = pageFrames[i].fixedCount;
            else array[i] = 0;
        }
        return array;
    }
    else return NULL;
}

int getNumReadIO (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        return metadata->readNum;
    }
    else return 0;
}

int getNumWriteIO (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        return metadata->writeNum;
    }
    else return 0;
}

/* Replacement Policies */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    int firstIndex = metadata->indexOfAQueue;
    int currentIndex = metadata->indexOfAQueue;

    // keep cycling in FIFO order until a frame is found that is not pinned
    do 
    {
        currentIndex = (currentIndex + 1) % bm->numPages;
        if (pageFrames[currentIndex].fixedCount == 0)
            break;
    }
    while (currentIndex != firstIndex);

    // put the index back into the metadata pointer
    metadata->indexOfAQueue = currentIndex;

    // ensure we did not cycle into a pinned frame (i.e. all frames are pinned) or return NULL
    if (pageFrames[currentIndex].fixedCount == 0)
        return getAfterEviction(bm, currentIndex);
    else return NULL;
}

BM_PageFrame *replacementLRU(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    TimeStamp min = UINT_MAX;
    int minimumIndex = -1;

    // find unpinned frame with smallest timestamp
    for (int i = 0; i < bm->numPages; i++)
    {
        if (pageFrames[i].fixedCount == 0 && pageFrames[i].timeStamp < min) 
        {
            min = pageFrames[i].timeStamp;
            minimumIndex = i;
        }
    }
    
    // if all frames were pinned, return NULL
    if (minimumIndex == -1) 
        return NULL;
    else return getAfterEviction(bm, minimumIndex);
}

/* Helpers */

TimeStamp getTimeStamp(BM_Metadata *metadata)
{
    // increment the global timestamp after returning it to be assigned to a frame
    return metadata->timeStamp++;
}

BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int indexOfFrame)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTabe = &(metadata->pageTables);

    // update timestamp
    pageFrames[indexOfFrame].timeStamp = getTimeStamp(metadata);
    if (pageFrames[indexOfFrame].filled)
    {
        // remove old mapping
        removePair(pageTabe, pageFrames[indexOfFrame].pageNum);

        // write old frame back to disk if dirty
        if (pageFrames[indexOfFrame].dirty) 
        {
            writeBlock(pageFrames[indexOfFrame].pageNum, &(metadata->pageFile), pageFrames[indexOfFrame].data);
            metadata->writeNum++;
        }
    }

    // return evicted frame (called must deal with setting the page's metadata)
    return &(pageFrames[indexOfFrame]);
}