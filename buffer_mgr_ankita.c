#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "hash_table.h"
#include <stdlib.h>
#include <limits.h>

/* Additional Definitions */

#define PAGE_TABLE_SIZE 256

typedef unsigned int TimeStamps;

typedef struct BM_PageFrame {
    // the frame's buffer
    char* datas;
    // the page currently occupying it
    PageNumber pageNumbering;
    // management data on the page frame
    int indexOfFrame;
    int fixedCount;
    bool isDirty;
    bool filled;
    TimeStamps timeStamps;
} BM_PageFrame;

typedef struct BM_Metadata {
    // an array of frames
    BM_PageFrame *pageFrames;
    // a page table that associates the a page ID with an index in pageFrames
    HT_TableHandle pageTables;
    // the file handle
    SM_FileHandle pageFiles;
    // increments everytime a page is accessed (used for frame's timeStamp)
    TimeStamps timeStamps;
    // used to treat *pageFrames as a queue
    int indexOfAQueue;
    // statistics
    int readingNum;
    int writingNum;
} BM_Metadata;

/* Declarations */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bufferManage);

BM_PageFrame *replacementLRU(BM_BufferPool *const bufferManage);

// use this helper to increment the pool's global timestamp and return it
TimeStamps getTimeStamps(BM_Metadata *metadata);

// use this help to evict the frame at indexOfFrame (write if filled and isDirty) and return the new empty frame
BM_PageFrame *getAfterEviction(BM_BufferPool *const bufferManage, int indexOfFrame);

/* Buffer Manager Interface Pool Handling */

RC initBufferPool(BM_BufferPool *const bufferManage, const char *const pageFileName, 
		const int numberOfPages, ReplacementStrategy strategies,
		void *strategyData)
{
    // initialize the metadata
    BM_Metadata *metadata = (BM_Metadata *)malloc(sizeof(BM_Metadata));
    HT_TableHandle *pageTab = &(metadata->pageTables);
    metadata->timeStamps = 0;

    // start the queue from the last element as it gets incremented by one and modded 
    // at the start of each call of replacementFIFO
    metadata->indexOfAQueue = bufferManage->numberOfPages - 1;
    metadata->readingNum = 0;
    metadata->writingNum = 0;
    RC result = openPageFile((char *)pageFileName, &(metadata->pageFiles));
    if (result == RC_OK)
    {
        initHashTable(pageTab, PAGE_TABLE_SIZE);
        metadata->pageFrames = (BM_PageFrame *)malloc(sizeof(BM_PageFrame) * numberOfPages);
        for (int i = 0; i < numberOfPages; i++)
        {
            metadata->pageFrames[i].indexOfFrame = i;
            metadata->pageFrames[i].datas = (char *)malloc(PAGE_SIZE);
            metadata->pageFrames[i].fixedCount = 0;
            metadata->pageFrames[i].isDirty = false;
            metadata->pageFrames[i].filled = false;
            metadata->pageFrames[i].timeStamps = getTimeStamps(metadata);
        }
        bufferManage->mgmtData = (void *)metadata;
        bufferManage->numberOfPages = numberOfPages;
        bufferManage->pageFiles = (char *)&(metadata->pageFiles);
        bufferManage->strategies = strategies;
        return RC_OK;
    }
    else
    {
        // in case the file can't be open, set the metadata to NULL
        bufferManage->mgmtData = NULL;
        return result;
    }
}

RC shutdownBufferPool(BM_BufferPool *const bufferManage)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTab = &(metadata->pageTables);
        
        // "It is an error to shutdown a buffer pool that has pinned pages."
        for (int i = 0; i < bufferManage->numberOfPages; i++)
        {
            if (pageFrames[i].fixedCount > 0) return RC_WRITE_FAILED;
        }
        forceFlushPool(bufferManage);
        for (int i = 0; i < bufferManage->numberOfPages; i++)
        {
            // free each page frame's data
            free(pageFrames[i].datas);
        }
        closePageFile(&(metadata->pageFiles));

        // free the pageFrames array and metadata
        freeHashTable(pageTab);
        free(pageFrames);
        free(metadata);
        return RC_OK;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC forceFlushPool(BM_BufferPool *const bufferManage)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        for (int i = 0; i < bufferManage->numberOfPages; i++)
        {
            // write the filled, isDirty, and unpinned pages to disk
            if (pageFrames[i].filled && pageFrames[i].isDirty && pageFrames[i].fixedCount == 0)
            {
                writeBlock(pageFrames[i].pageNumbering, &(metadata->pageFiles), pageFrames[i].datas);
                metadata->writingNum++;
                pageFrames[i].timeStamps = getTimeStamps(metadata);

                // clear the isDirty bool
                pageFrames[i].isDirty = false;
            }
        }
        return RC_OK;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

/* Buffer Manager Interface Access Pages */

RC markDirty (BM_BufferPool *const bufferManage, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTab = &(metadata->pageTables);
        int indexOfFrame;

        // get the mapped indexOfFrame from pageNumbering
        if (getValue(pageTab, page->pageNumbering, &indexOfFrame) == 0)
        {
            pageFrames[indexOfFrame].timeStamps = getTimeStamps(metadata);

            // set isDirty bool
            pageFrames[indexOfFrame].isDirty = true;
            return RC_OK;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC unpinPage (BM_BufferPool *const bufferManage, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTab = &(metadata->pageTables);
        int indexOfFrame;

        // get the mapped indexOfFrame from pageNumbering
        if (getValue(pageTab, page->pageNumbering, &indexOfFrame) == 0)
        {
            pageFrames[indexOfFrame].timeStamps = getTimeStamps(metadata);

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

RC forcePage (BM_BufferPool *const bufferManage, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTab = &(metadata->pageTables);
        int indexOfFrame;

        // get the mapped indexOfFrame from pageNumbering
        if (getValue(pageTab, page->pageNumbering, &indexOfFrame) == 0)
        {
            pageFrames[indexOfFrame].timeStamps = getTimeStamps(metadata);

            // only force the page if it is not pinned
            if (pageFrames[indexOfFrame].fixedCount == 0)
            {
                writeBlock(page->pageNumbering, &(metadata->pageFiles), pageFrames[indexOfFrame].datas);
                metadata->writingNum++;

                // clear isDirty bool
                pageFrames[indexOfFrame].isDirty = false;
                return RC_OK;
            }
            else return RC_WRITE_FAILED;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC pinPage (BM_BufferPool *const bufferManage, BM_PageHandle *const page, const PageNumber pageNumbering)
{
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTab = &(metadata->pageTables);
        int indexOfFrame;

        // make sure the pageNumbering is not negative
        if (pageNumbering >= 0) 
        {
            // check if page is already in a frame and get the mapped indexOfFrame from pageNumbering
            if (getValue(pageTab, pageNumbering, &indexOfFrame) == 0)
            {
                pageFrames[indexOfFrame].timeStamps = getTimeStamps(metadata);
                pageFrames[indexOfFrame].fixedCount++;
                page->datas = pageFrames[indexOfFrame].datas;
                page->pageNumbering = pageNumbering;
                return RC_OK;
            }
            else 
            {
                // use specified replacement strategies
                BM_PageFrame *pageFrame;
                if (bufferManage->strategies == RS_FIFO)
                    pageFrame = replacementFIFO(bufferManage);
                else // if (bufferManage->strategies == RS_LRU)
                    pageFrame = replacementLRU(bufferManage);

                // if the strategies failed (i.e. all frames are pinned) return error
                if (pageFrame == NULL)
                    return RC_WRITE_FAILED;
                else 
                {
                    // set the mapping from pageNumbering to indexOfFrame
                    setValue(pageTab, pageNumbering, pageFrame->indexOfFrame);

                    // grow the file if needed
                    ensureCapacity(pageNumbering + 1, &(metadata->pageFiles));

                    // read data from disk
                    readBlock(pageNumbering, &(metadata->pageFiles), pageFrame->datas);
                    metadata->readingNum++;

                    // set frame's metadata
                    pageFrame->isDirty = false;
                    pageFrame->fixedCount = 1;
                    pageFrame->filled = true;
                    pageFrame->pageNumbering = pageNumbering;
                    page->datas = pageFrame->datas;
                    page->pageNumbering = pageNumbering;
                    return RC_OK;
                }
            }
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

/* Statistics Interface */

PageNumber *getFrameContents (BM_BufferPool *const bufferManage)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        PageNumber *array = (PageNumber *)malloc(sizeof(PageNumber) * bufferManage->numberOfPages);
        for (int i = 0; i < bufferManage->numberOfPages; i++)
        {
            if (pageFrames[i].filled)
                array[i] = pageFrames[i].pageNumbering;
            else array[i] = NO_PAGE;
        }
        return array;
    }
    else return NULL;
}

bool *getDirtyFlags (BM_BufferPool *const bufferManage)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        bool *array = (bool *)malloc(sizeof(bool) * bufferManage->numberOfPages);
        for (int i = 0; i < bufferManage->numberOfPages; i++)
        {
            if (pageFrames[i].filled)
                array[i] = pageFrames[i].isDirty;
            else array[i] = false;
        }
        return array;
    }
    else return NULL;
}

int *getFixCounts (BM_BufferPool *const bufferManage)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        int *array = (int *)malloc(sizeof(int) * bufferManage->numberOfPages);
        for (int i = 0; i < bufferManage->numberOfPages; i++)
        {
            if (pageFrames[i].filled)
                array[i] = pageFrames[i].fixedCount;
            else array[i] = 0;
        }
        return array;
    }
    else return NULL;
}

int getNumReadIO (BM_BufferPool *const bufferManage)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        return metadata->readingNum;
    }
    else return 0;
}

int getNumWriteIO (BM_BufferPool *const bufferManage)
{
    // make sure the metadata was successfully initialized
    if (bufferManage->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
        return metadata->writingNum;
    }
    else return 0;
}

/* Replacement Policies */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bufferManage)
{
    BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    int firstIndex = metadata->indexOfAQueue;
    int currentIndex = metadata->indexOfAQueue;

    // keep cycling in FIFO order until a frame is found that is not pinned
    do 
    {
        currentIndex = (currentIndex + 1) % bufferManage->numberOfPages;
        if (pageFrames[currentIndex].fixedCount == 0)
            break;
    }
    while (currentIndex != firstIndex);

    // put the index back into the metadata pointer
    metadata->indexOfAQueue = currentIndex;

    // ensure we did not cycle into a pinned frame (i.e. all frames are pinned) or return NULL
    if (pageFrames[currentIndex].fixedCount == 0)
        return getAfterEviction(bufferManage, currentIndex);
    else return NULL;
}

BM_PageFrame *replacementLRU(BM_BufferPool *const bufferManage)
{
    BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    TimeStamps min = UINT_MAX;
    int minimumIndex = -1;

    // find unpinned frame with smallest timestamp
    for (int i = 0; i < bufferManage->numberOfPages; i++)
    {
        if (pageFrames[i].fixedCount == 0 && pageFrames[i].timeStamps < min) 
        {
            min = pageFrames[i].timeStamps;
            minimumIndex = i;
        }
    }
    
    // if all frames were pinned, return NULL
    if (minimumIndex == -1) 
        return NULL;
    else return getAfterEviction(bufferManage, minimumIndex);
}

/* Helpers */

TimeStamps getTimeStamps(BM_Metadata *metadata)
{
    // increment the global timestamp after returning it to be assigned to a frame
    return metadata->timeStamps++;
}

BM_PageFrame *getAfterEviction(BM_BufferPool *const bufferManage, int indexOfFrame)
{
    BM_Metadata *metadata = (BM_Metadata *)bufferManage->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTab = &(metadata->pageTables);

    // update timestamp
    pageFrames[indexOfFrame].timeStamps = getTimeStamps(metadata);
    if (pageFrames[indexOfFrame].filled)
    {
        // remove old mapping
        removePair(pageTab, pageFrames[indexOfFrame].pageNumbering);

        // write old frame back to disk if isDirty
        if (pageFrames[indexOfFrame].isDirty) 
        {
            writeBlock(pageFrames[indexOfFrame].pageNumbering, &(metadata->pageFiles), pageFrames[indexOfFrame].datas);
            metadata->writingNum++;
        }
    }

    // return evicted frame (called must deal with setting the page's metadata)
    return &(pageFrames[indexOfFrame]);
}