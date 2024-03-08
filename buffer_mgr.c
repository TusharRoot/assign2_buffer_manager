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
    int frameIndex;
    int fixCount;
    bool dirty;
    bool occupied;
    TimeStamp timeStamp;
} BM_PageFrame;

typedef struct BM_Metadata {
    // an array of frames
    BM_PageFrame *pageFrames;
    // a page table that associates the a page ID with an index in pageFrames
    HT_TableHandle pageTable;
    // the file handle
    SM_FileHandle pageFile;
    // increments everytime a page is accessed (used for frame's timeStamp)
    TimeStamp timeStamp;
    // used to treat *pageFrames as a queue
    int queueIndex;
    // statistics
    int numRead;
    int numWrite;
} BM_Metadata;

/* Declarations */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm);

BM_PageFrame *replacementLRU(BM_BufferPool *const bm);

// use this helper to increment the pool's global timestamp and return it
TimeStamp getTimeStamp(BM_Metadata *metadata);

// use this help to evict the frame at frameIndex (write if occupied and dirty) and return the new empty frame
BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int frameIndex);

/* Buffer Manager Interface Pool Handling */

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                    const int numPages, ReplacementStrategy strategy,
                    void *stratData) {

    // Check if buffer pool is already initialized
    if (bm->mgmtData != NULL) {
        return false; // Indicate initialization failure (already initialized)
    }

    // Allocate memory for metadata
    BM_Metadata *metadata = (BM_Metadata *)malloc(sizeof(BM_Metadata));
    if (metadata == NULL) {
        goto cleanup; // Handle memory allocation failure gracefully
    }

    // Allocate memory for page frames
    metadata->pageFrames = (BM_PageFrame *)calloc(numPages, sizeof(BM_PageFrame));
    if (metadata->pageFrames == NULL) {
        goto cleanup; // Handle memory allocation failure
    }

    // Initialize other metadata fields
    metadata->timeStamp = 0;
    metadata->queueIndex = 0;
    metadata->numRead = 0;
    metadata->numWrite = 0;
    bm->mgmtData = (void *)metadata;
    bm->numPages = numPages;
    bm->pageFile = (char *)pageFileName;  // Assuming pageFileName represents the file path
    bm->strategy = strategy;

    // Initialize other structures or resources if needed (placeholder)

    return true; // Indicate successful initialization

cleanup:
    // Clean up resources in case of failure
    if (metadata != NULL) {
        // Replace with actual page file closing logic if openPageFile was implemented
        // closePageFile(metadata->pageFile);
        free(metadata);
    }
    return false;
}



RC shutdownBufferPool(BM_BufferPool *const bm)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTable = &(metadata->pageTable);
    
    // Check if there are any pinned pages
    for (int i = 0; i < bm->numPages; i++) {
        if (pageFrames[i].fixCount > 0) {
            return RC_WRITE_FAILED; // It is an error to shutdown a buffer pool that has pinned pages
        }
    }
    
    // Flush dirty pages to disk
    RC result = forceFlushPool(bm);
    if (result != RC_OK) {
        return result;
    }
    
    // Free each page frame's data
    for (int i = 0; i < bm->numPages; i++) {
        free(pageFrames[i].data);
    }
    
    // Close the page file
    result = closePageFile(&(metadata->pageFile));
    if (result != RC_OK) {
        return result;
    }
    
    // Free the page frames array and metadata
    freeHashTable(pageTable);
    free(pageFrames);
    free(metadata);
    
    // Reset buffer pool metadata
    bm->mgmtData = NULL;
    
    return RC_OK;
}



RC forceFlushPool(BM_BufferPool *const bm)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    
    int i = 0;
    do {
        if (pageFrames[i].occupied && pageFrames[i].dirty && pageFrames[i].fixCount == 0)
        {
            // Write the occupied, dirty, and unpinned pages to disk
            RC result = writeBlock(pageFrames[i].pageNum, &(metadata->pageFile), pageFrames[i].data);
            if (result != RC_OK) {
                return result;
            }
            metadata->numWrite++;
            pageFrames[i].timeStamp = getTimeStamp(metadata);

            // Clear the dirty flag
            pageFrames[i].dirty = false;
        }
        i++;
    } while (i < bm->numPages);
    
    return RC_OK;
}


/* Buffer Manager Interface Access Pages */

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Find the frame index corresponding to the page
    int frameIndex = -1;
    int i = 0;
    do {
        if (pageFrames[i].pageNum == page->pageNum) {
            frameIndex = i;
            break;  // Found the page, exit the loop
        }
        i++;
    } while (i < bm->numPages);

    // Check if the page is found in the buffer pool
    if (frameIndex != -1) {
        // Update the timestamp
        pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

        // Mark the page as dirty
        pageFrames[frameIndex].dirty = true;

        return RC_OK;
    } else {
        // Page not found in the buffer pool
        return RC_IM_KEY_NOT_FOUND;
    }
}


RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Find the frame index corresponding to the page
    int frameIndex = -1;
    int i = 0;
    while (i < bm->numPages && frameIndex == -1) {
        if (pageFrames[i].pageNum == page->pageNum) {
            frameIndex = i;
        }
        i++;
    }

    // Check if the page is found in the buffer pool
    if (frameIndex != -1) {
        // Update the timestamp
        pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

        // Decrement the fix count (not below 0)
        pageFrames[frameIndex].fixCount--;
        if (pageFrames[frameIndex].fixCount < 0) {
            pageFrames[frameIndex].fixCount = 0;
        }

        return RC_OK;
    } else {
        // Page not found in the buffer pool
        return RC_IM_KEY_NOT_FOUND;
    }
}


RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTable = &(metadata->pageTable);
    int frameIndex = -1;
    int i = 0;
    while (i < bm->numPages && frameIndex == -1) {
        if (pageFrames[i].pageNum == page->pageNum) {
            frameIndex = i;
        }
        i++;
    }
    if (frameIndex != -1) {
        // Update the timestamp
        pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

        // Check if the page is pinned
        if (pageFrames[frameIndex].fixCount == 0) {
            // Force the page to disk
            RC result = writeBlock(page->pageNum, &(metadata->pageFile), pageFrames[frameIndex].data);
            if (result != RC_OK) {
                return result;
            }
            metadata->numWrite++;

            // Clear the dirty flag
            pageFrames[frameIndex].dirty = false;

            return RC_OK;
        } else {
            // Page is pinned, cannot force
            return RC_WRITE_FAILED;
        }
    } else {
        // Page not found in the buffer pool
        return RC_IM_KEY_NOT_FOUND;
    }
}


// RC pinPage(DLLBPMDll * BM, SM_PageHandle * BM, const U32 pageNum)
// {
//     Initialize buffer pool only if it has not already been initialized.
//         if (bm == NULL){
//             return RC_FILE_HANDLE_NOT_INIT;
//         }

//         //By doing so, the line is transformed into the following: BM_Metadata* metadata = (BM_Metadata*)bm->mgmtData;
//         BM_PageFrame *pageFrames = metadata->pageFrames;
//         HT_TableHandle *pageTable = &(metadata->pageTable);

//         // Since we've already reviewed the decline of traditional retail through negatively affected consumers in the first paragraph, let's now discuss the opening of the second paragraph by highlighting the rise of online shopping trend.
//         if (pageNum >= 0) {
//         // Obtain the frame index from map of pages where page is already in a frame, and the frameIndex is the key for the page.
//         int frameIndex = -1;
//         int i = 0;
//             for (i = 0; i < bm->numPages && frameIndex == -1) {
//                 if (pageFrames[i].fPages == pageNum) {
//                     frameIndex = i;
//                 }
//             i++;
//             }

//         if (frameIndex == -1:){
//         // Page already loaded into buffer pool, change metadata and rear next page.
//         pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);
//         pageFrames[frameIndex].fixCount++;
//         page->data = pageFrames[frameIndex].data;
//         page->pageNum = pageNum;
//         return RC_OK;   
//         } else {
//         // The page is not in the buffer pool, so operatring strategy will be employed as per the case prescribed.
//         BM_PageFrame *pageFrame;
//         if (bm->strategy == RS_FIFO)
//         pageFrame = replacementFIFO(bm);
//         else bm->strategy = RS_LRU ;
//         pageFrame = replacementLRU(bm);

//         Incorrect choices are penalized, and the best strategy that works for all frames should be retained (i.e., all frames are pinned) else error is returned.
//             if (pageFrame == NULL)
//             return RC_WRITE_FAILED;
//             else {
//         // Set the map from the pageNum to the frameIndex
//         setValue(pageTable, pageNum, pageFrame->frameIndex);

//         // If the desired results are not attained, grow the file.
//         ensureCapacity(pageNum + 1, &(metadata->pageFile));
        
//         // Enumeration shall be data extraction.
//         RC result = readBlock(pageNum, &(metadata->pageFile), pageFrame->dData);
//         goto_error: if (result != RC_OK) {
//             return result;
//         }
//         metadata->numRead++;
//         pageFrame->dirty = false;
//         pageFrame->fixCount = 1;
//         pageFrame->occupied = true; 
//         pageFrame->pageNum = pageNum;
//         page->data = pageFrame->data;
//         page->pageNum = pageNum;
//             return RC_OK;
//                 }
//             }
//                 } else {
//         // pageNum is negative
//         return RC_IM_KEY_NOT_FOUND;
//         }
//     }
//      else {
//         // pageNum is negative
//     return RC_IM_KEY_NOT_FOUND;
// }
RC pinPage (BM_BufferPool *bm, SM_PageHandle *page, const int pageNum) {
    // Error handling for uninitialized buffer pool
    if (bm == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Access metadata for page frames and page table
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Validate page number
    if (pageNum >= 0) {
        // Check if page is already in a frame
        int frameIndex = -1;
        for (int i = 0; i < bm->numPages && frameIndex == -1; i++) {
            if (pageFrames[i].pageNum == pageNum) {
                frameIndex = i;
            }
        }

        // If found in a frame, update metadata and return
        if (frameIndex != -1) {
            pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);  // Assuming getTimeStamp is defined
            pageFrames[frameIndex].fixCount++;
            *(page->data) = pageFrames[frameIndex].data;
            page->pageNum = pageNum;
            page->data = pageFrames->data;
            
            return RC_OK;
        } else {
            // Page not in buffer pool, apply replacement strategy
            BM_PageHandle *page;
            BM_PageFrame *pageFrame;
            if (bm->strategy == RS_FIFO) {
                pageFrame = replacementFIFO(bm);  // Assuming replacementFIFO is defined
            } else {
                // Assuming other strategies are handled elsewhere
                return RC_WRITE_FAILED;  // Temporary placeholder for error handling
            }

            // Check for available frame
            if (pageFrame == NULL) {
                return RC_WRITE_FAILED;  // No available frames
            } else {
                // Update page table, ensure file capacity, read block
                setValue(pageTable, pageNum, pageFrame->frameIndex);  // Assuming setValue is defined
                ensureCapacity(pageNum + 1, &(metadata->pageFile));  // Assuming ensureCapacity is defined
                RC result = readBlock(pageNum, &(metadata->pageFile), pageFrame->data);  // Assuming readBlock is defined

                // Handle potential errors during read
                if (result != RC_OK) {
                    return result;
                }

                // Update metadata and return
                metadata->numRead++;
                pageFrame->dirty = false;
                pageFrame->fixCount = 1;
                pageFrame->occupied = true; 
                pageFrame->pageNum = pageNum;
                page->data = pageFrame->data;
                page->pageNum = pageNum;
                return RC_OK;
            }
        }
    } else {
        // Invalid page number
        return RC_IM_KEY_NOT_FOUND;  // Assuming RC_IM_KEY_NOT_FOUND is defined
    }
}

/* Statistics Interface */

PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return NULL;
    }
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    // Allocate memory for the array
    PageNumber *array = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);
    if (array == NULL) {
        return NULL; // Memory allocation failed
    }
    int i = 0;
    do {
        if (pageFrames[i].occupied) {
            array[i] = pageFrames[i].pageNum;
        } else {
            array[i] = NO_PAGE;
        }
        i++;
    } while (i < bm->numPages);
    return array;
}


bool *getDirtyFlags(BM_BufferPool *const bm)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return NULL;
    }
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    // Allocate memory for the array
    bool *array = (bool *)malloc(sizeof(bool) * bm->numPages);
    if (array == NULL) {
        return NULL; // Memory allocation failed
    }
    int i = 0;
    do {
        if (pageFrames[i].occupied) {
            array[i] = pageFrames[i].dirty;
        } else {
            array[i] = false;
        }
        i++;
    } while (i < bm->numPages);
    return array;
}


int *getFixCounts(BM_BufferPool *const bm)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return NULL;
    }

    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    // Allocate memory for the array
    int *array = (int *)malloc(sizeof(int) * bm->numPages);
    if (array == NULL) {
        return NULL; // Memory allocation failed
    }

    int i = 0;
    do {
        if (pageFrames[i].occupied) {
            array[i] = pageFrames[i].fixCount;
        } else {
            array[i] = 0;
        }
        i++;
    } while (i < bm->numPages);

    return array;
}


int getNumReadIO(BM_BufferPool *const bm)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return 0;
    }
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    return metadata->numRead;
}


int getNumWriteIO(BM_BufferPool *const bm)
{
    // Check if buffer pool is already initialized
    if (bm->mgmtData == NULL) {
        return 0;
    }
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    return metadata->numWrite;
}


/* Replacement Policies */

// BM_PageFrame *replacementFIFO(BM_BufferPool *const bm)
// {
//     BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
//     BM_PageFrame *pageFrames = metadata->pageFrames;

//     // Initialize variables for keeping track of the oldest frame
//     int oldestIndex = 0;
//     int oldestTimeStamp = pageFrames[0].timeStamp;

//     // Find the oldest unpinned frame
//     for (int i = 1; i < bm->numPages; i++) {
//         if (pageFrames[i].fixCount == 0 && pageFrames[i].timeStamp < oldestTimeStamp) {
//             oldestIndex = i;
//             oldestTimeStamp = pageFrames[i].timeStamp;
//         }
//     }

//     // If all frames are pinned, return NULL
//     if (pageFrames[oldestIndex].fixCount != 0) {
//         return NULL;
//     }

//     // Otherwise, evict the oldest frame and return it
//     return getAfterEviction(bm, oldestIndex);
// }


BM_PageFrame *replacementFIFO(BM_BufferPool *const bm) {
  BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
  BM_PageFrame *pageFrames = metadata->pageFrames;

  int currentIndex = metadata->queueIndex;
  int i = 0;

  do {
    currentIndex = (currentIndex + 1) % bm->numPages;
    i++;
  } while (i < bm->numPages && pageFrames[currentIndex].fixCount != 0);

  // Update metadata and handle eviction (assuming getAfterEviction exists)
  metadata->queueIndex = currentIndex;
  return (pageFrames[currentIndex].fixCount == 0) ? getAfterEviction(bm, currentIndex) : NULL;
}


/* Helpers */

TimeStamp getTimeStamp(BM_Metadata *metadata)
{
    // increment the global timestamp after returning it to be assigned to a frame
    return metadata->timeStamp++;
}

BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int frameIndex)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTable = &(metadata->pageTable);

    // Update timestamp
    pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

    // If the frame is occupied, remove old mapping and write old frame back to disk if dirty
    if (pageFrames[frameIndex].occupied) {
        int i = 0;
        do {
            if (pageFrames[i].pageNum == pageFrames[frameIndex].pageNum) {
                removePair(pageTable, pageFrames[i].pageNum);
                if (pageFrames[i].dirty) {
                    writeBlock(pageFrames[i].pageNum, &(metadata->pageFile), pageFrames[i].data);
                    metadata->numWrite++;
                }
                break;
            }
            i++;
        } while (i < bm->numPages);
    }

    // Return evicted frame (caller must deal with setting the page's metadata)
    return &(pageFrames[frameIndex]);
}
