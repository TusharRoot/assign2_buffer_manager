#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_ATTEMPTS 5
/* manipulating page files */

void initStorageManager(void) { }

RC createPageFile(char *fileName) {
    FILE *fp = fopen(fileName, "w+");
    if (fp == NULL) 
        return RC_FILE_NOT_FOUND;
    else {
        // Seek to the end of the file
        if (fseek(fp, PAGE_SIZE - 1, SEEK_SET) != 0) {
            fclose(fp);
            return RC_WRITE_FAILED;
        }
        
        // Write a single byte to extend the file size
        if (fputc('\0', fp) == EOF) {
            fclose(fp);
            return RC_WRITE_FAILED;
        }

        // Close the file
        fclose(fp);
        return RC_OK;
    }
}

long _getFileSize(FILE *file) {
    long size = 0;
    
    // Store the current position of the file pointer
    long currentPosition = ftell(file);
    
    // Move the file pointer to the end
    fseek(file, 0L, SEEK_END);
    
    // Get the position of the file pointer (which represents the file size)
    size = ftell(file);
    
    // Restore the original position of the file pointer
    fseek(file, currentPosition, SEEK_SET);
    
    return size;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *fp = NULL;
    int attempt = 0;
    
    do {
        fp = fopen(fileName, "r+");
        if (fp != NULL)
            break;
        attempt++;
    } while (attempt < MAX_ATTEMPTS);
    
    if (fp == NULL)
        return RC_FILE_NOT_FOUND;
    
    fseek(fp, 0L, SEEK_END); 
    int size = ftell(fp); 
    int totalNumPages = size / PAGE_SIZE;
    
    fHandle->fileName = fileName;
    fHandle->totalNumPages = totalNumPages;
    fHandle->curPagePos = 0;

    fHandle->mgmtInfo = (void *)fp;
    
    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fp == NULL)
        return RC_FILE_NOT_FOUND;

    int attempt = 0;
    while (attempt < MAX_ATTEMPTS) {
        if (fclose(fp) == 0) {
            // unset the file pointer
            fHandle->mgmtInfo = NULL;
            return RC_OK;
        }
        attempt++;
    }

    return RC_FILE_NOT_FOUND;
}

RC destroyPageFile(char *fileName) {
    int attempt = 0;
    while (attempt < MAX_ATTEMPTS) {
        if (remove(fileName) == 0)
            return RC_OK;
        attempt++;
    }
    return RC_FILE_NOT_FOUND;
}

/* reading blocks from disc */

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check if pageNum is in range
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) 
        return RC_READ_NON_EXISTING_PAGE;

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    
    int attempt = 0;
    int fseekResult;
    do {
        fseekResult = fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
        if (fseekResult == 0) {
            // Read the page into memPage
            size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, fp);
            
            // Make sure the page was entirely read
            if (bytesRead != PAGE_SIZE)
                return RC_READ_NON_EXISTING_PAGE;
            else
                return RC_OK;
        }
        attempt++;
    } while (attempt < MAX_ATTEMPTS);
    
    return RC_FILE_NOT_FOUND;
}


int getBlockPos(SM_FileHandle *fHandle) {
    // Perform error checking to ensure curPagePos is within bounds
    if (fHandle == NULL || fHandle->totalNumPages <= 0 || fHandle->curPagePos < 0 || fHandle->curPagePos >= fHandle->totalNumPages) {
        // If an invalid handle or position is provided, return -1 to indicate error
        return -1;
    }

    // Return the current page position
    return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check if the current page position is the first page, return error if so
    if (fHandle->curPagePos <= 0)
        return RC_READ_NON_EXISTING_PAGE;

    // Calculate the previous pageNum
    int pageNum = fHandle->curPagePos - 1;

    // Initialize result variable
    RC result;

    // Loop until a valid page is read or non-existing page is encountered
    while (pageNum >= 0) {
        // Call readBlock function to read the previous block
        result = readBlock(pageNum, fHandle, memPage);

        // If the block was successfully read, update curPagePos and break the loop
        if (result == RC_OK) {
            fHandle->curPagePos = pageNum;
            break;
        }

        // Decrement pageNum for the next iteration
        pageNum--;
    }

    return result;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Calculate the next pageNum
    int pageNum = fHandle->curPagePos + 1;

    // Loop through the remaining pages to attempt reading
    for (; pageNum < fHandle->totalNumPages; pageNum++) {
        // Attempt to read the next block
        RC result = readBlock(pageNum, fHandle, memPage);

        // If the block was successfully read, update curPagePos and return result
        if (result == RC_OK) {
            fHandle->curPagePos = pageNum;
            return result;
        }
    }

    // Return non-existing page if no valid page is read
    return RC_READ_NON_EXISTING_PAGE;
}

// RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
// {
//     return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
// }
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    int lastPageNum = fHandle->totalNumPages - 1;
    if (lastPageNum < 0) {
        return RC_READ_NON_EXISTING_PAGE; // No pages in the file
    }
    return readBlock(lastPageNum, fHandle, memPage);
}

/* writing blocks to a page file */

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check if pageNum is in range
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) 
        return RC_READ_NON_EXISTING_PAGE;
    
    FILE *fp = (FILE *)fHandle->mgmtInfo;

    int attempt = 0;
    while (attempt < MAX_ATTEMPTS) {
        // Check to see if the position was set successfully
        if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) == 0) {
            size_t bytesToWrite = PAGE_SIZE;
            size_t totalBytesWritten = 0;

            while (bytesToWrite > 0) {
                size_t bytesWritten = fwrite(memPage + totalBytesWritten, sizeof(char), bytesToWrite, fp);
                if (bytesWritten <= 0) {
                    // If no bytes were written, return failure
                    return RC_WRITE_FAILED;
                }

                totalBytesWritten += bytesWritten;
                bytesToWrite -= bytesWritten;
            }
            
            if (totalBytesWritten != PAGE_SIZE)
                return RC_WRITE_FAILED;
            else
                return RC_OK;
        }
        attempt++;
    }
    
    return RC_FILE_NOT_FOUND;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    // Check if the current page position is within range
    if (fHandle->curPagePos < 0 || fHandle->curPagePos >= fHandle->totalNumPages)
        return RC_READ_NON_EXISTING_PAGE;
    
    FILE *fp = (FILE *)fHandle->mgmtInfo;

    // Calculate the offset to the current page position
    long offset = (long)fHandle->curPagePos * PAGE_SIZE;

    // Check if the position was set successfully
    if (fseek(fp, offset, SEEK_SET) == 0) {
        size_t bytesToWrite = PAGE_SIZE;
        size_t totalBytesWritten = 0;

        // Write the entire page
        while (bytesToWrite > 0) {
            size_t bytesWritten = fwrite(memPage + totalBytesWritten, sizeof(char), bytesToWrite, fp);
            if (bytesWritten <= 0) {
                return RC_WRITE_FAILED;
            }

            totalBytesWritten += bytesWritten;
            bytesToWrite -= bytesWritten;
        }
        
        if (totalBytesWritten != PAGE_SIZE)
            return RC_WRITE_FAILED;
        else
            return RC_OK;
    } else {
        return RC_FILE_NOT_FOUND;
    }
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    FILE *fp = (FILE *)fHandle->mgmtInfo;

    // Attempt to set the file pointer to the end of the file
    int attempt = 0;
    int fseekResult;
    do {
        fseekResult = fseek(fp, 0, SEEK_END);
        if (fseekResult == 0) {
            break; // Successfully positioned at the end of the file
        }
        attempt++;
    } while (attempt < MAX_ATTEMPTS);
    
    // If failed to set the file pointer, return error
    if (fseekResult != 0) {
        return RC_FILE_NOT_FOUND;
    }

    // Allocate a page of memory and fill it with zeros
    void *emptyPage = malloc(PAGE_SIZE);
    if (emptyPage == NULL) {
        return RC_WRITE_FAILED; // Failed to allocate memory
    }
    memset(emptyPage, '\0', PAGE_SIZE);

    // Write the empty page to the file
    size_t bytesWritten = fwrite(emptyPage, sizeof(char), PAGE_SIZE, fp);
    free(emptyPage);

    // Check if the write operation was successful
    if (bytesWritten != PAGE_SIZE) {
        return RC_WRITE_FAILED; // Failed to write entire page
    }

    // Increment totalNumPages and return success
    fHandle->totalNumPages++;
    return RC_OK;
}


RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    int pagesToAdd = numberOfPages - fHandle->totalNumPages;
    for (int i = 0; i < pagesToAdd; i++) {
        RC result = appendEmptyBlock(fHandle);
        if (result != RC_OK) {
            return result;
        }
    }
    return RC_OK;
}
