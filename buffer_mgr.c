#include<stdio.h>
#include<stdlib.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"




//Structure to represent the Page Frame
typedef struct Page
{

	SM_PageHandle data; 
	int fixCount; // To specify the number of clients using that page at a given instance
	int lastNo;   //  least recently used page -LRU
	int leastNo;   //least frequently used page-LFU// represents actual data of the page
	PageNumber pageNumber; // Identification for every Page
	int dirtyBit; // VAariable for dirtyBits or changes
	bool dirty; //store the state of the "dirty"
	int frameNumber;

}FramePage;

//Structure to represent the Buffer Pool
typedef struct BufferPool {
	
	int numRead;	//read number
	int numWrite;   //write number
	int numFrame;   //number of frame
	bool *dirtyFlag[maxBufferPool]; //dirtyflags of all the frames
} BufferPool;

/*VARIABLE DECLRATION*/

int lfuP = 0; //pointer for last frequently used page
int clockP = 0; //pointer for last added page
int end = 0; // variable to store the read pages count
int RC_ERROR = 15; // RC ERROR
RC rc;
int npp;
int frameCount = 0; //variable to count the addition of frames
int buffer = 0; // Variable for buffer Pool size
int write = 0; //variable to count the number of writes
int hit = 0;




/*REPLACEMENT STRATEGIES*/

//Implementing the FIFO Replacement Strategy
void FIFO(BM_BufferPool *const bm, FramePage *page){
	//Declaring the rearIndex 
	int startIndex = end % buffer;
	//Initiaising a page frame
	FramePage *pframe = bm -> mgmtData;
	int num;

	
	//Initialising a for loop as long as the variable is less than the buffer size
	for(num = 0 ; num < buffer ; num++){
		
		if(pframe[startIndex].fixCount != 0){
			// If the current page frame is being used by some client, we move on to the next location
			startIndex++;

			if(startIndex % buffer != 0){
				startIndex=startIndex;
			}
			else{
				startIndex=0;
			}
			
			
		}
		
		else
		
		{
			
			//if dirty bit is marked as 1
			if(pframe[startIndex].dirtyBit == 1){
				
				//int pNum = framePage[beginIndex].pageNumber;
				int pn=pframe[startIndex].pageNumber;
				int data_= pframe[startIndex].data;
				int bm_=bm -> pageFile;
				
				SM_FileHandle fileH;
				
				//Calling the openPageFile method
				openPageFile(bm -> pageFile, &fileH);
				//Calling the writeBlock method from storage_mgr.c
				writeBlock(pn, &fileH,pframe[startIndex].data);
				
				//Increase the number of writes
				write= write+1;
			}
			//Condition for when fixCount is not equal to )
			else if(pframe[startIndex].fixCount != 0){
				//Increment begin index
				startIndex=startIndex+1;
				
				if(startIndex % buffer != 0){
				
				startIndex = startIndex;
				
				}
				
				else{
				
				startIndex = 0;
				
				}
		}
		
		// Setting page frame's content to new page's content
			pframe[startIndex].data = page -> data;
			
			pframe[startIndex].dirtyBit = page -> dirtyBit;
			
			pframe[startIndex].fixCount = page -> fixCount;
			
			pframe[startIndex].pageNumber = page -> pageNumber;
		
			break;
		}
	}
}


extern void LRU(BM_BufferPool *const bm, FramePage *page)
{	
	FramePage *pFrame = bm->mgmtData;
	int i; 
	bool check = true;
	int LRU_pageIndex;
	int LRU_pageNum;
	int num;



	for(num=0; num < buffer; num++){
			
			if(pFrame[num].fixCount == 0){
			check = false;
			LRU_pageIndex = num;
			
			LRU_pageNum=pFrame[num].lastNo;
			
			
			
			if(!check){
				break;
			}
			
			}
			else if(pFrame[num].fixCount != 0){
				continue;
			
		}
		
	}
	
	int j;
	for(j = LRU_pageIndex + 1 ; j < buffer ; j++){
		if(pFrame[j].lastNo > LRU_pageNum){
			continue;
			
		}
		else{
			LRU_pageIndex = j;
			LRU_pageNum = pFrame[j].lastNo;
		}
	}

	
	// Setting page frame's content to new page's content
	pFrame[LRU_pageIndex].data = page->data;
	pFrame[LRU_pageIndex].pageNumber = page->pageNumber;
	pFrame[LRU_pageIndex].dirtyBit = page->dirtyBit;
	pFrame[LRU_pageIndex].fixCount = page->fixCount;
	pFrame[LRU_pageIndex].lastNo = page->lastNo;
}


/* LRU - Least Recently Used algorithm is a replacement strategy. 
The LRU algorithm replaces a page that has not been used for a time longer than the other pages in the buffer pool
*/
/*extern void LRU(BM_BufferPool *const bm, FramePage *frame)
{
	int i;
	int LRU_pageIndex;
	int LRU_pageNum;
	bool check = true;
	FramePage *pframe = (FramePage *) bm->mgmtData;
	
	//Finding the pageframe that is recently used, i.e. pageframe with the least hit_number
	for (i = LRU_pageIndex+1; i < buffer; i++)
	{
		if (pframe[i].lastNo < LRU_pageNum)
		{
			LRU_pageIndex = i;
			LRU_pageNum = pframe[i].lastNo;
			
		}
	}

	//Finding a pageframe that is not in use, i.e. a pageframe with fix count = 0
	for (i = 0; i < buffer; i++)
	{
		if (pframe[i].fixCount == 0)
		{
			LRU_pageIndex = i;
			LRU_pageNum = pframe[i].fixCount;
			break;
		}
	}

	/*Finding a pageframe with the dirty bit count as 1, if a page has dirty bit = 1, i.e. it has been modified
	Writing the modified page to disk*
	if (pframe[i].dirtyBit == 1)
	{
		SM_FileHandle file_handle;
		char file_name = bm->pageFile;
		//Calling the openPageFile, writeBlock, closePageFile functions from storage_mgr.c
		openPageFile(file_name, &file_handle);
		writeBlock(pframe[LRU_pageIndex].pageNumber, &file_handle, pframe[LRU_pageIndex].data);
		closePageFile(&file_handle);
	}

	

	//Setting the contents of the new page
	pframe[LRU_pageIndex].data = frame->data;
	pframe[LRU_pageIndex].fixCount = frame->fixCount;
	pframe[LRU_pageIndex].dirtyBit = frame->dirtyBit;
	pframe[LRU_pageIndex].lastNo = frame->lastNo;
	pframe[LRU_pageIndex].pageNumber = frame->pageNumber;

	

}*/

//Implementing the CLOCK Replacement Strategy
void CLOCK(BM_BufferPool *const bm, FramePage *page){	
	
	FramePage *pframe = bm->mgmtData;

	if(pframe[clockP].lastNo == 0){
		SM_FileHandle fileH;
		int pn = pframe[clockP].pageNumber;
		SM_PageHandle mP;
		// If Dirty Bit is equal to one, we can write
		if(pframe[clockP].dirtyBit == 1){
		
			openPageFile(bm->pageFile, &fileH);
			//calling writeBlock for storage Manager
			writeBlock(pn, &fileH, mP);
			// Increase the number of writes
			write=write+1;
		
	}
	else{
	
		pframe[clockP++].lastNo = 1;
		}
		// New Page frame content 
		pframe[clockP].pageNumber = page -> pageNumber;
		pframe[clockP].dirtyBit =  page -> dirtyBit;
		pframe[clockP].fixCount = page -> fixCount;	
		pframe[clockP].data = page -> data;
		pframe[clockP].lastNo = page -> lastNo;
		
		clockP=clockP+1;
		
	}
	//while true
	while(1){
		
		if(clockP % buffer != 0){
			clockP = clockP;}
			
		else{
			clockP = 0;
		}
			
		}
	}




	
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
	const int numPages, ReplacementStrategy strategy,
	void *stratData){
	// 
	bm->pageFile = (char*)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;

	/* numPages refers to the total number of pages in the memory or the buffer pool,
	Assigning the size of the buffer to be equal to numPages */
	buffer = bm->numPages;

	// Dynamic memory allocation
	FramePage *pframe = malloc(sizeof(FramePage) * numPages);
	//
	int i;
	for (i = 0; i < buffer; i++)
	{
		pframe[i].data = NULL;
		pframe[i].pageNumber = -1;
		pframe[i].dirtyBit = 0;
		pframe[i].frameNumber = i;
		pframe[i].fixCount = 0;
	}
//Stores the pframe value in the buffer pool variable 
	bm -> mgmtData = pframe;
	write = lfuP = clockP = 0;
	return RC_OK;

}


RC shutdownBufferPool(BM_BufferPool *const bm)
{
	//
	FramePage *pframe = bm->mgmtData;
	forceFlushPool(bm);

	int i;
	for (i = 0; i < buffer ; i++)
	{
		if (pframe[i].fixCount != 0)
		{
			return RC_PIN_IN_BUFFER_POOL;
		}

		else
		{
			return RC_OK;
		}
	}

	//Releasing space
	free(pframe);
    //Reintializing
	bm->mgmtData = NULL;
	return RC_OK;

}

/*forceFlushPool causes all the pages with a count_fix(Fix count) of 0 to be written to disk
*/
RC forceFlushPool(BM_BufferPool *const bm)
{

	FramePage* pframe = bm->mgmtData;
	
	int i;
	for (i = 0; i < buffer; i++){
		
		SM_FileHandle fileH;
	
		/*If the fix count(count_fix) is equal to 0 and the dirty bit is 1, then the page is written to disk
		The OpenPageFile, writeBlock and closePageFile is called from SM_FileHandle*/
		if(pframe[i].dirtyBit == 1 && pframe[i].fixCount == 0){
			char *fileName = bm -> pageFile;
		
		//The OpenPageFile function opens the file available on disk
			openPageFile(fileName, &fileH);
			
			//The writeBlock function writes the data of the page available, to the disk
			writeBlock(pframe[i].pageNumber, &fileH, pframe[i].data);

			//Reinitializing dirty bit to 0
			pframe[i].dirtyBit = 0;

			//The closePageFile closes the file
			closePageFile(&fileH);
			write=write+1;
		}
		
	}
	return RC_OK;
}


RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	FramePage *pframe = (FramePage *)bm->mgmtData;

	
	SM_FileHandle fileH;
	bool checkBufferCapacity = true;
	int num  = 0;
	float LRU_ = 1;
	float CLOCK_= 2;
	float LFU_= 3;

	// If Buffer Pool is not empty
	if(pframe[0].pageNumber != -1){
		
		//while num is less than the buffer pool size
		while(num < buffer){
			if(pframe[num].pageNumber != -1){	
			
				if(pframe[num].pageNumber == pageNum)
				{
				
				if(bm->strategy == LRU_){
		
				pframe[num].lastNo = frameCount;
				
				}	
				else if(pframe[num].pageNumber != pageNum){
					
					checkBufferCapacity = true;
				}	
				
				else if(bm->strategy ==CLOCK_){
				
				pframe[num].lastNo = 1;
				
				}
				/*pframe[num+1].fixCount++;*/
				/*pframe[num+2].fixCount++;*/
					
					
					
					pframe[num].fixCount++;
					checkBufferCapacity = false;
					frameCount =frameCount+1; 
				
					page->pageNum = pageNum;
					page->data = pframe[num].data;
					clockP= clockP+1;
					break;
				}
									
			}
			else {
				
				
				openPageFile(bm->pageFile, &fileH);
				
				
				pframe[num].data = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
				readBlock(pageNum, &fileH, pframe[num].data);
				
				pframe[num].leastNo = 0;
				pframe[num].pageNumber = pageNum;
				pframe[num].fixCount = 1;
				
				end=end+1;	
				frameCount=frameCount+1; 
				
				if(bm->strategy == LRU_){
			
				pframe[num].lastNo = frameCount;
				
				}		
				else if(bm->strategy == CLOCK_){
			
				pframe[num].lastNo = 1;

				}
				page -> pageNum = pageNum;
				page -> data = pframe[num].data;
				checkBufferCapacity = false;
				
				break;
			}
		num=num+1;
		}
		
	// if buffer Pool is full
		if(checkBufferCapacity)
		{
			//creating a new frame Page called new 
			FramePage *new = (FramePage *) calloc(sizeof(FramePage), sizeof(char));		
			
			
			SM_FileHandle fileH;
			SM_PageHandle memPage = new -> data;
		
			openPageFile(bm -> pageFile, &fileH);
			new -> data = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
		
			readBlock(pageNum, &fileH, new->data);
			
			new -> dirtyBit = 0;		
			new -> fixCount = 1;
			new -> leastNo = 0;
			new -> pageNumber = pageNum;
			end=end+1;
			frameCount=frameCount+1;
			
			if(bm->strategy == LRU_){
				
				new->lastNo = frameCount;	}			
			else if(bm->strategy == CLOCK_){
			
				new->lastNo = 1;}

			page->pageNum = pageNum;
			page->data = new->data;	
				

		//Selects the required replacement strategy
			switch(bm -> strategy){			
				case 0: // Using FIFO algorithm
					FIFO(bm, new);
					break;
				case 1: // Using LRU algorithm
					LRU(bm, new);
					break;
				case 2: // Using CLOCK algorithm
					CLOCK(bm, new);
					break;
			
				default:
					printf("\nCheck Algorithm\n");

					
			}				
		}		
		return RC_OK;		
	}
	else
	{	
		SM_FileHandle fileH;
		
		openPageFile(bm->pageFile, &fileH);
		
		pframe[0].data = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
		
		ensureCapacity(pageNum, &fileH);
		
		readBlock(pageNum, &fileH, pframe[0].data);
		
		pframe[0].pageNumber = pageNum;
		pframe[0].fixCount++;
		pframe[0].leastNo = 0;
		page->pageNum = pageNum;
		page->data = pframe[0].data;
		end = frameCount = 0;
		pframe[0].lastNo = frameCount;	
		
		
		return RC_OK;
	}	
}


extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	//Creating a new Page frame
	FramePage *pframe = bm->mgmtData;
	//initialising a variable 
	int num;
	
	
	//Implementing a for loop
	for(num = 0; num < buffer; num++)
	{
		
		if(pframe[num].pageNumber == page->pageNum)
		{
			/*fixCount_=fixCount-1;*/
			pframe[num].fixCount--;
			break;		
		}

	}
	
	return RC_OK;
}



RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	int num =0;

	FramePage *pframe = bm -> mgmtData;
	
	 if(num == (*bm).numPages)
    {
        return RC_READ_NON_EXISTING_PAGE;
    }
	
	do{
		int pageInfo = pframe[num].pageNumber;
  		
		if(pageInfo == page->pageNum){
		  	
			pframe[num].dirtyBit = 1;
			
			return RC_OK;	
			
		}
  		num=num+1;
	} while(num<(*bm).numPages);

	
	return RC_ERROR;

}
	

/***********************************

	@function brief   :forcePage
					  1. RC_OK, write back to the page file in disk
					  2. if pageNum == page->pageNum
					  3. write the page back to file
					  4. pin the current page dirty state is false

	

***********************************/
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) 
{
	
	FramePage *p = bm->mgmtData;
	SM_FileHandle fH;
	RC rc = RC_OK;

	if (rc == RC_OK) {
		// write back to the page file in disk
		int i;
		for (i = 0; i < bm->numPages; i++)
		{
			int pageCount = page->pageNum;
			int pNum = p[i].pageNumber;
			SM_PageHandle memPage = p[i].data;

			if (p[i].pageNumber == page->pageNum) {
				openPageFile(bm->pageFile, &fH);
				ensureCapacity(page->pageNum, &fH);
				writeBlock(p[i].pageNumber, &fH, memPage);
				closePageFile(&fH);
				printf("forcePage Success!!!\n");

				p[i].dirty = false;
			}
		}
		return RC_OK;
}
}


/***********************************

	@function brief   :getFrameContents
					  1.set a pointer to locate
					  2.go through the frame
					  3.return the value


***********************************/
PageNumber *getFrameContents(BM_BufferPool * const bm) {
	
	FramePage *pf = bm->mgmtData;//bookkeeping info your buffer
	PageNumber *frameContent= malloc(buffer * sizeof(PageNumber));
	//frameContent = (int *)malloc(sizeof(int)*bm->numPages);
	int i;
	for (i = 0; i < bm->numPages; i++)
	{
		if(pf[i].pageNumber != -1){
		frameContent[i] = pf[i].pageNumber;
	}
	else{
			//An empty page frame 'NO_PAGE'
			frameContent[i] = NO_PAGE; 
		}
}
return frameContent;
}


/***********************************

	@function brief   :getDirtyFlags
					  1. 
	@author  : Yiqi Wang
	@date    : 2022/10/09

***********************************/
bool *getDirtyFlags(BM_BufferPool *const bm)
{
	FramePage *pf = bm->mgmtData;
	//bool *dirtyFlag[maxBufferPool];

	bool *dirtyFlag = malloc(buffer* sizeof(bool));
	int i;
	for (i = 0; i < buffer; i++)  //if i< numPages, ithe PageList is not full, add the rest values of the BUffer Pool into the array
	{
		dirtyFlag[i] = dirtyFlag[i] = (pf[i].dirtyBit == 1) ? true : false;
	}
	return dirtyFlag;
}



/***********************************

	@function brief   :getFixCounts

***********************************/
int *getFixCounts(BM_BufferPool *const bm) {
	// goes through the list of frames and updates the value of fixedCounts
	FramePage *pf = bm->mgmtData;
	int *fixCount;
	
	fixCount = (int*)malloc(sizeof(int)* buffer);
	int i;
	for (i = 0; i < bm->numPages; i++)
	{
		if(pf[i].fixCount != -1){
		fixCount[i] = pf[i].fixCount;
	}
	else{
			fixCount[i] = 0;
		}
	}
	return fixCount;
}


/***********************************

	@function brief   :getNumReadIO
	

***********************************/
int getNumReadIO(BM_BufferPool *const bm) {
	return (end + 1);
}


/***********************************

	@function brief   :getNumWriteIO
	

***********************************/

int getNumWriteIO(BM_BufferPool *const bm) {
	
	//float getNumberOfWrites = write;
	
	return write;
}




