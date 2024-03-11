#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"

#include <stdio.h>
#include <stdlib.h>

// local functions
static void printStrat (BM_BufferPool *const bufferManage);

// external functions
void 
printPoolContent (BM_BufferPool *const bufferManage)
{
	PageNumber *frameContent;
	bool *isDirty;
	int *fixedCount;
	int i;

	frameContent = getFrameContents(bufferManage);
	isDirty = getDirtyFlags(bufferManage);
	fixedCount = getFixCounts(bufferManage);

	printf("{");
	printStrat(bufferManage);
	printf(" %i}: ", bufferManage->numberOfPages);

	for (i = 0; i < bufferManage->numberOfPages; i++)
		printf("%s[%i%s%i]", ((i == 0) ? "" : ",") , frameContent[i], (isDirty[i] ? "x": " "), fixedCount[i]);
	printf("\n");
}

char *
sprintPoolContent (BM_BufferPool *const bufferManage)
{
	PageNumber *frameContent;
	bool *isDirty;
	int *fixedCount;
	int i;
	char *message;
	int pos = 0;

	message = (char *) malloc(256 + (22 * bufferManage->numberOfPages));
	frameContent = getFrameContents(bufferManage);
	isDirty = getDirtyFlags(bufferManage);
	fixedCount = getFixCounts(bufferManage);

	for (i = 0; i < bufferManage->numberOfPages; i++)
		pos += sprintf(message + pos, "%s[%i%s%i]", ((i == 0) ? "" : ",") , frameContent[i], (isDirty[i] ? "x": " "), fixedCount[i]);

	return message;
}


void
printPageContent (BM_PageHandle *const page)
{
	int i;

	printf("[Page %i]\n", page->pageNumbering);

	for (i = 1; i <= PAGE_SIZE; i++)
		printf("%02X%s%s", page->datas[i], (i % 8) ? "" : " ", (i % 64) ? "" : "\n");
}

char *
sprintPageContent (BM_PageHandle *const page)
{
	int i;
	char *message;
	int pos = 0;

	message = (char *) malloc(30 + (2 * PAGE_SIZE) + (PAGE_SIZE % 64) + (PAGE_SIZE % 8));
	pos += sprintf(message + pos, "[Page %i]\n", page->pageNumbering);

	for (i = 1; i <= PAGE_SIZE; i++)
		pos += sprintf(message + pos, "%02X%s%s", page->datas[i], (i % 8) ? "" : " ", (i % 64) ? "" : "\n");

	return message;
}

void
printStrat (BM_BufferPool *const bufferManage)
{
	switch (bufferManage->strategies)
	{
	case RS_FIFO:
		printf("FIFO");
		break;
	case RS_LRU:
		printf("LRU");
		break;
	case RS_CLOCK:
		printf("CLOCK");
		break;
	case RS_LFU:
		printf("LFU");
		break;
	case RS_LRU_K:
		printf("LRU-K");
		break;
	default:
		printf("%i", bufferManage->strategies);
		break;
	}
}
