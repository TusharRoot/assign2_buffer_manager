#include "storage_mgr.h"
#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "test_helper.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// var to store the current test's name
char *testName;

// check whether two the content of a buffer pool is the same as an expected content
// (given in the format produced by sprintPoolContent)
#define ASSERT_EQUALS_POOL(expected,bufferManage,message)                    \
do {                                    \
char *real;                                \
char *_exp = (char *) (expected);                                   \
real = sprintPoolContent(bufferManage);                    \
if (strcmp((_exp),real) != 0)                    \
{                                    \
printf("[%s-%s-L%i-%s] FAILED: expected <%s> but was <%s>: %s\n",TEST_INFO, _exp, real, message); \
free(real);                            \
exit(1);                            \
}                                    \
printf("[%s-%s-L%i-%s] OK: expected <%s> and was <%s>: %s\n",TEST_INFO, _exp, real, message); \
free(real);                                \
} while(0)

// test and helper methods
static void createDummyPages(BM_BufferPool *bufferManage, int num);

static void testLRU_K (void);

static void testError (void);

// main method
int
main (void)
{
    initStorageManager();
    testName = "";
    
    testLRU_K();
    testError();
    return 0;
}


void
createDummyPages(BM_BufferPool *bufferManage, int num)
{
    int i;
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    
    CHECK(initBufferPool(bufferManage, "testbuffer.bin", 3, RS_FIFO, NULL));
    
    for (i = 0; i < num; i++)
    {
        CHECK(pinPage(bufferManage, h, i));
        sprintf(h->datas, "%s-%i", "Page", h->pageNumbering);
        CHECK(markDirty(bufferManage, h));
        CHECK(unpinPage(bufferManage,h));
    }
    
    CHECK(shutdownBufferPool(bufferManage));
    
    free(h);
}

// test the LRU_K page replacement strategy
void
testLRU_K (void)
{
    // expected results
    const char *poolContents[] = {
        // read first five pages and directly unpin them
        "[0 0],[-1 0],[-1 0],[-1 0],[-1 0]" ,
        "[0 0],[1 0],[-1 0],[-1 0],[-1 0]",
        "[0 0],[1 0],[2 0],[-1 0],[-1 0]",
        "[0 0],[1 0],[2 0],[3 0],[-1 0]",
        "[0 0],[1 0],[2 0],[3 0],[4 0]",
        // use some of the page to create a fixed LRU_K order without changing pool content
        "[0 0],[1 0],[2 0],[3 0],[4 0]",
        "[0 0],[1 0],[2 0],[3 0],[4 0]",
        "[0 0],[1 0],[2 0],[3 0],[4 0]",
        "[0 0],[1 0],[2 0],[3 0],[4 0]",
        "[0 0],[1 0],[2 0],[3 0],[4 0]",
        // check that pages get evicted in LRU_K order
        "[0 0],[1 0],[2 0],[5 0],[4 0]",
        "[0 0],[1 0],[2 0],[5 0],[6 0]",
        "[7 0],[1 0],[2 0],[5 0],[6 0]",
        "[7 0],[1 0],[8 0],[5 0],[6 0]",
        "[7 0],[9 0],[8 0],[5 0],[6 0]"
    };
    const int orderRequests[] = {3,4,0,2,1};
    const int numLRU_KOrderChange = 5;
    
    int i;
    int snapshot = 0;
    BM_BufferPool *bufferManage = MAKE_POOL();
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    testName = "Testing LRU_K page replacement";
    
    CHECK(createPageFile("testbuffer.bin"));
    createDummyPages(bufferManage, 100);
    // int k = 1; CHECK(initBufferPool(bufferManage, "testbuffer.bin", 5, RS_LRU_K, &k));
    CHECK(initBufferPool(bufferManage, "testbuffer.bin", 5, RS_LRU_K, NULL));
    
    // reading first five pages linearly with direct unpin and no modifications
    for(i = 0; i < 5; i++)
    {
        pinPage(bufferManage, h, i);
        unpinPage(bufferManage, h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bufferManage, "check pool content reading in pages");
    }
    
    // read pages to change LRU_K order
    for(i = 0; i < numLRU_KOrderChange; i++)
    {
        pinPage(bufferManage, h, orderRequests[i]);
        unpinPage(bufferManage, h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bufferManage, "check pool content using pages");
    }
    
    // replace pages and check that it happens in LRU_K order
    for(i = 0; i < 5; i++)
    {
        pinPage(bufferManage, h, 5 + i);
        unpinPage(bufferManage, h);
        ASSERT_EQUALS_POOL(poolContents[snapshot++], bufferManage, "check pool content using pages");
    }
    
    // check number of write IOs
    ASSERT_EQUALS_INT(0, getNumWriteIO(bufferManage), "check number of write I/Os");
    ASSERT_EQUALS_INT(10, getNumReadIO(bufferManage), "check number of read I/Os");
    
    CHECK(shutdownBufferPool(bufferManage));
    CHECK(destroyPageFile("testbuffer.bin"));
    
    free(bufferManage);
    free(h);
    TEST_DONE();
}


// test error cases
void
testError (void)
{
    BM_BufferPool *bufferManage = MAKE_POOL();
    BM_PageHandle *h = MAKE_PAGE_HANDLE();
    BM_PageHandle *h2 = MAKE_PAGE_HANDLE();
    BM_PageHandle *h3 = MAKE_PAGE_HANDLE();
    testName = "ERROR TEST";
    
    CHECK(createPageFile("testbuffer.bin"));
    
    // pinpage until buffer pool is full and then request additional page.
    CHECK(initBufferPool(bufferManage, "testbuffer.bin", 3, RS_FIFO, NULL));
    CHECK(pinPage(bufferManage, h, 0));
    CHECK(pinPage(bufferManage, h2, 1));
    CHECK(pinPage(bufferManage, h3, 2));
    
    ASSERT_ERROR(pinPage(bufferManage, h, 3), "try to pin page when pool is full of pinned pages with fix-count > 0");

    // "It is an error to shutdown a buffer pool that has pinned pages."
    // the pages must be unpinned before shutting down
    CHECK(unpinPage(bufferManage, h));
    CHECK(unpinPage(bufferManage, h2));
    CHECK(unpinPage(bufferManage, h3));
    
    CHECK(shutdownBufferPool(bufferManage));
    
    // try to pin page with negative page number.
    CHECK(initBufferPool(bufferManage, "testbuffer.bin", 3, RS_FIFO, NULL));
    ASSERT_ERROR(pinPage(bufferManage, h, -10), "try to pin page with negative page number");
    CHECK(shutdownBufferPool(bufferManage));
    
    
    // try to use uninitialized buffer pool
    ASSERT_ERROR(initBufferPool(bufferManage, "unavailable.bin", 3, RS_FIFO, NULL), "try to init buffer pool for non existing page file");
    ASSERT_ERROR(shutdownBufferPool(bufferManage), "shutdown buffer pool that is not open");
    ASSERT_ERROR(forceFlushPool(bufferManage), "flush buffer pool that is not open");
    ASSERT_ERROR(pinPage(bufferManage, h, 1), "pin page in buffer pool that is not open");
    
    
    // try to unpin, mark, or force page that is not in pool
    CHECK(initBufferPool(bufferManage, "testbuffer.bin", 3, RS_FIFO, NULL));
    ASSERT_ERROR(unpinPage(bufferManage, h), "Try to unpin a page which is not available in framelist.");
    ASSERT_ERROR(forcePage(bufferManage, h), "Try to forceflush a page which is not available in framelist.");
    ASSERT_ERROR(markDirty(bufferManage, h), "Try to markdirty a page which is not available in framelist.");
    CHECK(shutdownBufferPool(bufferManage));
    
    // done remove page file
    CHECK(destroyPageFile("testbuffer.bin"));
    
    free(bufferManage);
    free(h);
    free(h2);
    free(h3);
    TEST_DONE();
}
