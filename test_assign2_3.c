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

// test and helper methods
static void testInitShutDownBuffer (void);

// main method
int 
main (void) 
{
  initStorageManager();
  testName = "";

  testInitShutDownBuffer();
}

// create a buffer pool then closes it
void
testInitShutDownBuffer (void)
{
  BM_BufferPool bm;
  testName = "Initialize and shutdown buffer pool";
  char *pageFileName = "testbuffer.bin";

  CHECK(createPageFile(pageFileName));

  CHECK(initBufferPool(&bm, pageFileName, 16, RS_LRU, NULL));
  CHECK(forceFlushPool(&bm));
  CHECK(shutdownBufferPool(&bm));

  CHECK(destroyPageFile(pageFileName));

  TEST_DONE();
}