Functions:

Replacement Strategies:

FIFO
It ascertains if the page is present in the buffer pool. If so, this frame is given back, increasing the number of pins. If not, it will look to see if there is an available vacant frame in the pool. 
If this frame is dirty, it will then read the contents of the page that has to be returned from the disk and write the contents of the filthy frame back to the disk.

LRU
LRU is a page replacement strategy.Least Recently Used Algorithm works on replacing the page that has not been used recently in the
buffer pool.If a page is old in the buffer pool (at the end of the buffer pool), the LRU algorithm writes the page
to the disk before replacing it.


CLOCK 
The existence of the page in the buffer pool is checked. If so, it returns this frame and raises the number of pins. and designates this node as a reference node by setting its reference bit to 1. If not, it will see if the pool contains an empty frame that the empty frame can use. If so, it will use that frame node and read data into that frame from the disk.

initBufferPool
The initBufferPool function creates a new buffer pool with predefined number of pages.Initially, all page frame contents are set to null.Involves dynamic memory allocation.The initBufferPool function does not generate any new pages, it works using the existing pages in
memory

shutdownBufferPool
The shutdownBufferPool function is responsible for destroying a buffer pool.This function frees up resources and memory before completely destroying a buffer pool.After freeing the memory, the shutdownBufferPool function involves reinitializing the buffer
manager variable

forceFlushPool
The forceFlushPool function writes the pages to disk that do not have a dirty bit count of 0.The function causes all pages with a fix count of 0 to be written to disk.Involves Opening the existing/ available file from disk followed by writing the contents of the page to the disk and then closing the page

markDirty
This method suggests that the provided page is unclean. It confirms the accuracy of the inputs.It searches the buffer pool iteratively for the frame containing this page. It marks a page as dirty.It produces an error if the page cannot be found.Returns RC_OK if no error is found.

pinPage
pageNum is used to pin the page. Utilizes LRU and FIFO logic.

unpinPage
The page's pin is deleted using this technique.It checks the input for accuracy.It finds the FrameNode corresponding to the supplied page.It ascertains if the fixed count is higher than 0. Reduce the fixed count if this is the case. Returns RC OK upon success.

Force page
This method will write the page's contents back to disk. RC_OK, write back to the page file in disk.if pageNum == page->pageNum.Write the page back to file. Pin the current page dirty state is false

getFrameContents
This method getFrameContents sets a pointer to locate, go through the frame and return the value

getDirtyFlags
The ith item of the array of bools returned by the function getDirtyFlags is TRUE if the page stored in the ith page frame is dirty.

getFixCounts
The fixed count of the page stored in the ith page frame is the ith element of the array of ints (of size numPages) returned by the getFixCounts method.

getNumReadIO
Returns the number of pages that have been read from the disk since a buffer pool has been initialized.The number of pages that have been read from the disk since a buffer pool was initially created is given by the getNumReadIO function.

getNumWriteIO
The number of pages that have been written to the page file since the buffer pool was established is given by the function getNumWriteIO.



How to Run:


  Step 1: Go to the directory in the Linux terminal
  Step 2: make clean
  Step 3: make
  Step 4: Run ./test_assign1
