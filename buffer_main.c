#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "hash_table.h"
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>

/* Additional Definitions */
#define MAX_TABLE_SIZE 256

/* Definitions */
typedef uint32_t Timestamp;

other

typedef struct BM_Page {
    char* data;
    PageNumber page_num;
    int frame_idx;
    int pin_count;
    bool dirty_flag;
    bool is_used;
    Timestamp last_access;
} BM_Page;

typedef struct BM_Metadata {
    BM_Page *pages;
    HT_TableHandle page_table;
    SM_FileHandle file_handle;
    Timestamp global_time;
    int queue_idx;
    int read_ops;
    int write_ops;
} BM_Metadata;

/* Declarations */
BM_Page *fifo_replacement(BM_BufferPool *const bm);
BM_Page *lru_replacement(BM_BufferPool *const bm);
Timestamp increment_global_time(BM_Metadata *metadata);
BM_Page *evict_page(BM_BufferPool *const bm, int frame_index);

/* Buffer Manager Interface Pool Handling */
RC init_buffer_pool(BM_BufferPool *const bm, const char *const page_file_name, 
                    const int num_pages, ReplacementStrategy strategy,
                    void *strategy_data) {
    BM_Metadata *metadata = (BM_Metadata *)malloc(sizeof(BM_Metadata));
    if (metadata == NULL) return RC_BUFFER_POOL_INIT_ERROR; // Handle memory allocation failure
    HT_TableHandle *page_table = &(metadata->page_table);
    metadata->global_time = 0;
other

    metadata->queue_idx = num_pages - 1;
    metadata->read_ops = 0;
    metadata->write_ops = 0;

    RC result = openPageFile((char *)page_file_name, &(metadata->file_handle));
    if (result == RC_OK) {
        initHashTable(page_table, MAX_TABLE_SIZE);
        metadata->pages = (BM_Page *)malloc(sizeof(BM_Page) * num_pages);
        if (metadata->pages == NULL) {
            free(metadata); // Free allocated metadata before returning
other

            return RC_BUFFER_POOL_INIT_ERROR;
        }
        for (int i = 0; i < num_pages; i++) {
            metadata->pages[i].frame_idx = i;
            metadata->pages[i].data = (char *)malloc(PAGE_SIZE);
            if (metadata->pages[i].data == NULL) {
                // Handle memory allocation failure
                for (int j = 0; j < i; j++) free(metadata->pages[j].data); // Free allocated data
                free(metadata->pages); // Free allocated pages array
                free(metadata); // Free allocated metadata
                return RC_BUFFER_POOL_INIT_ERROR;
            }
other

            metadata->pages[i].pin_count = 0;
            metadata->pages[i].dirty_flag = false;
            metadata->pages[i].is_used = false;
            metadata->pages[i].last_access = increment_global_time(metadata);
        }
        bm->mgmtData = (void *)metadata;
        bm->numPages = num_pages;
        bm->pageFile = (char *)&(metadata->file_handle);
        bm->strategy = strategy;
        return RC_OK;
    } else {
        free(metadata); // Free allocated metadata if openPageFile fails
other

        bm->mgmtData = NULL;
        return result;
    }
}

RC shutdown_buffer_pool(BM_BufferPool *const bm) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        HT_TableHandle *page_table = &(metadata->page_table);
        
        for (int i = 0; i < bm->numPages; i++) {
            if (pages[i].pin_count > 0) 
                return RC_WRITE_FAILED;
        }
        force_flush_pool(bm);
        for (int i = 0; i < bm->numPages; i++) {
            free(pages[i].data);
        }
        closePageFile(&(metadata->file_handle));
        freeHashTable(page_table);
        free(pages);
        free(metadata);
        return RC_OK;
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

RC force_flush_pool(BM_BufferPool *const bm) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        for (int i = 0; i < bm->numPages; i++) {
            if (pages[i].is_used && pages[i].dirty_flag && pages[i].pin_count == 0) {
                writeBlock(pages[i].page_num, &(metadata->file_handle), pages[i].data);
                metadata->write_ops++;
                pages[i].last_access = increment_global_time(metadata);
                pages[i].dirty_flag = false;
            }
        }
        return RC_OK;
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

/* Buffer Manager Interface Access Pages */
RC mark_dirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        HT_TableHandle *page_table = &(metadata->page_table);
        int frame_index;

        if (getValue(page_table, page->page_num, &frame_index) == 0) {
            pages[frame_index].last_access = increment_global_time(metadata);
            pages[frame_index].dirty_flag = true;
            return RC_OK;
        } else {
            return RC_IM_KEY_NOT_FOUND;
        }
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}
RC unpin_page(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        HT_TableHandle *page_table = &(metadata->page_table);
        int frame_index;

        if (getValue(page_table, page->page_num, &frame_index) == 0) {
            pages[frame_index].last_access = increment_global_time(metadata);
            pages[frame_index].pin_count--;
            if (pages[frame_index].pin_count < 0)
                pages[frame_index].pin_count = 0;
            return RC_OK;
        } else {
            return RC_IM_KEY_NOT_FOUND;
        }
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

RC force_page(BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        HT_TableHandle *page_table = &(metadata->page_table);
        int frame_index;

        if (getValue(page_table, page->page_num, &frame_index) == 0) {
            pages[frame_index].last_access = increment_global_time(metadata);
            if (pages[frame_index].pin_count == 0) {
                writeBlock(page->page_num, &(metadata->file_handle), pages[frame_index].data);
                metadata->write_ops++;
                pages[frame_index].dirty_flag = false;
                return RC_OK;
            } else {
                return RC_WRITE_FAILED;
            }
        } else {
            return RC_IM_KEY_NOT_FOUND;
        }
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

RC pin_page(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber page_num) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        HT_TableHandle *page_table = &(metadata->page_table);
        int frame_index;

        if (page_num != NO_PAGE) {
other

            if (getValue(page_table, page_num, &frame_index) == 0) {
                pages[frame_index].last_access = increment_global_time(metadata);
                pages[frame_index].pin_count++;
                page->data = pages[frame_index].data;
                page->page_num = page_num;
                return RC_OK;
            } else {
                BM_Page *page_frame;
                if (bm->strategy == RS_FIFO)
                    page_frame = fifo_replacement(bm);
                else // if (bm->strategy == RS_LRU)
                    page_frame = lru_replacement(bm);

                if (page_frame == NULL)
                    return RC_WRITE_FAILED;
                else {
                    setValue(page_table, page_num, page_frame->frame_idx);
                    ensureCapacity(page_num + 1, &(metadata->file_handle));
                    readBlock(page_num, &(metadata->file_handle), page_frame->data);
                    metadata->read_ops++;
                    page_frame->dirty_flag = false;
                    page_frame->pin_count = 1;
                    page_frame->is_used = true;
                    page_frame->page_num = page_num;
                    page->data = page_frame->data;
                    page->page_num = page_num;
                    return RC_OK;
                }
            }
        } else {
            return RC_IM_KEY_NOT_FOUND;
        }
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}
/* Buffer Manager Interface Access Pages */
RC unpin_page (BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        HT_TableHandle *page_table = &(metadata->page_table);
        int frame_index;

        if (getValue(page_table, page->page_num, &frame_index) == 0) {
            pages[frame_index].last_access = increment_global_time(metadata);
            pages[frame_index].pin_count--;
            if (pages[frame_index].pin_count < 0) {
                pages[frame_index].pin_count = 0; // Ensure pin count is non-negative
            }
            return RC_OK;
        } else {
            return RC_IM_KEY_NOT_FOUND;
        }
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

RC force_page (BM_BufferPool *const bm, BM_PageHandle *const page) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        HT_TableHandle *page_table = &(metadata->page_table);
        int frame_index;

        if (getValue(page_table, page->page_num, &frame_index) == 0) {
            pages[frame_index].last_access = increment_global_time(metadata);
            if (pages[frame_index].pin_count == 0) {
                writeBlock(page->page_num, &(metadata->file_handle), pages[frame_index].data);
                metadata->write_ops++;
                pages[frame_index].dirty_flag = false;
                return RC_OK;
            } else {
                return RC_WRITE_FAILED; // Page cannot be forced if pinned
            }
        } else {
            return RC_IM_KEY_NOT_FOUND;
        }
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

RC pin_page (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber page_num) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        HT_TableHandle *page_table = &(metadata->page_table);
        int frame_index;

        if (page_num != NO_PAGE) {
            if (getValue(page_table, page_num, &frame_index) == 0) {
                pages[frame_index].last_access = increment_global_time(metadata);
                pages[frame_index].pin_count++;
                page->data = pages[frame_index].data;
                page->page_num = page_num;
                return RC_OK;
            } else {
                BM_Page *page_frame;
                if (bm->strategy == RS_FIFO)
                    page_frame = fifo_replacement(bm);
                else // if (bm->strategy == RS_LRU)
                    page_frame = lru_replacement(bm);

                if (page_frame == NULL)
                    return RC_WRITE_FAILED;
                else {
                    setValue(page_table, page_num, page_frame->frame_idx);
                    ensureCapacity(page_num + 1, &(metadata->file_handle));
                    readBlock(page_num, &(metadata->file_handle), page_frame->data);
                    metadata->read_ops++;
                    page_frame->dirty_flag = false;
                    page_frame->pin_count = 1;
                    page_frame->is_used = true;
                    page_frame->page_num = page_num;
                    page->data = page_frame->data;
                    page->page_num = page_num;
                    return RC_OK;
                }
            }
        } else {
            return RC_IM_KEY_NOT_FOUND;
other

        }
    } else {
        return RC_FILE_HANDLE_NOT_INIT;
    }
}

/* Statistics Interface */
PageNumber *get_frame_contents (BM_BufferPool *const bm) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        PageNumber *array = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);
        if (array == NULL) return NULL; // Handle memory allocation failure
other

        for (int i = 0; i < bm->numPages; i++) {
            if (pages[i].is_used)
                array[i] = pages[i].page_num;
            else
                array[i] = NO_PAGE;
        }
        return array;
    } else {
        return NULL;
    }
}
/* Statistics Interface */
bool *get_dirty_flags (BM_BufferPool *const bm) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        bool *array = (bool *)malloc(sizeof(bool) * bm->numPages);
        if (array == NULL) return NULL; // Handle memory allocation failure
other

        for (int i = 0; i < bm->numPages; i++) {
            if (pages[i].is_used)
                array[i] = pages[i].dirty_flag;
            else
                array[i] = false;
        }
        return array;
    } else {
        return NULL;
    }
}

int *get_fix_counts (BM_BufferPool *const bm) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_Page *pages = metadata->pages;
        int *array = (int *)malloc(sizeof(int) * bm->numPages);
        if (array == NULL) return NULL; // Handle memory allocation failure
other

        for (int i = 0; i < bm->numPages; i++) {
            if (pages[i].is_used)
                array[i] = pages[i].pin_count;
            else
                array[i] = 0;
        }
        return array;
    } else {
        return NULL;
    }
}

int get_read_ops (BM_BufferPool *const bm) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        return metadata->read_ops;
    } else {
        return 0;
    }
}

int get_write_ops (BM_BufferPool *const bm) {
    if (bm->mgmtData != NULL) {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        return metadata->write_ops;
    } else {
        return 0;
    }
}

/* Replacement Policies */
BM_Page *fifo_replacement(BM_BufferPool *const bm) {
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_Page *pages = metadata->pages;
    int first_idx = metadata->queue_idx;
    int current_idx = metadata->queue_idx;

    do {
        current_idx = (current_idx + 1) % bm->numPages;
        if (!pages[current_idx].is_used)
            break;
    } while (current_idx != first_idx);

    metadata->queue_idx = current_idx;

    if (!pages[current_idx].is_used)
other

        return evict_page(bm, current_idx);
    else
        return NULL;
}

BM_Page *lru_replacement(BM_BufferPool *const bm) {
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_Page *pages = metadata->pages;
    Timestamp min = UINT32_MAX;
    int min_idx = -1;

    for (int i = 0; i < bm->numPages; i++) {
        if (!pages[i].is_used && pages[i].last_access < min) {
            min = pages[i].last_access;
            min_idx = i;
        }
    }
    
    if (min_idx != -1)
        return evict_page(bm, min_idx);
    else
        return NULL;
}