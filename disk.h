#ifndef _DISK_H_
#define _DISK_H_

#include <stdint.h>
#include <stdio.h>
#include "bufferManager.h"

extern const uint64_t PAGESIZE;

class DiskManager {
   public:
    DiskManager();
    virtual ~DiskManager();
    void pageFlush(BufferManager* buffer_manager, int buffer_id, uint64_t pageId, size_t dataSize);
};

// typedef struct DiskManager {
//     FILE* heap_file;
//     uint8_t page_id;
// } DiskManager;

// DiskManager* new_disk(FILE* heap_file);
// DiskManager* open_disk(const char* path);
// char* read_page_data(DiskManager* diskmgr);
// void write_page_data(DiskManager* diskmgr);
// void allocate_page(DiskManager* diskmgr);

#endif