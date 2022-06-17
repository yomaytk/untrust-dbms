#include "disk.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

const uint64_t PAGESIZE = 4096;

// DiskManager::DiskManager(const char* filePath) {
//     file_path = filePath;
//     fd        = -1;
//     if (strlen(file_path) > 0) {
//         fd = open(file_path, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
//     }

//     off_t offset = lseek(fd, 0, SEEK_END);
//     file_length  = offset;
// }

// void DiskManager::pageFlush(uint64_t pageId, size_t dataSize) {
//     if (pager->pages[pageId] == NULL) {
//         printf("Cannot flush null page.\n");
//         exit(EXIT_FAILURE);
//     }

//     off_t off = lseek(pager->fd, PAGE_SIZE * pageId, SEEK_SET);

//     if (off == -1) {
//         printf("Seeking error.");
//         exit(EXIT_FAILURE);
//     }

//     ssize_t bytes = write(pager->fd, pager->pages[pageId], dataSize);

//     if (bytes == -1) {
//         printf("Writing file error.\n");
//         exit(EXIT_FAILURE);
//     }
// }

// DiskManager::~DiskManager() { close(fd); }