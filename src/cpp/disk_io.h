#ifndef DISK_READER_H
#define DISK_READER_H

#include <cstddef>
#include <cstdint>

/**
 * Fast disk reader with platform-specific I/O optimizations
 * 
 * @param path File path to read
 * @param dst Destination buffer (must be pre-allocated)
 * @param out_len Output parameter for bytes read
 * @param sequential Hint for sequential access pattern
 * @param willneed Hint that data will be needed soon (prefetch)
 * @param drop_after Drop page cache after reading
 * @return 0 on success, negative errno on failure
 */
int read_all_pread(const char* path, uint8_t* dst, size_t* out_len,
                   bool sequential, bool willneed, bool drop_after);

int read_all_mmap(const char* path, uint8_t** dst, size_t* out_len);

int unmap_memory_c(unsigned char* addr, size_t size);
#endif // DISK_READER_H