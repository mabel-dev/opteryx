/*
 * Ultra-fast disk reader with platform-specific optimizations
 */

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <cstring>

size_t get_optimal_chunk_size(size_t file_size) {
    // For very small files, read in one chunk
    if (file_size <= (2 << 20)) {  // 2MB
        return file_size;
    }
    // For medium files, use 16MB chunks
    else if (file_size <= (128 << 20)) {  // 128MB
        return 16 << 20;
    }
    // For large files, use larger chunks but limit to 64MB
    else {
        return 64 << 20;
    }
}

#ifdef __linux__
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

int read_all_pread(const char* path, uint8_t* dst, size_t* out_len,
                   bool sequential, bool willneed, bool drop_after) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t size = static_cast<size_t>(st.st_size);
    
    const size_t CHUNK = get_optimal_chunk_size(size);
    
    // For files smaller than chunk size, read in one go
    if (size <= CHUNK) {
        if (sequential) posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
        if (willneed) posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
        
        ssize_t n = read(fd, dst, size);  // Use read instead of pread for single read
        if (n < 0 || static_cast<size_t>(n) != size) {
            int e = (n < 0) ? -errno : -EIO;
            close(fd);
            return e;
        }
    } else {
        // For larger files, use pread with fewer, larger chunks
        if (sequential) posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
        if (willneed) posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
        
        size_t off = 0;
        while (off < size) {
            size_t to_read = std::min(CHUNK, size - off);
            ssize_t n = pread(fd, dst + off, to_read, static_cast<off_t>(off));
            if (n <= 0) { 
                int e = (n == 0) ? -EIO : -errno; 
                close(fd); 
                return e; 
            }
            off += static_cast<size_t>(n);
        }
    }

    if (drop_after) posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    close(fd);
    *out_len = size;
    return 0;
}

#elif defined(__APPLE__)
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

int read_all_pread(const char* path, uint8_t* dst, size_t* out_len,
                   bool sequential, bool willneed, bool drop_after) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t size = static_cast<size_t>(st.st_size);
    
    const size_t CHUNK = get_optimal_chunk_size(size);
    
    if (sequential) fcntl(fd, F_RDAHEAD, 1);
    if (drop_after) fcntl(fd, F_NOCACHE, 1);

    // Try to read in larger chunks for better performance
    if (size <= CHUNK) {
        ssize_t n = read(fd, dst, size);
        if (n < 0 || static_cast<size_t>(n) != size) {
            int e = (n < 0) ? -errno : -EIO;
            close(fd);
            return e;
        }
    } else {
        size_t off = 0;
        while (off < size) {
            size_t to_read = std::min(CHUNK, size - off);
            ssize_t n = pread(fd, dst + off, to_read, static_cast<off_t>(off));
            if (n <= 0) { 
                int e = (n == 0) ? -EIO : -errno; 
                close(fd); 
                return e; 
            }
            off += static_cast<size_t>(n);
        }
    }

    close(fd);
    *out_len = size;
    return 0;
}

#else
// Windows optimized version
#include <windows.h>

int read_all_pread(const char* path, uint8_t* dst, size_t* out_len,
                   bool sequential, bool willneed, bool drop_after) {
    HANDLE hFile = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, 
                              NULL, OPEN_EXISTING, 
                              FILE_ATTRIBUTE_NORMAL | 
                              (sequential ? FILE_FLAG_SEQUENTIAL_SCAN : FILE_FLAG_RANDOM_ACCESS), 
                              NULL);
    if (hFile == INVALID_HANDLE_VALUE) {
        return -1;
    }

    DWORD sizeHigh = 0;
    DWORD sizeLow = GetFileSize(hFile, &sizeHigh);
    size_t size = (static_cast<size_t>(sizeHigh) << 32) | sizeLow;

    DWORD bytesRead = 0;
    BOOL success = ReadFile(hFile, dst, static_cast<DWORD>(size), &bytesRead, NULL);
    
    CloseHandle(hFile);

    if (!success || bytesRead != size) {
        return -1;
    }

    *out_len = bytesRead;
    return 0;
}
#endif

// Ultra-fast mmap version - often the fastest for file reading
int read_all_mmap(const char* path, uint8_t** dst, size_t* out_len) {
#ifdef __linux__
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st;
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t size = static_cast<size_t>(st.st_size);
    
    // Handle empty files - mmap doesn't work with size 0
    if (size == 0) {
        close(fd);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    void* mapped = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (mapped == MAP_FAILED) {
        return -errno;
    }

    *dst = static_cast<uint8_t*>(mapped);
    *out_len = size;
    
    // Caller must call munmap(*dst, *out_len) when done!
    return 0;
#elif defined(__APPLE__)
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -errno;

    struct stat st{};
    if (fstat(fd, &st) != 0) { 
        int e = -errno; 
        close(fd); 
        return e; 
    }

    size_t size = static_cast<size_t>(st.st_size);
    
    // Handle empty files - mmap doesn't work with size 0
    if (size == 0) {
        close(fd);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }
    
    void* mapped = mmap(NULL, size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (mapped == MAP_FAILED) {
        return -errno;
    }

    // On macOS, advise sequential access
    madvise(mapped, size, MADV_SEQUENTIAL);
    
    *dst = static_cast<uint8_t*>(mapped);
    *out_len = size;
    return 0;
#else
    // Windows mmap
    HANDLE hFile = CreateFileA(path, GENERIC_READ, FILE_SHARE_READ, 
                              NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hFile == INVALID_HANDLE_VALUE) return -1;

    DWORD sizeHigh = 0;
    DWORD sizeLow = GetFileSize(hFile, &sizeHigh);
    size_t size = (static_cast<size_t>(sizeHigh) << 32) | sizeLow;

    // Handle empty files
    if (size == 0) {
        CloseHandle(hFile);
        *dst = nullptr;
        *out_len = 0;
        return 0;
    }

    HANDLE hMapping = CreateFileMappingA(hFile, NULL, PAGE_READONLY, 0, 0, NULL);
    if (!hMapping) {
        CloseHandle(hFile);
        return -1;
    }

    void* mapped = MapViewOfFile(hMapping, FILE_MAP_READ, 0, 0, size);
    CloseHandle(hMapping);
    CloseHandle(hFile);

    if (!mapped) return -1;

    *dst = static_cast<uint8_t*>(mapped);
    *out_len = size;
    return 0;
#endif
}

int unmap_memory_c(unsigned char* addr, size_t size) {
#ifdef __linux__
    return munmap(addr, size) == 0 ? 0 : -errno;
#elif defined(__APPLE__)
    return munmap(addr, size) == 0 ? 0 : -errno;
#else
    return UnmapViewOfFile(addr) ? 0 : -1;
#endif
}