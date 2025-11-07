#ifndef OPTERYX_DIRECTORIES_H
#define OPTERYX_DIRECTORIES_H

#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char* name;
    int is_directory;
    int is_regular_file;
    int64_t size;
    int64_t mtime;
} file_info_t;

int list_directory(const char* path, file_info_t** files, size_t* count);
void free_file_list(file_info_t* files, size_t count);

typedef int (*file_callback_t)(const char* path, const struct stat* st, void* user_data);

int walk_directory(const char* base_path, file_callback_t callback, void* user_data, int max_depth);
int list_matching_files(const char* base_path, const char** extensions, size_t ext_count,
                        char*** files, size_t* count);
void free_file_names(char** files, size_t count);

#ifdef __cplusplus
}
#endif

#endif
