#include "directories.h"

#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <string>
#include <vector>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

// List files in a directory (non-recursive)
int list_directory(const char* path, file_info_t** files, size_t* count) {
    DIR* dir = opendir(path);
    if (!dir) return -errno;
    
    struct dirent* entry;
    size_t capacity = 32;
    size_t num_files = 0;
    
    *files = (file_info_t*)malloc(capacity * sizeof(file_info_t));
    if (!*files) {
        closedir(dir);
        return -ENOMEM;
    }
    
    while ((entry = readdir(dir)) != NULL) {
        // Skip . and ..
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        
        // Resize if needed
        if (num_files >= capacity) {
            capacity *= 2;
            file_info_t* new_files = (file_info_t*)realloc(*files, capacity * sizeof(file_info_t));
            if (!new_files) {
                // Cleanup on failure
                for (size_t i = 0; i < num_files; i++) {
                    free((*files)[i].name);
                }
                free(*files);
                closedir(dir);
                return -ENOMEM;
            }
            *files = new_files;
        }
        
        // Build full path for stat
        char full_path[PATH_MAX];
        snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);
        
        // Get file info
        struct stat st;
        if (stat(full_path, &st) != 0) {
            continue; // Skip files we can't stat
        }
        
        // Fill file info
        (*files)[num_files].name = strdup(entry->d_name);
        (*files)[num_files].is_directory = S_ISDIR(st.st_mode);
        (*files)[num_files].is_regular_file = S_ISREG(st.st_mode);
        (*files)[num_files].size = (int64_t)st.st_size;
        (*files)[num_files].mtime = (int64_t)st.st_mtime;
        
        num_files++;
    }
    
    closedir(dir);
    *count = num_files;
    return 0;
}

// Free file list
void free_file_list(file_info_t* files, size_t count) {
    for (size_t i = 0; i < count; i++) {
        free(files[i].name);
    }
    free(files);
}

// Recursive directory walk with callback
typedef int (*file_callback_t)(const char* path, const struct stat* st, void* user_data);

int walk_directory(const char* base_path, file_callback_t callback, void* user_data, int max_depth) {
    DIR* dir = opendir(base_path);
    if (!dir) return -errno;
    
    struct dirent* entry;
    char path[PATH_MAX];
    
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        
        snprintf(path, sizeof(path), "%s/%s", base_path, entry->d_name);
        
        struct stat st;
        if (stat(path, &st) != 0) continue;
        
        // Call callback for this entry
        int result = callback(path, &st, user_data);
        if (result != 0) {
            closedir(dir);
            return result; // Early termination if callback returns non-zero
        }
        
        // Recurse into subdirectories if we haven't hit max depth
        if (S_ISDIR(st.st_mode) && max_depth != 0) {
            int result = walk_directory(path, callback, user_data, max_depth - 1);
            if (result != 0) {
                closedir(dir);
                return result;
            }
        }
    }
    
    closedir(dir);
    return 0;
}

static std::string join_paths(const std::string& base, const char* name) {
    if (base.empty()) {
        return std::string(name);
    }
    if (base == "/") {
        return std::string("/") + name;
    }
    if (base.back() == '/') {
        std::string result(base);
        result.append(name);
        return result;
    }
    std::string result(base);
    result.push_back('/');
    result.append(name);
    return result;
}

static bool matches_extension(const char* name, const std::vector<std::string>& extensions) {
    if (extensions.empty()) {
        return true;
    }

    const size_t name_len = strlen(name);
    for (const auto& ext : extensions) {
        const size_t ext_len = ext.length();
        if (ext_len == 0) {
            continue;
        }
        if (name_len >= ext_len && strncmp(name + name_len - ext_len, ext.c_str(), ext_len) == 0) {
            return true;
        }
    }
    return false;
}

static int classify_entry(const std::string& path, const struct dirent* entry, bool* is_directory,
                          bool* is_file) {
    *is_directory = false;
    *is_file = false;

#if defined(DT_DIR)
    unsigned char dtype = entry->d_type;
    if (dtype == DT_DIR) {
        *is_directory = true;
        return 0;
    }
    if (dtype == DT_REG) {
        *is_file = true;
        return 0;
    }
    if (dtype != DT_LNK && dtype != DT_UNKNOWN) {
        return 0;
    }
#endif

    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        return -errno;
    }

    if (S_ISDIR(st.st_mode)) {
        *is_directory = true;
    } else if (S_ISREG(st.st_mode)) {
        *is_file = true;
    }

    return 0;
}

int list_matching_files(const char* base_path, const char** extensions, size_t ext_count,
                        char*** files, size_t* count) {
    if (!base_path || !files || !count) {
        return -EINVAL;
    }

    *files = nullptr;
    *count = 0;

    std::vector<std::string> extension_list;
    extension_list.reserve(ext_count);
    for (size_t i = 0; i < ext_count; ++i) {
        if (extensions[i] != nullptr) {
            extension_list.emplace_back(extensions[i]);
        }
    }

    std::vector<std::string> stack;
    stack.emplace_back(base_path);

    std::vector<std::string> matches;
    matches.reserve(128);

    bool processed_root = false;

    while (!stack.empty()) {
        std::string current = std::move(stack.back());
        stack.pop_back();

        DIR* dir = opendir(current.c_str());
        if (!dir) {
            int err = errno;
            if (current == base_path) {
                return -err;
            }
            // Skip directories that disappear or are inaccessible during traversal
            continue;
        }

        processed_root = true;

        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }

            std::string full_path = join_paths(current, entry->d_name);

            bool is_directory = false;
            bool is_file = false;
            int classify_result = classify_entry(full_path, entry, &is_directory, &is_file);
            if (classify_result != 0) {
                continue;
            }

            if (is_directory) {
                stack.emplace_back(std::move(full_path));
            } else if (is_file) {
                if (matches_extension(entry->d_name, extension_list)) {
                    matches.emplace_back(std::move(full_path));
                }
            }
        }

        closedir(dir);
    }

    if (!processed_root) {
        return -ENOENT;
    }

    const size_t total = matches.size();
    if (total == 0) {
        return 0;
    }

    char** out = (char**)malloc(total * sizeof(char*));
    if (!out) {
        return -ENOMEM;
    }

    for (size_t i = 0; i < total; ++i) {
        out[i] = strdup(matches[i].c_str());
        if (!out[i]) {
            for (size_t j = 0; j < i; ++j) {
                free(out[j]);
            }
            free(out);
            return -ENOMEM;
        }
    }

    *files = out;
    *count = total;
    return 0;
}

void free_file_names(char** files, size_t count) {
    if (!files) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        free(files[i]);
    }
    free(files);
}