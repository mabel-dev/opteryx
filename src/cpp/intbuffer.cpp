// intbuffer.cpp
#include "intbuffer.h"

#include <algorithm>

// Fallback constants for buffer growth strategy.
#ifndef INITIAL_CAPACITY
#define INITIAL_CAPACITY 1024
#endif

#ifndef GROWTH_FACTOR
#define GROWTH_FACTOR 2
#endif

CIntBuffer::CIntBuffer(size_t size_hint) {
    buffer.reserve(size_hint > 0 ? size_hint : INITIAL_CAPACITY);
}

void CIntBuffer::append(int64_t value) {
    buffer.push_back(value);
}

void CIntBuffer::append(unsigned long value) {
    buffer.push_back(static_cast<int64_t>(value));
}

void CIntBuffer::append(unsigned long long value) {
    buffer.push_back(static_cast<int64_t>(value));
}

void CIntBuffer::append(int64_t value1, int64_t value2) {
    if (buffer.capacity() - buffer.size() < 2) {
        buffer.reserve(buffer.capacity() * GROWTH_FACTOR);
    }
    buffer.push_back(value1);
    buffer.push_back(value2);
}

void CIntBuffer::append(const int64_t* values, size_t count) {
    if (count > 0) {
        const size_t new_size = buffer.size() + count;
        if (new_size > buffer.capacity()) {
            buffer.reserve(std::max(new_size, buffer.capacity() * GROWTH_FACTOR));
        }
        buffer.insert(buffer.end(), values, values + count);
    }
}

void CIntBuffer::append_optimized(int64_t value) {
    // More aggressive growth strategy for append-heavy workloads
    if (buffer.size() == buffer.capacity()) {
        buffer.reserve(buffer.capacity() * GROWTH_FACTOR + 1024); // Extra padding
    }
    buffer.push_back(value);
}

void CIntBuffer::append_repeated(int64_t value, size_t count) {
    if (count == 0) {
        return;
    }

    const size_t new_size = buffer.size() + count;
    if (new_size > buffer.capacity()) {
        const size_t current_capacity = buffer.capacity();
        const size_t growth_candidate = current_capacity > 0 ? current_capacity * GROWTH_FACTOR : new_size;
        buffer.reserve(std::max(new_size, growth_candidate));
    }

    buffer.insert(buffer.end(), count, value);
}

void CIntBuffer::extend(const std::vector<int64_t>& values) {
    append(values.data(), values.size());
}

void CIntBuffer::extend(const int64_t* values, size_t count) {
    append(values, count);
}

void CIntBuffer::reserve(size_t additional_capacity) {
    buffer.reserve(buffer.size() + additional_capacity);
}

const int64_t* CIntBuffer::data() const noexcept {
    return buffer.data();
}

size_t CIntBuffer::size() const noexcept {
    return buffer.size();
}

size_t CIntBuffer::capacity() const noexcept {
    return buffer.capacity();
}

void CIntBuffer::shrink_to_fit() {
    buffer.shrink_to_fit();
}

void CIntBuffer::clear() noexcept {
    buffer.clear();
}

template<typename InputIt>
void CIntBuffer::extend(InputIt first, InputIt last) {
    buffer.insert(buffer.end(), first, last);
}