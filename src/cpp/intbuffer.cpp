#include "intbuffer.h"

CIntBuffer::CIntBuffer(size_t size_hint) {
    buffer.reserve(size_hint);  // Pre-allocate memory
}

void CIntBuffer::append(int64_t value) {
    buffer.push_back(value);
}

void CIntBuffer::extend(const std::vector<int64_t>& values) {
    buffer.insert(buffer.end(), values.begin(), values.end());
}

const int64_t* CIntBuffer::data() const noexcept {
    return buffer.data();
}

size_t CIntBuffer::size() const noexcept {
    return buffer.size();
}