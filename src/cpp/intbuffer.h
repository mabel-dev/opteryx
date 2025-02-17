#ifndef INTBUFFER_H
#define INTBUFFER_H

#include <vector>
#include <cstdint>
#include <cstddef>

class CIntBuffer {
private:
    std::vector<int64_t> buffer;

public:
    explicit CIntBuffer(size_t size_hint = 1024);

    void append(int64_t value);
    void extend(const std::vector<int64_t>& values);
    const int64_t* data() const noexcept;
    size_t size() const noexcept;
};

#endif // INTBUFFER_H
