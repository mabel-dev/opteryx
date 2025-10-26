# distutils: language = c++
# cython: language_level=3

from cpython.mem cimport PyMem_Malloc, PyMem_Free
from libc.stdint cimport uint64_t, uintptr_t
from libc.string cimport memset

# IMPORTANT: cimport the module by its full package path
cimport opteryx.compiled.io.iouring as C

cdef int _check_errno(int rc):
    if rc < 0:
        raise OSError(-rc, "io_uring error")
    return rc

cdef class BufferPool:
    cdef void **ptrs
    cdef C.iovec *iov
    cdef size_t nbuf
    cdef size_t buf_size
    cdef size_t alignment

    def __cinit__(self, size_t nbuf, size_t buf_size, size_t alignment=4096):
        self.nbuf = nbuf
        self.buf_size = buf_size
        self.alignment = alignment
        self.ptrs = <void **>PyMem_Malloc(nbuf * sizeof(void *))
        if self.ptrs == NULL:
            raise MemoryError("alloc ptrs")
        self.iov = <C.iovec *>PyMem_Malloc(nbuf * sizeof(C.iovec))
        if self.iov == NULL:
            PyMem_Free(self.ptrs)
            raise MemoryError("alloc iov")
        for i in range(nbuf):
            self.ptrs[i] = NULL

        cdef void *p
        for i in range(nbuf):
            if C.posix_memalign(&p, alignment, buf_size) != 0:
                self._cleanup(i)
                raise MemoryError(f"posix_memalign failed for buffer {i}")
            memset(p, 0, buf_size)
            self.ptrs[i] = p
            self.iov[i].iov_base = p
            self.iov[i].iov_len = buf_size

    cdef void _cleanup(self, size_t upto):
        cdef size_t j
        for j in range(upto):
            if self.ptrs[j] != NULL:
                C.free(self.ptrs[j])
        if self.iov != NULL:
            PyMem_Free(self.iov)
        if self.ptrs != NULL:
            PyMem_Free(self.ptrs)

    def __dealloc__(self):
        self._cleanup(self.nbuf)

    property n:
        def __get__(self):
            return self.nbuf

    property size:
        def __get__(self):
            return self.buf_size

    def addr(self, size_t idx) -> int:
        if idx >= self.nbuf:
            raise IndexError
        return <uintptr_t>self.ptrs[idx]

    def view(self, size_t idx, Py_ssize_t length):
        if idx >= self.nbuf or length > self.buf_size:
            raise IndexError
        cdef unsigned char[:] mv = <unsigned char[:length]> self.ptrs[idx]
        return mv


cdef class Uring:
    cdef C.io_uring ring
    cdef BufferPool pool
    cdef bint buffers_registered

    def __cinit__(self, unsigned entries=4096, unsigned flags=0):
        if flags == 0:
            flags = C.IORING_SETUP_CLAMP | C.IORING_SETUP_COOP_TASKRUN | C.IORING_SETUP_SINGLE_ISSUER
        _check_errno(C.io_uring_queue_init(entries, &self.ring, flags))
        self.pool = None
        self.buffers_registered = False

    def __dealloc__(self):
        try:
            if self.buffers_registered:
                C.io_uring_unregister_buffers(&self.ring)
        except Exception:
            pass
        C.io_uring_queue_exit(&self.ring)

    def register_buffers(self, BufferPool pool):
        if pool is None:
            raise ValueError("pool is None")
        _check_errno(C.io_uring_register_buffers(&self.ring, pool.iov, <unsigned>pool.nbuf))
        self.pool = pool
        self.buffers_registered = True

    def submit_read_fixed(self, int fd, size_t buf_index, size_t nbytes, long long offset, uint64_t user_data=0):
        if not self.buffers_registered:
            raise RuntimeError("buffers not registered")
        if buf_index >= self.pool.nbuf:
            raise IndexError
        if nbytes > self.pool.buf_size:
            raise ValueError("nbytes > buffer size")

        cdef C.io_uring_sqe* sqe = C.io_uring_get_sqe(&self.ring)
        if sqe == NULL:
            raise RuntimeError("no available SQE (ring full)")

        C.io_uring_prep_read_fixed(sqe, fd, self.pool.ptrs[buf_index],
                                   <unsigned>nbytes, offset, <int>buf_index)
        # Use helper instead of touching struct fields
        C.io_uring_sqe_set_data64(sqe, user_data)

    def submit(self) -> int:
        return _check_errno(C.io_uring_submit(&self.ring))

    def wait_cqe(self):
        cdef C.io_uring_cqe* cqe
        _check_errno(C.io_uring_wait_cqe(&self.ring, &cqe))
        res = cqe.res
        ud = C.io_uring_cqe_get_data64(cqe)
        C.io_uring_cqe_seen(&self.ring, cqe)
        return res, ud

    def peek_cqe(self):
        cdef C.io_uring_cqe* cqe
        rc = C.io_uring_peek_cqe(&self.ring, &cqe)
        if rc == 0 and cqe != NULL:
            res = cqe.res
            ud = C.io_uring_cqe_get_data64(cqe)
            C.io_uring_cqe_seen(&self.ring, cqe)
            return res, ud
        return None


def open_direct(path: bytes) -> int:
    """Open O_RDONLY|O_DIRECT|O_CLOEXEC. Caller must close(fd)."""
    cdef int fd = C.open(<const char*>path, C.O_RDONLY | C.O_DIRECT | C.O_CLOEXEC)
    if fd < 0:
        raise OSError(C.errno, "open(O_DIRECT) failed")
    return fd


def close_fd(int fd):
    if C.close(fd) != 0:
        raise OSError(C.errno, "close failed")
