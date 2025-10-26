# distutils: language = c++
# cython: language_level=3

cdef extern from "errno.h":
    int errno

cdef extern from "stdlib.h":
    int posix_memalign(void **memptr, size_t alignment, size_t size)
    void free(void *ptr)

cdef extern from "unistd.h":
    int close(int fd)

cdef extern from "fcntl.h":
    int open(const char *pathname, int flags, ...)

cdef extern from "sys/types.h":
    pass

cdef extern from "sys/uio.h":
    ctypedef struct iovec:
        void   *iov_base
        size_t  iov_len

cdef extern from "liburing.h":
    ctypedef struct io_uring:
        pass

    ctypedef struct io_uring_sqe:
        pass

    ctypedef struct io_uring_cqe:
        unsigned int    flags
        int             res
        unsigned long long user_data

    int  io_uring_queue_init(unsigned entries, io_uring *ring, unsigned flags)
    void io_uring_queue_exit(io_uring *ring)

    io_uring_sqe* io_uring_get_sqe(io_uring *ring)
    int  io_uring_submit(io_uring *ring)

    int  io_uring_wait_cqe(io_uring *ring, io_uring_cqe **cqe_ptr)
    int  io_uring_peek_cqe(io_uring *ring, io_uring_cqe **cqe_ptr)
    void io_uring_cqe_seen(io_uring *ring, io_uring_cqe *cqe)

    # buffer registration
    int  io_uring_register_buffers(io_uring *ring, const iovec *iovecs, unsigned nr_iovecs)
    int  io_uring_unregister_buffers(io_uring *ring)

    # prep helpers
    void io_uring_prep_read_fixed(io_uring_sqe *sqe, int fd, void *buf, unsigned nbytes, long long offset, int buf_index)

    # user_data helpers (declared in liburing.h as static inline)
    void               io_uring_sqe_set_data64(io_uring_sqe *sqe, unsigned long long data)
    unsigned long long io_uring_cqe_get_data64(const io_uring_cqe *cqe)

    # common setup flags
    cdef unsigned IORING_SETUP_CLAMP
    cdef unsigned IORING_SETUP_COOP_TASKRUN
    cdef unsigned IORING_SETUP_SINGLE_ISSUER

# open(2) flags
cdef extern from "fcntl.h":
    int O_RDONLY
    int O_DIRECT
    int O_CLOEXEC

# Helper (defined in .pyx)
cdef int _check_errno(int rc)