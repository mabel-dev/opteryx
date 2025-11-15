#pragma once
#include <stdint.h>
#include <stddef.h>

typedef enum {
    // Integer types: 1–19
    DRAKEN_INT8           = 1,
    DRAKEN_INT16          = 2,
    DRAKEN_INT32          = 3,
    DRAKEN_INT64          = 4,

    // Floating-point types: 20–29
    DRAKEN_FLOAT32        = 20,
    DRAKEN_FLOAT64        = 21,

    // Temporal types: 30–49
    DRAKEN_DATE32         = 30,
    DRAKEN_TIMESTAMP64    = 40,
    DRAKEN_TIME32         = 41,
    DRAKEN_TIME64         = 42,
    DRAKEN_INTERVAL       = 43,

    // Boolean: 50
    DRAKEN_BOOL           = 50,

    // String-like: 60–79
    DRAKEN_STRING         = 60,

    // Complex types: 80–99
    DRAKEN_ARRAY          = 80,

    // Catch-all
    DRAKEN_NON_NATIVE     = 100,  // Unoptimized or fallback-wrapped Arrow types
} DrakenType;

typedef struct {
    void* data;               // int64_t*, double*, etc.
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;
    size_t itemsize;
    DrakenType type;
} DrakenFixedBuffer;

typedef struct {
    uint8_t* data;            // UTF-8 bytes
    int32_t* offsets;         // [N+1] entries
    uint8_t* null_bitmap;     // optional
    size_t length;
    DrakenType type;
} DrakenVarBuffer;

typedef struct {
    int32_t* offsets;         // [length + 1] entries
    void* values;             // pointer to another column's data (DrakenFixedColumn*, DrakenVarColumn*, etc.)
    uint8_t* null_bitmap;     // optional, 1 bit per row
    size_t length;            // number of array entries (rows)
    DrakenType value_type;    // type of the child values
} DrakenArrayBuffer;

typedef struct {
    const char** column_names;       // length == num_columns
    DrakenType* column_types;        // length == num_columns
    void** columns;                  // (DrakenFixedColumn* or DrakenVarColumn*)[num_columns]
    size_t num_columns;
    size_t num_rows;
} DrakenMorsel;