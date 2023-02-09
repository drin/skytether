/**
 * Header for format-specific definitions used by this library.
 * Author: Aldrin Montana
 */
#ifndef __STRUCTURES_HPP
#define __STRUCTURES_HPP


// ------------------------------
// Dependencies

#include "dataformats.hpp"

struct ChunkedDictionary {

    ChunkedDictionary(ChunkedArrayPtr chunked_dict);

    int64_t  size();
    ArrayPtr words();
};


#endif // __STRUCTURES_HPP
