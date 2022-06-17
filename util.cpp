#include "util.h"
#include <iostream>
#include <string>

void debug_error(std::string message) {
    std::cout << message << std::endl;
    exit(1);
}

Vector* new_vec() {
    Vector* vec   = (Vector*)malloc(sizeof(Vector));
    vec->data     = (void**)malloc(sizeof(void*) * 160);
    vec->capacity = 160;
    vec->len      = 0;
    return vec;
}

void vec_push(Vector* vec, void* elem) {
    if (vec->len == vec->capacity) {
        vec->capacity += 160;
        vec->data = (void**)realloc(vec->data, sizeof(void*) * vec->capacity);
    }
    vec->data[vec->len++] = elem;
}

void free_vec(Vector* vec, int if_p) {
    if (if_p) {
        for (int i = (int)vec->len - 1; i >= 0; i--) {
            free(vec->data[i]);
        }
    }
    free(vec->data);
    free(vec);
}