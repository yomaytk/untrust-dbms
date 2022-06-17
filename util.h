#ifndef _UTIL_H_
#define _UTIL_H_

#include <iostream>
#include <string>
#include "Enclave_u.h"

void debug_error(std::string message);
Vector* new_vec();
void vec_push(Vector* vec, void* elem);
void free_vec(Vector* vec, int if_p);

#endif