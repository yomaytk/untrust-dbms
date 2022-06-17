#ifndef _INPUT_HPP_
#define _INPUT_HPP_

#include "Enclave_u.h"
#include <stdint.h>

class InputBufWrapper {
  public:
    InputBufWrapper();
    virtual ~InputBufWrapper();
    void readInput();
    bool inputStartCheck(const char* command);
    InputBuf* inputBuf;
};

#endif