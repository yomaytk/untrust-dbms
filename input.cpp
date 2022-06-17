#include "input.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

InputBufWrapper::InputBufWrapper() {
    inputBuf             = (InputBuf*)malloc(sizeof(InputBuf));
    inputBuf->buf        = NULL;
    inputBuf->buf_length = 0;
    inputBuf->in_length  = 0;
}

InputBufWrapper::~InputBufWrapper() { free(inputBuf); }

void InputBufWrapper::readInput() {
    ssize_t byteRead = getline(&(inputBuf->buf), (size_t*)&(inputBuf->buf_length), stdin);

    if (byteRead <= 0) {
        printf("failed to read input.\n");
        exit(EXIT_FAILURE);
    }

    inputBuf->in_length         = (uint32_t)(byteRead - 1);
    inputBuf->buf[byteRead - 1] = 0;
}

bool InputBufWrapper::inputStartCheck(const char* command) {
    size_t size = strlen(command);
    return inputBuf->in_length >= size && 0 == strncmp(inputBuf->buf, command, size);
}