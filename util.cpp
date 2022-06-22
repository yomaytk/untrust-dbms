#include "util.h"
#include <iostream>
#include <string>

void debug_error(std::string message) {
    std::cout << message << std::endl;
    exit(1);
}
