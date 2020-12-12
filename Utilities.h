#pragma once
#include <string>

template <typename T>
void swap(T *x, T *y)
{
    auto temp = *x;
    *x = *y;
    *y = temp;
}

void debugLog(const std::string &param1, const std::string &param2);

bool ignoreCaseCompare(const std::string &a, const std::string &b);

//get RAM memory
unsigned long getRamSize();

std::FILE* openFile(const std::string &fileName, const std::string &mode);

std::size_t getFileSile(const std::string &file);