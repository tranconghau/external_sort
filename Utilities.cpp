#include <strings.h>
#include <iostream>
#include <sys/sysinfo.h>
#include <fstream>

#include "Utilities.h"

void debugLog(const std::string &param1, const std::string &param2)
{
#ifdef DEBUG
    std::cout << param1 << ": " << param2 << std::endl;
#endif
}

bool ignoreCaseCompare(const std::string &a, const std::string &b)
{
    if(strcasecmp(a.c_str(), b.c_str()) < 0)
        return true;
    return false;
}

unsigned long getRamSize()
{
    struct sysinfo info;
    if(sysinfo(&info) < 0)
        return 0;
    return info.totalram;
}

std::FILE* openFile(const std::string &fileName, const std::string &mode)
{
    auto fp = std::fopen(fileName.c_str(), mode.c_str());
    if(fp == nullptr)
    {
        std::cerr << "Error while opening file" << fileName << std::endl;
        std::exit(EXIT_FAILURE);
    }
    return fp;
}

std::size_t getFileSile(const std::string &file)
{
    auto ifs = std::ifstream(file, std::ios::ate);
    if (!ifs.is_open())
    {
        std::cerr << "Cannot open " << file << std::endl;
        std::exit(EXIT_FAILURE);
    }
    auto tmp = ifs.tellg();
    ifs.close();
    return tmp;
}
