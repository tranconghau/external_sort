#include <cstdio>
//for getrlimit
#include <sys/time.h>
#include <sys/resource.h>

#include <iostream>
#include <algorithm>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>

#include "MinHeap.h"
#include "Utilities.h"

#ifndef LINE_MAX
#define LINE_MAX 2048
#endif

std::atomic<std::size_t> fileNameInNum(1);
std::mutex readMutex;
auto moreInput = true;
std::mutex tempFileMux;

void mergeSomeFiles(const std::string &out, const std::vector<std::string> &list,
                    unsigned long processed, unsigned long numFiles)
{
    auto inFiles = std::vector<std::FILE*>();
    for(auto i = 0; i < numFiles; i++)
        inFiles.push_back(openFile(list[processed + i], "r"));
    std::remove(out.c_str());
    //create output file
    auto outFd = openFile(out, "w");

    // Create a min heap
    MinHeapNode *harr = new MinHeapNode[numFiles];
    char line[LINE_MAX];
    auto i = 0;
    for(; i < numFiles; i++)
    {
        // break if eof
        if(std::fgets(line, LINE_MAX, inFiles[i]) == nullptr)
            break;
        harr[i].element = line;
        harr[i].idx = i;
    }
    // Create the heap
    MinHeap hp(harr, i);
    // Now one by one get the minimum element from min heap and replace it with next element
    while(!hp.empty())
    {
        // Get the minimum element and store it in output file
        MinHeapNode root = hp.getMin();
        std::fputs(root.element.c_str(), outFd);

        // Find the next element that will replace current root of heap.
        // The next element belongs to same input file as the current min element.
        if(std::fgets(line, LINE_MAX, inFiles[root.idx]) == nullptr)
        {
            //root.element = maxString; ==> waste time
            hp.deleteMin();
        }
        else
            root.element = line;
        // Replace root with next element of input file
        hp.replaceMin(root);
    }

    for(auto i = 0; i < numFiles; i++)
    {
        std::fclose(inFiles[i]);
        std::remove(list[processed + i].c_str());
    }

    std::fclose(outFd);
}

void mergeFiles(const std::string &outputFile, std::vector<std::string> &files, unsigned long fdLimit)
{
    auto numFdLimit = fdLimit - 4;//4 means 0,1,2 and fd for output
    auto numProcessed = 0;
    if(files.size() > fdLimit)
    {
        auto tempList = std::vector<std::string>(0);
        while(files.size() > numProcessed)
        {
            auto fileName = std::to_string(fileNameInNum++);
            auto nextProcess = files.size() - numProcessed;
            nextProcess = nextProcess > numFdLimit ? numFdLimit : nextProcess;
            mergeSomeFiles(fileName.c_str(), files, numProcessed, nextProcess);
            numProcessed += nextProcess;
            tempList.push_back(std::move(fileName));
        }

        if(tempList.size() > 0) //Assume the size less than fdLimit :D
            mergeSomeFiles(outputFile, tempList, 0, tempList.size());
    }
    else
        mergeSomeFiles(outputFile, files, 0, files.size());
}

void readAndSort(std::vector<std::string> &tempFiles, std::FILE *in, std::size_t runSize)
{
    if(in ==  nullptr)
        return;
    while(true)
    {
        //create temp file
        auto fileName = std::to_string(fileNameInNum++);
        std::remove(fileName.c_str());
        auto outTemp = openFile(fileName, "w");

        auto numRead = 0;
        char line[LINE_MAX] = {0};
        auto temp = std::string();
        auto lineVec = std::vector<std::string>();
        {
            std::lock_guard<std::mutex> lock(readMutex);
            if (moreInput)
            {
                while(numRead < runSize)
                {
                    if(std::fgets(line, LINE_MAX, in) == nullptr)
                    {
                        moreInput = false;
                        break;
                    }

                    if(line[0] == '\n')  //ignore empty line
                        numRead += 1;
                    else
                    {
                        temp = line;
                        numRead += temp.length() + 1; // 1 for '\n'
                        lineVec.push_back(std::move(temp));
                    }
                    temp = "";
                }
            }
            else
            {
                std::fclose(outTemp);
                std::remove(fileName.c_str());
                return;
            }
        }

        std::sort(lineVec.begin(), lineVec.end());
        //write to temp file
        for(auto &i : lineVec)
        {
            std::fputs(i.c_str(), outTemp);
            if(i[i.length() - 1] != '\n')
                std::fputc('\n', outTemp);
        }
        std::fclose(outTemp);

        {
            std::lock_guard<std::mutex> lock(tempFileMux);
            tempFiles.push_back(std::move(fileName));
        }
    }
}

std::vector<std::string>
createInitialRuns(const std::string &inputFile, std::size_t limitBytes, unsigned int numThreads)
{
    auto in = openFile(inputFile, "r");
    auto tempFiles = std::vector<std::string>();
    auto runSize = limitBytes / numThreads;
    auto threadList = std::vector<std::thread>();
    for(auto i = 0; i < numThreads; i++)
    {
        threadList.push_back(std::thread(readAndSort, std::ref(tempFiles), in, runSize));
    }

    std::for_each(threadList.begin(), threadList.end(), [](std::thread &th)
    {
        th.join();
    });
    std::fclose(in);
    return tempFiles;
}

// For sorting data stored on disk
void externalSort(const std::string &inputFile, const std::string &outputFile, std::size_t limitBytes)
{
    auto numThreads = std::thread::hardware_concurrency();
    if(numThreads == 0)
    {
        std::cerr << "Num of threads seem invalid" << std::endl;
        return;
    }

    debugLog("Using thread", std::to_string(numThreads));
    //get file descriptor limit
    struct rlimit resourceLimit;
    if(getrlimit(RLIMIT_NOFILE, &resourceLimit) != 0)
    {
        std::cerr << "Cannot get fd limit" << std::endl;
        return;
    }
    debugLog("System limit fd", std::to_string(resourceLimit.rlim_cur));

    // read the input file, create the initial runs, and assign the runs to the scratch output files
    auto tempFiles = createInitialRuns(inputFile, limitBytes, numThreads);
    debugLog("Using temp file", std::to_string(tempFiles.size()));

    // Merge the runs using the K-way merging
    mergeFiles(outputFile, tempFiles, resourceLimit.rlim_cur);

    //deletes temp files
    for(auto &i : tempFiles)
        std::remove(i.c_str());
}

int main(int argc, const char *argv[])
{
    if(argc != 4)
    {
        std::cerr << "Using: " << argv[0] << " <intput.txt> <output.txt> <limit_in_bytes>" << std::endl;
        return EXIT_FAILURE;
    }

    auto limitInBytes = std::stol(argv[3]); //limit in bytes
    if(limitInBytes < 0)
    {
        std::cerr << "Invalid limit bytes" << std::endl;
        return EXIT_FAILURE;
    }

    auto inputFile = std::string(argv[1]);
    auto outputFile = std::string(argv[2]);
    auto fileSize = getFileSile(inputFile);

    if(fileSize == 0)
    {
        std::cerr << "Input is empty" << std::endl;
        return EXIT_FAILURE;
    }

#ifndef DEBUG
    std::cout << "Processing..." << std::endl;
#endif

    externalSort(inputFile, outputFile, limitInBytes);

#ifndef DEBUG
    std::cout << "Done" << std::endl;
#endif

    return EXIT_SUCCESS;
}