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

const std::string tempDir = "/tmp/";
std::atomic<std::size_t> fileNameInNum(1);
std::mutex readMutex;
auto moreInput = true;
std::mutex tempFileMux;

void mergeSomeFiles(const std::string &out, const std::vector<std::string> &list,
                    unsigned long processed, unsigned long numFiles)
{
    auto inFiles = std::vector<std::ifstream>();
    for(auto i = 0; i < numFiles; i++)
        inFiles.push_back(std::ifstream(list[processed + i]));

    std::remove(out.c_str());
    auto outStream = std::ofstream(out);

    // Create a min heap
    MinHeapNode *harr = new MinHeapNode[numFiles];
    auto line = std::string();
    auto i = 0;
    for(; i < numFiles; i++)
    {
        if(!std::getline(inFiles[i], line))
            continue;

        harr[i].element = std::move(line);
        harr[i].idx = i;
    }

    // Create the heap
    MinHeap hp(harr, i);
    // Now one by one get the minimum element from min heap and replace it with next element
    while(!hp.empty())
    {
        // Get the minimum element and store it in output file
        MinHeapNode root = hp.getMin();
        outStream.write(root.element.c_str(), root.element.length());
        outStream.write("\n", 1);

        // Find the next element that will replace current root of heap.
        // The next element belongs to same input file as the current min element.
        std::getline(inFiles[root.idx], line);
        if (inFiles[root.idx].eof() && line.empty())
        {
            //root.element = maxString; ==> waste time
            hp.deleteMin();
        }

        if(!line.empty())
        {
            root.element = std::move(line);
            hp.replaceMin(root);
        }
    }

    for(auto i = 0; i < numFiles; i++)
    {
        inFiles[i].close();
        std::remove(list[processed + i].c_str());
    }

    outStream.close();
}

void mergeFiles(const std::string &outputFile, std::vector<std::string> &files, unsigned long fdLimit)
{
    auto numFdLimit = fdLimit - 4;//4 means 0,1,2 and fd for output
    auto numProcessed = 0UL;
    if(files.size() > numFdLimit)
    {
        auto tempList = std::vector<std::string>(0);
        while(files.size() > numProcessed)
        {
            auto fileName = tempDir + std::to_string(fileNameInNum++);
            auto nextProcess = files.size() - numProcessed;
            nextProcess = nextProcess > numFdLimit ? numFdLimit : nextProcess;
            mergeSomeFiles(fileName.c_str(), files, numProcessed, nextProcess);
            numProcessed += nextProcess;
            tempList.push_back(std::move(fileName));
        }

        if(tempList.size() > 0) //Assume the size less than numFdLimit :D
            mergeSomeFiles(outputFile, tempList, 0, tempList.size());
    }
    else
        mergeSomeFiles(outputFile, files, 0, files.size());
}

void readAndSort(std::vector<std::string> &tempFiles, std::ifstream &in, std::size_t runSize)
{
    while(true)
    {
        auto numRead = 0UL;
        auto line = std::string();
        auto lineVec = std::vector<std::string>();
        {
            std::lock_guard<std::mutex> lock(readMutex);
            if (moreInput)
            {
                while(numRead < runSize)
                {
                    if(!std::getline(in, line))
                    {
                        moreInput = false;
                        break;
                    }
                    if(line.empty() && !in.eof())
                        numRead += 1;
                    else
                    {
                        numRead += line.length() + 1; // 1 for '\n'
                        lineVec.push_back(std::move(line));
                    }
                    if(in.eof())
                        break;
                }
            }
            else
            {
                return;
            }
        }

        if(lineVec.size() > 0)
        {
            std::sort(lineVec.begin(), lineVec.end());
            //create & write to temp file
            auto fileName = tempDir + std::to_string(fileNameInNum++);
            std::remove(fileName.c_str());
            auto outTemp = std::ofstream(fileName);
            for(auto &i : lineVec)
            {
                outTemp.write(i.c_str(), i.length());
                outTemp.write("\n", 1);
            }
            outTemp.close();
            {
                std::lock_guard<std::mutex> lock(tempFileMux);
                tempFiles.push_back(std::move(fileName));
            }
        }
    }
}

std::vector<std::string>
createInitialRuns(const std::string &inputFile, std::size_t limitBytes, unsigned int numThreads)
{
    auto in = std::ifstream(inputFile);
    auto tempFiles = std::vector<std::string>();
    auto runSize = limitBytes / numThreads;
    auto threadList = std::vector<std::thread>();
    for(auto i = 0; i < numThreads; i++)
    {
        threadList.push_back(std::thread(readAndSort, std::ref(tempFiles), std::ref(in), runSize));
    }

    std::for_each(threadList.begin(), threadList.end(), [](std::thread &th)
    {
        th.join();
    });
    in.close();
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
    struct rlimit resourceLimit;
    if(getrlimit(RLIMIT_NOFILE, &resourceLimit) != 0)
    {
        std::cerr << "Cannot get fd limit" << std::endl;
        return;
    }
    debugLog("Limit fd", std::to_string(resourceLimit.rlim_cur));

    // read the input file, create the initial runs, and assign the runs to the scratch output files
    auto tempFiles = createInitialRuns(inputFile, limitBytes, numThreads);
    debugLog("Temp file", std::to_string(tempFiles.size()));

    // Merge the runs using the K-way merging
    mergeFiles(outputFile, tempFiles, resourceLimit.rlim_cur);
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
        std::cerr << inputFile << " is empty" << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Processing..." << std::endl;
    externalSort(inputFile, outputFile, limitInBytes);
    std::cout << "Done" << std::endl;

    return EXIT_SUCCESS;
}
