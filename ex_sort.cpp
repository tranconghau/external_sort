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

int mergeSomeFiles(const std::string &out, const std::vector<std::string> &list,
                    unsigned long processed, unsigned long numFiles)
{
    auto retCode = 1;
    //Prepare IO
    auto inFiles = std::vector<std::ifstream>();
    for (auto i = 0UL; i < numFiles; i++)
    {
        auto inTemp = std::ifstream(list[processed + i]);
        if(inTemp.is_open())
            inFiles.push_back(std::move(inTemp));
        else
        {
            std::cerr << "Can't open temp file for merging" << std::endl;
            return retCode;
        }
    }
    std::remove(out.c_str());
    auto outStream = std::ofstream(out);
    if (!outStream.is_open())
    {
        std::cerr << "Can't open out temp for merging" << std::endl;
        return retCode;
    }

    //Create a min heap
    MinHeapNode *harr = new MinHeapNode[numFiles];
    auto line = std::string();
    auto i = 0UL;
    for(; i < numFiles; i++)
    {
        if(!std::getline(inFiles[i], line))
            continue;

        harr[i].element = std::move(line);
        harr[i].idx = i;
    }
    MinHeap hp(harr, i);

    //Now one by one get the minimum element from min heap and replace it with next element
    while(!hp.empty())
    {
        //Get the minimum element and store it in output file
        MinHeapNode root = hp.getMin();
        outStream.write(root.element.c_str(), root.element.length());
        outStream.write("\n", 1);

        //Find the next element that will replace current root of heap.
        //The next element belongs to same input file as the current min element.
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

    for(auto i = 0UL; i < numFiles; i++)
    {
        inFiles[i].close();
        std::remove(list[processed + i].c_str());
    }
    outStream.close();
    return 0;
}

int mergeFiles(const std::string &outputFile, std::vector<std::string> &files, unsigned long fdLimit)
{
    auto retCode = 1;
    auto numFdLimit = fdLimit - 4;//4 means 0,1,2 and fd for output
    auto listSize = files.size();
    if(listSize > numFdLimit)
    {
        do
        {
            auto numProcessed = 0UL;
            auto tempListIn = std::move(files);
            auto tmpListSize = tempListIn.size();
            while(tmpListSize > numProcessed)
            {
                auto fileName = tempDir + std::to_string(fileNameInNum++);
                auto nextProcess = tmpListSize - numProcessed;
                nextProcess = nextProcess > numFdLimit ? numFdLimit : nextProcess;
                if (mergeSomeFiles(fileName.c_str(), tempListIn, numProcessed, nextProcess) != 0)
                    return retCode;
                numProcessed += nextProcess;
                files.push_back(std::move(fileName));
            }
        }while(files.size() > numFdLimit);

        if(files.size() > 0)//final output
            retCode = mergeSomeFiles(outputFile, files, 0, files.size());
    }
    else
        retCode = mergeSomeFiles(outputFile, files, 0, files.size());
    return retCode;
}

void readAndSort(std::vector<std::string> &tempFiles, std::ifstream &in, std::size_t runSize)
{
    while(true)
    {
        //reading phase
        auto line = std::string();
        auto lineVec = std::vector<std::string>();
        {
            std::lock_guard<std::mutex> lock(readMutex);
            if (moreInput)
            {
                auto numRead = 0UL;
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

        //sorting and write to temp file
        if(lineVec.size() > 0)
        {
            std::sort(lineVec.begin(), lineVec.end());
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
    auto tempFiles = std::vector<std::string>();
    auto in = std::ifstream(inputFile);
    if (!in.is_open())
    {
        std::cerr << "Can't open input file for sorting" << std::endl;
        return tempFiles;
    }
    auto runSize = limitBytes / numThreads;
    auto threadList = std::vector<std::thread>();
    for(auto i = 0U; i < numThreads; i++)
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
int externalSort(const std::string &inputFile, const std::string &outputFile, std::size_t limitBytes)
{
    auto retCode = 1;
    auto numThreads = std::thread::hardware_concurrency();
    if(numThreads == 0)
    {
        std::cerr << "Num of threads seem invalid" << std::endl;
        return retCode;
    }

    debugLog("Using thread", std::to_string(numThreads));
    struct rlimit resourceLimit;
    if(getrlimit(RLIMIT_NOFILE, &resourceLimit) != 0)
    {
        std::cerr << "Cannot get fd limit" << std::endl;
        return retCode;
    }
    debugLog("Limit fd", std::to_string(resourceLimit.rlim_cur));

    // read the input file, create the initial runs, and assign the runs to the scratch output files
    auto tempFiles = createInitialRuns(inputFile, limitBytes, numThreads);
    debugLog("Temp file", std::to_string(tempFiles.size()));

    // Merge the runs using the K-way merging
    if(tempFiles.size() > 0)
        retCode = mergeFiles(outputFile, tempFiles, resourceLimit.rlim_cur);
    return retCode;
}

int main(int argc, const char *argv[])
{
    if(argc != 4)
    {
        std::cerr << "Using: " << argv[0] << " <intput.txt> <output.txt> <limit_in_bytes>" << std::endl;
        return EXIT_FAILURE;
    }

    auto limitInBytes = std::stol(argv[3]);
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

    debugLog("Processing", "");
    if (externalSort(inputFile, outputFile, limitInBytes) == 1)
    {
        std::cerr << "Sort failed" << std::endl;
        return EXIT_FAILURE;
    }
    debugLog("Result", "success");

    return EXIT_SUCCESS;
}
