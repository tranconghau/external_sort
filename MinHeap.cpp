#include "MinHeap.h"
#include "Utilities.h"

MinHeap::MinHeap(MinHeapNode a[], std::size_t size)
{
    heapSize = size;
    harr = a;
    long int i = (heapSize - 1) / 2;
    while(i >= 0)
    {
        MinHeapify(i);
        i--;
    }
}

MinHeap::~MinHeap()
{
    if(harr != nullptr)
    {
        delete[] harr;
        harr = nullptr;
    }
}

std::size_t MinHeap::left(std::size_t i)
{
    return (2 * i + 1);
}

std::size_t MinHeap::right(std::size_t i)
{
    return (2 * i + 2);
}

const MinHeapNode MinHeap::getMin()
{
    return harr[0];
}

void MinHeap::replaceMin(const MinHeapNode &x)
{
    if(heapSize > 0)
    {
        harr[0] = x;
        MinHeapify(0);
    }
}

const bool MinHeap::empty()
{
    return (heapSize == 0);
}

void MinHeap::deleteMin()
{
    if(heapSize > 0)
    {
        harr[0] = std::move(harr[heapSize - 1]);
        heapSize--;
        MinHeapify(0);
    }
}

// A recursive method to heapify a subtree with root at given index.
void MinHeap::MinHeapify(std::size_t i)
{
    auto l = left(i);
    auto r = right(i);
    auto smallest = i;
    if(l < heapSize && harr[l].element < harr[i].element)
        smallest = l;
    if(r < heapSize && harr[r].element < harr[smallest].element)
        smallest = r;
    if(smallest != i)
    {
        swap(&harr[i], &harr[smallest]);
        MinHeapify(smallest);
    }
}