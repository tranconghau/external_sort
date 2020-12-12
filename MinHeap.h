#pragma once
#include <string>
struct MinHeapNode
{
    std::string element;
    std::size_t idx = 0;
};

// A class for Min Heap
class MinHeap
{
    MinHeapNode *harr = nullptr;
    std::size_t heapSize;

public:
    // Constructor: creates a min heap of given size
    MinHeap(MinHeapNode a[], std::size_t size);

    // To heapify a subtree with root at given index
    void MinHeapify(std::size_t);

    // Get index of left child of node at index i
    size_t left(std::size_t i);

    // Get index of right child of node at index i
    std::size_t right(std::size_t i);

    // Get the root
    MinHeapNode getMin();

    // Replace root with new node x and heapify() new root
    void replaceMin(MinHeapNode x);

    //Delete root and heapify() new root
    void deleteMin();

    bool empty();

    ~MinHeap();
};