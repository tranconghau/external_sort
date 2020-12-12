# External sort

Main idea:
- Using to sort the file which the size greater than the memory.
- Seperates the big file to many files that fit to the memory to sort them. After that merge them using the MinHeap data structure.

Available libraries:
- Using sort() from STL.
- Using MinHeap data structure.

# build

- change to repo directory, type make
```sh
$ cd sorting_algos
$ make
```

# run
```sh
$ ./ex_sort input.txt output.txt num_limit_in_bytes
```

Refererences

- https://www.geeksforgeeks.org/external-sorting/
- https://web.archive.org/web/20150202022830/http://faculty.simpson.edu/lydia.sinapova/www/cmsc250/LN250_Weiss/L17-ExternalSortEX2.htm
