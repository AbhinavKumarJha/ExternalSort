External Sort algorithm based on seastar framework.

Pre-requisites:
* Need to have all installations as per [https://github.com/scylladb/seastar/tree/master](https://github.com/scylladb/seastar/blob/master/README.md)
* Having a seastar clone in the working directory also works fine(along with other installations)
* input.txt file for reading records.

Commands for Running on terminal:
1) cmake in the working directory
2) make
3) ./sort --buffer-size 10000 --dir-path "/dir-path/of/input/"

EXPECTED results: 
1) output.txt file in same directory.
2) - "Output is sorted.  SUCCESS!" displayed on terminal. A small testing util confirms this after visiting output.txt.

buffer-size is the memory(number of pages) intended to be allocated for this execution per core shard.

Sample input and Sample Output have been added.
