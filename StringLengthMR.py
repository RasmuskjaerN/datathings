# This file provides a number of algorithms to obtain the longest string from a list.

# This function iterates the whole list to find the longest string.
def find_longest_string(list_of_strings):
    longest_string = None
    longest_string_len = 0 
    for s in list_of_strings:
        if len(s) > longest_string_len:
            longest_string_len = len(s)
            longest_string = s
    return longest_string


# Below are different versions of MapReduce algorithms.

from functools import reduce

# Our Mapper is intended to get the length of a string.
mapper = len

# Our Reducer is intended to compare two (string, len) pairs and return the longer one.
def reducer(pair1, pair2):
    if pair1[1] > pair2[1]:
        return pair1
    else:
        return pair2
    

# This MapReduce version works in a straightforward without splitting the data
def getLongestMR(list_of_strings):
    from functools import reduce

    #step 1
    mapped = map(mapper, list_of_strings)
    mapped = zip(list_of_strings, mapped)

    #step 2:
    reduced = reduce(reducer, mapped)
    print(reduced)
    
# This function splits a given list into a number of smaller chunks each in the size of chunk_size    
def split(list_a, chunk_size):
    for i in range(0, len(list_a), chunk_size):
        yield list_a[i : i+chunk_size]


# This MapReduce version works on chunked input data, however, it uses one Mapper and one Reducer
def getLongestMR_chunked(list_of_strings):
    data_chunks = split(list_of_strings, 30)
    
    #step 1:
    reduced_all = []
    for chunk in data_chunks:
        mapped_chunk = map(mapper, chunk)
        mapped_chunk = zip(chunk, mapped_chunk)

        reduced_chunk = reduce(reducer, mapped_chunk)
        reduced_all.append(reduced_chunk)

    #step 2:
    reduced = reduce(reducer, reduced_all)
    print(reduced)

    
# We define a new Mapper which works on each chunk. It contains a local Reducer after the original Mapper
def chunks_mapper(chunk):
    mapped_chunk = map(mapper, chunk) 
    mapped_chunk = zip(chunk, mapped_chunk)
    
    return reduce(reducer, mapped_chunk)


# This MapReduce version works on chunked input data, and it uses one Mapper and two Reducers (one in the Mapper)
def getLongestMR_chunked_2(list_of_strings):
    data_chunks = split(list_of_strings, 30)
    
    #step 1:
    mapped = map(chunks_mapper, data_chunks)
    
    #step 2:
    reduced = reduce(reducer, mapped)
    print(reduced)
    
    
# This MapReduce version works on chunked input data, uses one Mapper and two Reducers (one in the Mapper), and uses multiprocessing to simulate the multiple processors in real MapReduce
def getLongestMR_chunked_mp(list_of_strings, chunk_size, pool):
    """
    pool: A pool of worker processes.
    """
    data_chunks = split(list_of_strings, chunk_size)
    
    #step 1:
    mapped = pool.map(chunks_mapper, data_chunks)
    
    #step 2:
    reduced = reduce(reducer, mapped)
    print(reduced)