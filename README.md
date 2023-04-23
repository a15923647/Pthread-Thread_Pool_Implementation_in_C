# Pthread Thread Pool Implementation in C
To dynamically add new tasks to a running task pool, utilize pthread and semaphore.\
The purpose of the pool is to speed up the execution of divide-and-conquer algorithms by executing each subproblem in a separate thread.\
As an illustration, consider performing merge sort on a large sequence, as demonstrated in main.c, using the aforementioned pool.
