# Discussion #
# Sort-merge join
This implementation works by decomposing the n-ary join into a series of binary joins, which are then executed in a sorted manner. 

In the first step the left table is treated as the result table and the right table is joined to it forming a new result table. 
This result talble is then repeatedly joined with the next right table  and updated until all tables have been joined.

This implementation has a complexity of `O(n * log(n))` for the sorting step for each table which is the most expensive part of the algorithm. 

The total complexity is the sum for each table - for tables `T1, T2, ..., Tn` the complexity is `O(T1 * log(T1) + T2 * log(T)) + ... + Tn * log(Tn))`.

# Hash-join
This implementation works by creating a hash table for each table, except the last table, and then probes backwards to come at a solution.

Due to the fact that keys are not unique, we will store collected results inside of a indexes object for each probed row, meaning we can handle the cross join effect.

This implementation will scale well, as main cost and complexity comes with the size of the final, probing table. However, this does come with a steep memory cost for each built hash table.

For many applications, we will have a properly normalised dataset, meaning that out last relational table, i.e our probing table has the potential to be very small, depending on the use case.

Our join algorithm will stop searching on a given probe row if any of the backward relations do not hold, causing minimal wasted computation.

However, the steep memory cost will cause many applications to not be appropriate. A solution could be to reduce the algorithm to only one hash table.

One note - a hybrid solution may be very beneficial, as we can then start to store a hash counter, which we can increment, even allowing for multithreading. This can both mean a reduced number of non-contiguous memory accesses, and thus page faults, increasing performance. Lastly, if we have a sorted dataset by hash, then we can delegate the building process, in order to free up memory during execution of the join.

