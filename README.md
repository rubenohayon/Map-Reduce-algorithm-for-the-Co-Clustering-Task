# Map-Reduce-algorithm-for-the-Co-Clustering-Task

Given a ratings dataset, the algorithm generates a codebook "B" - a KxL matrix specifying the common ratings for each user-item group. 
In addition, the algorithm returns cluster assignments for each user and item: 
U is a vector specifying for each user her associated user-cluster index (ranges from 0 to K-1);
V - a symmetric vector, mapping each item to item-cluster index (0 to L-1).

The CodeBook is generated via the general co-clustering approach suggested in [1] by solving the optimization problem.
This optimization problem aims to minimize the error between the actual rating in the training dataset (ST) and the relevant codebook pattern.

[1]	Papadimitriou, Spiros, and Jimeng Sun. "Disco: Distributed co-clustering with map-reduce: A case study towards petabyte-scale end-to-end mining." Data Mining, 2008. ICDM'08. Eighth IEEE International Conference on. IEEE, 2008.‏
