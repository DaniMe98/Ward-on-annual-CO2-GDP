# Scalability-project

Scalable and cloud programming course.

This project is based on the ward's minimum variance clustering applied on a dataset containing the emission values of co2 and gdp of the countries in the world.
## Ward's minimum variance method

Ward's clustering is an agglomerative hierarchical clustering method based on minimizations of the total within-cluster variance.

Since ward is an agglomerative clustering algorithm, we will start from a forest of $n$ *clusters* each containing a single point $p$ such as $p_i = (co2,gdp)$.

At each steps we search for all the possible merge combinations of **two clusters** avaiable in the forest. 

Once all the clusters combinations are obtained, the midpoint $(\bar{X},	\bar{Y})$ must be calculated among all the points belonging to the clusters under analysis.


$$ 	(\bar{X},	\bar{Y}) = \bigg(~ {1 \over k} {\sum_{i=1}^k x_i} ~~~~  , ~~~~  {1 \over k} {\sum_{i=1}^k y_i} ~ \bigg)$$

After obtaining the midpoint between the points belonging to a cluster, we proceed with the calculation of the variance (or squared error) between the cluster's points and the coordinates of the midpoint $(X,Y)$ calculated previously.


$$    SE = {\sum_{i=1}^k \bigg(~ (x_i-\bar{X})^2+(y_i-\bar{Y})^2\bigg)~} $$

Once all the variances of all the merged clusters has been obtained, we search for the minimum variance. This will represent the cluster $u$ that will be added to the hierarchical tree.

After we find the cluster $u$ formed by the cluster $s$ and $t$ we procede by deleting from the forest $s,t$ and by adding to $forest[n+1]$ the cluster $u$

We will repeat all these steps until there is only one cluster in the forest representing the root of the hierarchical tree.

### Number of clusters
The algorithm allows the automatic choice of the best number of clusters based on the cutting dendogram method. The process follows the theoretical approach in which we choose the vertical branch, starting from the root, with the greatest distance and draw a horizontal line. All branches that intercept that line represent clusters. 

<p align="center">
  <img width="200" height="200" src="https://online.stat.psu.edu/stat555/sites/onlinecourses.science.psu.edu.stat555/files/cluster/single_linkage_02/index.png">
</p>

<p align="center">
<em>Cutting Dendogram Method (3 clusters in this figure) </em>
</p>


