# Ward on annual CO2-GDP

Project work for the **Scalable and Cloud Programming** course at Unibo (Master's Degree in Computer Science), made by Daniele Polidori, Giovanni Pietrucci and Danilo Meleleo.

In this project we applied the **Ward's minimum variance clustering method** on a dataset containing the emission values of CO2 and GDP of almost all the countries in the world from 1965 to 2018.

It has been developed using the **Scala+Spark** framework, which allowed us to obtain a scalable and portable application, that can run on different volume of data and on different machine within a cluster in a **parallel** and **distributed** way.

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

## Google Cloud Platform Setup

#### 1. Create new Project

#### 2. Create a Cloud Storage Bucket

#### 3. Create a Dataproc Cluster
To create a Dataproc cluster on the command line, run the Cloud SDK gcloud dataproc clusters create command locally in a terminal window or in Cloud Shell.
```
$ gcloud dataproc clusters create <cluster-name> \
    --region=<region> \
    --zone $ZONE \
    --master-machine-type $MASTER_MACHINE_TYPE \
    --num-workers $NUM_WORKERS \
    --worker-machine-type $WORKER_MACHINE_TYPE
```


#### 4. Write and compile Scala code locally 

#### 5. Create a jar with SBT
  First download SBT at https://www.scala-sbt.org/.

  From the root directory of the project, launch the command ```$ sbt package```. This will package your project as a JAR file, located in the ```target/scala-2.12/``` directory.

#### 6. Copy the jar to a Cloud Storage bucket in your project
You can use the gsutil command
```
$ gsutil cp <ProjectName>.jar gs://<bucket-name>/
```
or upload it manually from the Google Cloud Console.

#### 7. Submit jar to a Dataproc Spark job
Select the cluster's name from the cluster list, the Job type (Spark) and main class or jar specifying the Cloud Storage path to your jar (```gs://<bucket-name>/<ProjectName>.jar```). Or you can use the gcloud command:
```
$ gcloud dataproc jobs submit spark --jar=gs://<bucket-name>/<ProjectName>.jar \
    --region=<region> \
    --cluster=<cluster-name>
```

#### 8. Copy output files created
To copy the output files created, by the job, in the bucket, you can use the gsutil command:
```
$ gsutil -m cp -r gs://<bucket-name>/output/ <destination-directory>
```

Use the ```-r``` option to copy an entire directory tree. If you have a large number of files to transfer, you can perform a parallel multi-threaded/multi-processing copy using the top-level gsutil ```-m``` option.

#### 9. Shutdown your cluster
To avoid ongoing charges, shutdown your cluster and delete the Cloud Storage resources (Cloud Storage bucket and files) used.

To shutdown a cluster:
```
$ gcloud dataproc clusters delete <cluster-name> \
    --region=<region>
```
To delete a bucket and all of its folders and files:
```
$ gsutil rm -r gs://<bucket-name>/
```
