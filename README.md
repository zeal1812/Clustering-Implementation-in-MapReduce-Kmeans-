# Clustering-Implementation-in-MapReduce-Kmeans-
Given the input file as datapoints.txt.(input.txt)
Given 10 K clusters as the centers in centers.txt. 
And get output in an output folder (outputk2 in single iteration).
Input and Output of Mapper and Reducer:
Mapper:  Mapper has k centers in memory.
  Input : Key-value pair (each input data point x). 
  Find the index of the closest of the k centers (call it iClosest).
  Emit: (key, value) = (iClosest, x) 
Reducer(s):
  Input (key, value)  
  Key = index of center  Value = iterator over input data points closest to ith center
  Emit: (key, value) = (index of center, new center)       
