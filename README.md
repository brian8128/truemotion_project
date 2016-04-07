# truemotion_project
## Assumptions
Raw data are given in the data folder.  Although they exist in text files we assume they represent much larger data sets 
stored on a hadoop cluster.  This means we have to do all of the data cleaning using distributed technology.  

## Approach
We clean the data using spark.  

## Challenges
Spark tends to assume that all lines are separate and order doesn't matter.  However in our case the order of the lines
does matter.  We overcome this challenge by assigning each line an index and then grouping like data by their indices.
