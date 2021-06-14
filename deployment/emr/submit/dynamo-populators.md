# Data Asset:
* ~8,500,000 records
* record size --> ~0.5kb

# DDB Config:
* Provisioned WCUs --> 5,000

# Conclusion:
* EMR Parallelism too high
* Spark Job facing "Too Many Request" Errors
* Must be recursively re-trying & slowing down
* So smaller cluster performs better 

##1)
* 10 nodes, static config
* --> 1 hour 25 minutes

##2)
* 10 nodes, dynamic config
* --> 32 minutes

##3)
* 20 nodes, static config
* --> 1 hour 52 minutes

## 4)
* 20 nodes, dynamic config
* --> 37 minutes

## 5)
* 5 nodes, dynamic config
* --> 29 minutes
