# etcd2-aws-cluster

A self-contained tool to assist in managing a etcd2 cluster from an Amazon auto scaling group

Thanks to the project [etcd-aws-cluster](https://github.com/MonsantoCo/etcd-aws-cluster) 

# The Story

 - CoreOS removed early-docker since [1284.0.0 (Jan 5, 2017)](https://github.com/coreos/bugs/issues/1429) 
 - Using docker to start etcd2 cluster for kubernetes doesn't work anymore.
 - I need to run the script without dependencies.
 - I choose Rust this time!

# The logic
- The logic are nearly the same as [etcd-aws-cluster](https://github.com/MonsantoCo/etcd-aws-cluster) 
- I change the logic of bad member removal 
