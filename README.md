flume_monitor_source
====================
# What is 
flume_monitor_source is a flume source plugin for monitoring files under a specified directory. It is different from the 'spooldir' source in flume, which the flume_monitor_source can incrementally read data from the specified directory in real time--that means the the file under the specified directory is writable(only append opertaion).

# How to use it

* Build the jar

```
   ant jar
```

* Copy jar to lib of flume

```
   cp dist/flume-monitor-source-0.1.jar ${FLUME_HOME}/lib
```

# Configure the source
  

