flume_monitor_source
====================
# What is 
flume_monitor_source is a flume source plug-in for monitoring files under a specified directory. It is different from the 'spooldir' source in flume in the following ways:
1. The flume_monitor_source can incrementally read data from the specified directory in real time, which means the file under the specified directory is writeable (only append operation). This is not support by 'spooldir' source;
2. The flume_monitor_source can handle the multiple lines such as Java call stack  or exception as ONE understandable complete record , while the flume can only handle one line per time;
3.  The flume_monitor_source will process the file at the point which it had already processed when it was stopped at the last time.

---

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

## Prerequisites
[FlumeUserGuide]: http://flume.apache.org/FlumeUserGuide.html
   You are supposed to known how to use flume. See [Flume Documentation.][FlumeUserGuide]
   
## Parameter List
  
   **Property Name**  | **default**  | **Description** 
   :--------------- | :--------- | :---------------------------------
   *type*           |  -         | The component type name, needs to be com.minsheng.flume.source.MonitorDirectorySource
   *monitor_dir*    |  -         | Required. The directory under which all files under will be monitored. Files satisfying the condition will be parsed and send to the flume channel
   *meta_store_dir* | 
   
   

