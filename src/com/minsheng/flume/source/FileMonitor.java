// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flume.Context;

public abstract class FileMonitor {
  public FileMonitor() {
  }
  
  public abstract void Configure(Context context);
  
  public abstract void Start();
  
  public abstract void Stop();
  
  public abstract Map<Integer, FileInfo> GetLatestFileInfo(
      Map<Integer, FileInfo> file_map_with_latest_offet);
  
  public abstract String GetMonitorDir();
}