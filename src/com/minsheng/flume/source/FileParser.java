// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;
import java.util.List;
import org.apache.flume.Context;

public abstract class FileParser {
  public abstract void Configure(Context context);
  public abstract List<String> GetNextBatchRecords(String file_name,
                                                   Long offset);
  public abstract boolean ShouldDrop(String record);
}