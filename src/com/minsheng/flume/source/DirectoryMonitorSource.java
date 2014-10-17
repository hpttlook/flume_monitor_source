// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;

import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryMonitorSource extends AbstractSource
 implements EventDrivenSource, Configurable {
  private static Logger LOG = 
      LoggerFactory.getLogger(DirectoryMonitorSource.class);
  
  private  SourceCounter sourceCounter_;

  DirFileRecorder dir_recorder_ = null;
  public DirectoryMonitorSource() {
    dir_recorder_ = new DirFileRecorder(this);
    if (sourceCounter_ == null) {
      sourceCounter_ = new SourceCounter(getName());
    }
  }
  
  public void UpdateSourceCounter(long event_size) {
    sourceCounter_.addToEventAcceptedCount(event_size);
  }
  
  @Override
  public void configure(Context context) {
    // TODO Auto-generated method stub
    if (LOG.isDebugEnabled()) {
      for (Map.Entry<String, String> entry : 
        context.getParameters().entrySet()) {
        LOG.debug("*****key=" + entry.getKey() + " value=" + entry.getValue());
      }
    }
    dir_recorder_.Configure(context);
  }
  
  @Override
  public void start() {
    dir_recorder_.Start();
  }
  
  @Override
  public void stop() {
    dir_recorder_.Stop();
  }
}