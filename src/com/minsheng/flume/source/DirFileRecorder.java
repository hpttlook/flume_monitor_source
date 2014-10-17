// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;


import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.Context;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DirFileRecorder {
  private static final Logger LOG = LoggerFactory
      .getLogger(DirFileRecorder.class);

  // id --> FileInfo, id may be either the inode in fs or the hash value of
  // file name(FileInfo.GetIdFromName)
  private Map<Integer, FileInfo> file_info_map_ = null;
  private FileMonitor file_monitor_ = null;
  private FileMapReaderWriter reader_writer_ = null;
  private FileParser file_parser_ = null;
  private boolean auto_delete_line_delimiter_ = false;

  // check and send content every 3 second
  private Long send_interval_ = 3L;
  private String meta_store_file_  = "";
  
  DirectoryMonitorSource monitor_source_ = null;
  
  private ScheduledExecutorService executor_service_ = null;
  private Runnable sender_runnable_ = null;
  private ScheduledFuture<?> sender_future_ = null;
  
  
  public DirFileRecorder(DirectoryMonitorSource source) {
    LOG.info("Init DirFileRecorder");
    file_info_map_ = new ConcurrentHashMap<Integer, FileInfo>();
    file_monitor_ = new SimpleFileMonitor();
    reader_writer_ = new FileMapReaderWriter();
    file_parser_ = new MultiLineParser();
    monitor_source_ = source;
  }

  public void Configure(Context context) {
    LOG.info("Configure DirFileRecorder.");
    file_monitor_.Configure(context);
    String meta_dir = context.getString(FlumeConstants.META_STORE_DIR,
        "./meta/");
    send_interval_ = context.getLong(FlumeConstants.FILE_SEND_INTERVAL, 3L);
    String tmp_meta_store_file = meta_dir + FlumeConstants.DIR_SEP + 
                       file_monitor_.GetMonitorDir().hashCode();
    File tmp_file = new File(tmp_meta_store_file);
    meta_store_file_ = tmp_file.getAbsolutePath(); // 
    
    auto_delete_line_delimiter_ = 
        context.getBoolean(FlumeConstants.AUTO_DELETE_LINE_DEILMITER, false);
    
    reader_writer_.Configure(meta_store_file_);
    
    file_parser_ = new MultiLineParser();
    file_parser_.Configure(context);
    
    executor_service_ = Executors.newScheduledThreadPool(1);
    sender_runnable_ = new SenderRunnable();
    file_info_map_ = new ConcurrentHashMap<Integer, FileInfo>();
  }

  public void Start() {
    LOG.info("Start DirFileRecorder.");
    reader_writer_.LoadMap(file_info_map_);
    FileMapReaderWriter.PrintMap(file_info_map_);
    file_monitor_.Start();
    sender_future_ = executor_service_.scheduleAtFixedRate(sender_runnable_,
           0L, 
           send_interval_.longValue(), 
           TimeUnit.SECONDS);
  }

  public void Stop() {
    file_monitor_.Stop();
    sender_future_.cancel(true);
    reader_writer_.WriteMap(file_info_map_);
    executor_service_.shutdown();
  }

  private boolean SendEvents(Map<Integer, FileInfo> file_map) {
    if (null == file_map || file_map.isEmpty()) {
      LOG.warn("file_map is null(wait for update) or file_map is empty");
      return false;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("SendEvents, with total file num = " + file_map.size() 
          + " dir {}"
         , file_monitor_.GetMonitorDir());
    }
    // currently we update every time for debug
    boolean should_update_meta = true;
    long event_num = 0;
    try {
      for (FileInfo file_info : file_map.values()) {
        if (file_info.get_offset() >= file_info.get_file_length()) {
          // this file already processd
          if (LOG.isDebugEnabled()) {
            LOG.debug("File done, skip: " + file_info.get_file_name());
          }
          continue;
        }

        List<String> records = file_parser_.GetNextBatchRecords(
            file_info.get_file_name(), file_info.get_offset());
        Long offset = file_info.get_offset();
        for (String record : records) {
          // no matter drop it or not ,we should first update file read offset
          byte[] record_bytes = record.getBytes();
          offset += record_bytes.length;
          /*
           * if (auto_delete_line_delimiter_) { record_bytes =
           * record.trim().getBytes(); offset += record_bytes.length + 1; // 1
           * for line delimiter } else { record_bytes = record.getBytes();
           * offset += record_bytes.length; }
           */
          // NOTICE: every record is end in with a '\n',if the flume will
          // auto to add a '\n', we may handle it here, other otherwise, switch
          // off
          // the auto-add-new-line.
          if (file_parser_.ShouldDrop(record)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Drop record: " + record);
            }
            continue;
          }
          Event event = EventBuilder.withBody(record_bytes);
          monitor_source_.getChannelProcessor().processEvent(event);
          event_num += 1;
        } // end for loop
        // update offset
        file_info.set_offset(offset);
        should_update_meta = true;
      } // end for loop
      if (LOG.isDebugEnabled()) {
        LOG.debug("Send Event Num this time: " + event_num + " for dir {}",
            file_monitor_.GetMonitorDir());
      }
      monitor_source_.UpdateSourceCounter(event_num);
    } catch (Exception e) {
      LOG.warn("Exception in SendEvents: " + e.getMessage());
      e.printStackTrace();
    }
    return should_update_meta;
  }
  
  class SenderRunnable implements Runnable {
    @Override
    public void run() {
      // TODO Auto-generated method stub
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Before Update, file_map_size: " + file_info_map_.size() +" dir {}",
              file_monitor_.GetMonitorDir());
          FileMapReaderWriter.PrintMap(file_info_map_);
        }
        Map<Integer, FileInfo> new_map =
          file_monitor_.GetLatestFileInfo(file_info_map_);
        if (LOG.isDebugEnabled()) {
          LOG.debug("After Update, file_map_size: " 
                + new_map.size() + " dir {}", file_monitor_.GetMonitorDir());
          FileMapReaderWriter.PrintMap(new_map);
        }
        
        file_info_map_ = new_map;
        if (SendEvents(file_info_map_)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Write file map for dir {}", file_monitor_.GetMonitorDir());
          }
          reader_writer_.WriteMap(new_map);
        }
        
      } catch (Exception e) {
        LOG.warn("Exception in SenderRunable: " + e.getMessage());
        e.printStackTrace();
      }
    }
  }
  
};
