// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.lang.*;

import junit.framework.Assert;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.apache.flume.Context;

import com.minsheng.flume.source.FileInfo;
import com.minsheng.flume.source.FlumeConstants;
import com.minsheng.flume.source.SimpleFileMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSimpleFileMonitor {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestSimpleFileMonitor.class);
 
  private int file_num_ = 3;
  private String target_dir_;
  
  private String prefix_ = "file";
  private String include_suffix_ = ".test";
  private String include_file_regex_ = prefix_ + ".*" + include_suffix_;
  
  private String date_str_ = ".2013-01-17-12";
  private String exclude_suffix_ = include_suffix_ + date_str_;
  private String exclude_file_regex_ = prefix_ + ".*" + exclude_suffix_;
  
  private Pattern default_include_pattern_ = 
      Pattern.compile(".*");
  
  private Pattern full_exclude_pattern_ =
      Pattern.compile(exclude_file_regex_);
  
  Context default_context_;
  Context full_context_;
  
  SimpleFileMonitor default_monitor_;
  SimpleFileMonitor full_monitor_;
  
  
  @Before
  public void SetUp() {
    target_dir_ = "/tmp/ms_flume/monitor/";
    File file_target_dir = new File(target_dir_);
    LOG.info("Create test target directory: " + target_dir_);
    Assert.assertTrue("Create target dir failed", file_target_dir.mkdirs());
    
    LOG.info("Create child directory");
    for (int i = 0; i < file_num_; i++) {
      File child_dir = new File(target_dir_ +"/" + i);
      LOG.info("\tCreate  directory:" + child_dir.toString());
      Assert.assertTrue("Create child dir failed", child_dir.mkdirs());
    }
    
    try {
      LOG.info("Create include file in target directory");
      for (int i = 0; i < file_num_; i++) {
        File test_file = File.createTempFile(prefix_ + i, 
            include_suffix_, file_target_dir);
        LOG.info("\tCreate  include file:" + test_file.toString());
        Assert.assertTrue("Create failed for test file " + test_file.getName(),
            test_file.exists());
      }

      System.out
          .println("Create exclude file(start with '.') in target directory");
      for (int i = 0; i < file_num_; i++) {
        File test_file = File
            .createTempFile("." + prefix_ + i, include_suffix_, file_target_dir);
        LOG.info("\tCreate  hidden file:" + test_file.toString());
        Assert.assertTrue("Create failed for test file " + test_file.getName(),
            test_file.exists());
      }

      System.out
          .println("Create exclude file(with date suffix) in target directory");
      for (int i = 0; i < file_num_; i++) {
        File test_file = File.createTempFile(prefix_ + i,
            exclude_suffix_, file_target_dir);
        LOG.info("\tCreate  exclude file:" + test_file.toString());
        Assert.assertTrue("Create failed for test file " + test_file.getName(),
            test_file.exists());
      }
    } catch (IOException e) {
      LOG.info("IOException: " + e.getMessage());
    }
    
    LOG.info("\n*****Create default flume context(only specify target dir)");
    Map<String, String> params = new HashMap<String, String>();
    params.put(FlumeConstants.MONITOR_DIR, target_dir_);
    default_context_ = new Context(params);
    
    for (Map.Entry<String, String> s : params.entrySet()) {
      LOG.info("key=" + s.getKey()
          + " value=" + s.getValue());
    }
    
    LOG.info("\n*****Create full flume context");
    params.put(FlumeConstants.FILE_CHECK_INTERVAL, "3");
    params.put(FlumeConstants.FILE_NAME_INCLUDE, include_file_regex_);
    params.put(FlumeConstants.FILE_NAME_EXCLUDE, exclude_file_regex_);

    for (Map.Entry<String, String> s : params.entrySet()) {
      LOG.info("key=" + s.getKey()
          + " value=" + s.getValue());
    }
    full_context_ = new Context(params);
  }

  @Test
  public void TestDefaultMonitor() {
    LOG.info("Start test default action at" + target_dir_);
    default_monitor_ = new SimpleFileMonitor();
    default_monitor_.Configure(default_context_);
    default_monitor_.Start();
    
    try {
      LOG.info("Slepp 5 sec for monitor to update");
      Thread.sleep(5000L);
    } catch (InterruptedException e) {
      LOG.info("Sleep interrupted.");
    }
    
    Map<Integer, FileInfo> my_map = new ConcurrentHashMap<Integer, FileInfo>();
    Map<Integer, FileInfo> new_map = 
        default_monitor_.GetLatestFileInfo(my_map);
    LOG.info("Total valid file num: " + new_map.size());
    Assert.assertTrue(new_map.size() == (file_num_ * 2));
    for (FileInfo file_info : new_map.values()) {
      LOG.info(file_info.toString());
      Assert.assertTrue("include ilega files", 
          default_include_pattern_.matcher(file_info.get_file_name()).matches());
    }
    default_monitor_.Stop();
  }
  
  @Test
  public void TestFullMonitor() {
    LOG.info("Start test default action at" + target_dir_);
    full_monitor_ = new SimpleFileMonitor();
    full_monitor_.Configure(full_context_);
    full_monitor_.Start();
    
    try {
      LOG.info("Slepp 5 sec for monitor to update");
      Thread.sleep(5000L);
    } catch (InterruptedException e) {
      LOG.info("Sleep interrupted.");
    }
    
    Map<Integer, FileInfo> my_map = new ConcurrentHashMap<Integer, FileInfo>();
    Map<Integer, FileInfo> new_map = 
        full_monitor_.GetLatestFileInfo(my_map);
    LOG.info("Total valid file num: " + new_map.size());
    Assert.assertTrue(new_map.size() == (file_num_));
    for (FileInfo file_info : new_map.values()) {
      LOG.info(file_info.toString());
      Assert.assertTrue("include files not in include pattern",
              default_include_pattern_.matcher(file_info.get_file_name())
                  .matches());
      Assert.assertFalse("some file should be excludesd", full_exclude_pattern_
          .matcher(file_info.get_file_name()).matches());
    }
    full_monitor_.Stop();
  }
  
  public void RecursiveDelete(File file) {
    if (file.isDirectory()) {
      for (File f : file.listFiles()) {
        RecursiveDelete(f);
      }
      file.delete();
    } else {
      file.delete();
    }
  }

  @After
  public void CleanUp() {
    File file = new File(target_dir_);
    LOG.info("Clean target dir");
    RecursiveDelete(file);
    Assert.assertFalse(file.exists());
  }
}
