// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.lang.*;
import java.io.PrintWriter;
import junit.framework.Assert;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.apache.flume.Context;

import com.minsheng.flume.source.FileInfo;
import com.minsheng.flume.source.FlumeConstants;
import com.minsheng.flume.source.MultiLineParser;
import com.minsheng.flume.source.SimpleFileMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestDirFileRecorder {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestDirFileRecorder.class);
  
  private File target_dir_ = new File("/tmp/ms_flume/parser");
  
  private String first_regex_ = "\\[\\[.*";
  private String last_regex_ = ".*END\\]\\]";
  private Pattern start_line_pattern_ = Pattern.compile(first_regex_);
  private Pattern end_line_pattern_ = Pattern.compile(last_regex_);
  private Pattern record_include_pattern_ = 
      Pattern.compile(".*INCLUDE_RECORD.*");
  private Pattern record_exclude_pattern_ = 
      Pattern.compile(".*EXCLUDE_RECORD.*");
  
  private String record_include_str_ = "\tINCLUDE_RECORD";
  private String record_exclude_str_ = "\tEXCLUDE_RECORD";
  
  
  public void CreateMIXFile(File file) {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(file);
      String start_line = "[[INFO] 2013-01-17 13:54:32 reord first line";
      String end_line = "\trecord end line END]]";
      String mid_line = "\tmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
      
      // write record 1  ---include if include_record = .*
      writer.println(start_line);
      writer.println(end_line);
      
      // write record 1  ---include
      writer.println(start_line);
      writer.print(record_include_str_);
      writer.println(end_line);
      
   // write record 1  ---exclude
      writer.println(start_line);
      writer.print(record_exclude_str_);
      writer.println(end_line);
      
      // write record 2  -- include
      writer.println(start_line);
      writer.print(record_include_str_);
      writer.println(mid_line);
      writer.println(end_line);
      
      // write record 2  -- exclude
      writer.println(start_line);
      writer.print(record_exclude_str_);
      writer.println(mid_line);
      writer.println(end_line);

      // write random line and tailer witout header
      // we should see this as a new record
      writer.println("\tno header no header no header");
      writer.println(end_line);
      
      
      // write random line and tailer witout header
      // we should never see this, because it will wait the end or start line;
      writer.println(start_line);
      writer.println("\t no tailer no tailer no tailer");
    } catch (FileNotFoundException e) {
      LOG.error("Write content failed at file: " + file);
    } finally {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } 
  }
  
  public void CreateFIRSTFile(File file) {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(file);
      String start_line = "[[INFO] 2013-01-17 13:54:32 reord first line";
      String mid_line = "\tmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
      // write record 1
      writer.println(start_line);
      writer.print(record_include_str_);
      // write record 2
      writer.println(start_line);
      writer.print(record_exclude_str_);
      writer.println(mid_line);
      
      // write record 3
      writer.println(start_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);

    } catch (FileNotFoundException e) {
      LOG.error("Write content failed at file: " + file);
    } finally {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } 
  }
  
  public void CreateLASTFile(File file) {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(file);
      String end_line = "[[INFO] 2013-01-17 13:54:32 reord end line";
      String mid_line = "\tmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
      // write record 1
      writer.print(record_include_str_);
      writer.println(end_line);
      // write record 2
      writer.println(mid_line);
      writer.print(record_include_str_);
      writer.println(end_line);
      
      // write record 3

      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(end_line);
      
      // useless line
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.print(record_exclude_str_);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
    } catch (FileNotFoundException e) {
      LOG.error("Write content failed at file: " + file);
    } finally {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } 
  }
  
  public void CreateNONEFile(File file) {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(file);
      String end_line = "[[INFO] 2013-01-17 13:54:32 random arbitray";
      String mid_line = "\tmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
      // write record 1
      writer.println(end_line);
      writer.print(record_exclude_str_);
      writer.print(record_include_str_);
      // write record 2
      writer.println(end_line);
      writer.println(mid_line);
    } catch (FileNotFoundException e) {
      LOG.error("Write content failed at file: " + file);
    } finally {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } 
  }
  

  public void SetUp() {
   LOG.info("Start test MultiLineParser");
   LOG.info("Create test dir: " + target_dir_);
   target_dir_.mkdirs();
   Assert.assertTrue(target_dir_.exists());

  
   
   File first_file = new File(target_dir_ + "/" + "first.file");
   CreateFIRSTFile(first_file);
   
   File last_file = new File(target_dir_ + "/" + "last.file");
   CreateLASTFile(last_file);
   
   File none_file = new File(target_dir_ + "/" + "none.file");
   CreateNONEFile(none_file);
    
  }
  
  public void WriteFile(File file, List<String> records) {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(file);
      for (String line : records) {
        writer.print(line);
      }
    } catch (FileNotFoundException e) {
      LOG.error("Write content failed at file: " + file);
    } finally {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } 
  }
  

  @Test
  public void TestDirFile() {
    LOG.info("Start test dir file recorder");
  }
}