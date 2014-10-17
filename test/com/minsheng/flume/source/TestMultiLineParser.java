// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;
import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.ArrayList;
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


public class TestMultiLineParser {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestMultiLineParser.class);
  
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
      // writer.println("\tno header no header no header");
      // writer.println(end_line);
      
      
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
      String end_line = "record end line END]]";
      String mid_line = "\tmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
      // write record 1
      writer.print(record_include_str_);
      writer.println();
      writer.println(end_line);
      // write record 2
      writer.println(mid_line);
      writer.print(record_include_str_);
      writer.println(end_line);
      
      // write record 3

      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println();
      writer.println(end_line);
      
      // useless line
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println();
      writer.println();
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
      writer.println();
      writer.print(record_include_str_);
      // write record 2
      writer.println(end_line);
      writer.println(mid_line);
      writer.println();
    } catch (FileNotFoundException e) {
      LOG.error("Write content failed at file: " + file);
    } finally {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } 
  }
  
  public void CreateFIRSTBigRecordFile(File file) {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(file);
      String start_line = "[[INFO] 2013-01-17 13:54:32 reord first line";
      String mid_line = "\tmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm";
      // write record 1
      writer.println(start_line);
      writer.println();
      writer.println(mid_line);
      writer.println();
      // write record 3
      writer.println(start_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println(mid_line);
      writer.println();

    } catch (FileNotFoundException e) {
      LOG.error("Write content failed at file: " + file);
    } finally {
      if (writer != null) {
        writer.close();
        writer = null;
      }
    } 
  }
  
  @Before 
  public void SetUp() {
   LOG.info("Start test MultiLineParser");
   LOG.info("Create test dir: " + target_dir_);
   target_dir_.mkdirs();
   Assert.assertTrue(target_dir_.exists());
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
  public void TestMixParse() {
    LOG.info("Start test mix file mode");
    Map<String, String> params = new HashMap<String, String>();
    params.put(FlumeConstants.FILE_CONTENT_INCLUDE, ".*INCLUDE_RECORD.");
    params.put(FlumeConstants.FILE_CONTENT_EXCLUDE, ".*EXCLUDE_RECORD.");
    params.put(FlumeConstants.FIRST_LINE_PATTERN, first_regex_);
    params.put(FlumeConstants.LAST_LINE_PATTERN, last_regex_);
    Context context = new Context(params);
    
    File mix_file = new File(target_dir_ + "/" + "mix.file");
    File mix_file_handled = new File(target_dir_ + "/" + "mix.file.hd");
    CreateMIXFile(mix_file);
    
    MultiLineParser parser = new MultiLineParser();
    parser.Configure(context);
    List<String> records = parser.GetNextBatchRecords(mix_file.toString(), 0L);
    int cnter = 0;
    int byte_length = 0;
    for (String record : records) {
      cnter++;
      LOG.info("the \'" + cnter + "\' record=" + record);
      byte_length += record.getBytes().length;
    }
    WriteFile(mix_file_handled, records);
    LOG.info("***********Summary Info**************");
    LOG.info("File: " + mix_file);
    LOG.info("Processed Record Num:" + cnter);
    LOG.info("File Length: " + mix_file.length());
    LOG.info("Process Bytes:" + byte_length);
    LOG.info("*************************************");
  }
  
  @Test
  public void TestFirstParse() {
    LOG.info("Start test first file mode");
    Map<String, String> params = new HashMap<String, String>();
    params.put(FlumeConstants.FILE_CONTENT_INCLUDE, ".*INCLUDE_RECORD.");
    params.put(FlumeConstants.FILE_CONTENT_EXCLUDE, ".*EXCLUDE_RECORD.");
    params.put(FlumeConstants.FIRST_LINE_PATTERN, first_regex_);
    Context context = new Context(params);
    
    File first_file = new File(target_dir_ + "/" + "first.file");
    File first_file_handled = new File(target_dir_ + "/" + "first.file.hd");
    CreateFIRSTFile(first_file);
    
    MultiLineParser parser = new MultiLineParser();
    parser.Configure(context);
    List<String> records = parser.GetNextBatchRecords(first_file.toString(), 0L);
    int cnter = 0;
    int byte_length = 0;
    for (String record : records) {
      cnter++;
      LOG.info("the \'" + cnter + "\' record=" + record);
      byte_length += record.getBytes().length;
    }
    WriteFile(first_file_handled, records);
    LOG.info("***********Summary Info**************");
    LOG.info("File: " + first_file);
    LOG.info("Processed Record Num:" + cnter);
    LOG.info("File Length: " + first_file.length());
    LOG.info("Process Bytes:" + byte_length);
    LOG.info("*************************************");
  }
  
  @Test
  public void TestLastParse() {
    LOG.info("Start test last file mode");
    Map<String, String> params = new HashMap<String, String>();
    params.put(FlumeConstants.FILE_CONTENT_EXCLUDE, ".*EXCLUDE_RECORD.");
    params.put(FlumeConstants.FIRST_LINE_PATTERN, first_regex_);
    Context context = new Context(params);
    
    File last_file = new File(target_dir_ + "/" + "last.file");
    File last_file_handled = new File(target_dir_ + "/" + "last.file.hd");
    CreateLASTFile(last_file);
    
    MultiLineParser parser = new MultiLineParser();
    parser.Configure(context);
    List<String> records = parser.GetNextBatchRecords(last_file.toString(), 0L);
    int cnter = 0;
    int byte_length = 0;
    for (String record : records) {
      cnter++;
      LOG.info("the \'" + cnter + "\' record=" + record);
      byte_length += record.getBytes().length;
    }
    WriteFile(last_file_handled, records);
    LOG.info("***********Summary Info**************");
    LOG.info("File: " + last_file);
    LOG.info("Processed Record Num:" + cnter);
    LOG.info("File Length: " + last_file.length());
    LOG.info("Process Bytes:" + byte_length);
    LOG.info("*************************************");
  }
  

  @Test
  public void TestNoneParse() {
    LOG.info("Start test none  mode");
    Map<String, String> params = new HashMap<String, String>();
    params.put(FlumeConstants.FILE_CONTENT_INCLUDE, ".*INCLUDE_RECORD.");
    params.put(FlumeConstants.FILE_CONTENT_EXCLUDE, ".*EXCLUDE_RECORD.");
    params.put(FlumeConstants.FIRST_LINE_PATTERN, first_regex_);
    Context context = new Context(params);
    
    File none_file = new File(target_dir_ + "/" + "none.file");
    File none_file_handled = new File(target_dir_ + "/" + "none.file.hd");
    CreateNONEFile(none_file);
    
    MultiLineParser parser = new MultiLineParser();
    parser.Configure(context);
    List<String> records = parser.GetNextBatchRecords(none_file.toString(), 0L);
    int cnter = 0;
    int byte_length = 0;
    for (String record : records) {
      cnter++;
      LOG.info("the \'" + cnter + "\' record=" + record);
      byte_length += record.getBytes().length;
    }
    WriteFile(none_file_handled, records);
    LOG.info("***********Summary Info**************");
    LOG.info("File: " + none_file);
    LOG.info("Processed Record Num:" + cnter);
    LOG.info("File Length: " + none_file.length());
    LOG.info("Process Bytes:" + byte_length);
    LOG.info("*************************************");
  }
  
  @Test
  public void TestFirstBigLine() {
    LOG.info("Start test first file mode with very small buffersize");
    Map<String, String> params = new HashMap<String, String>();
    params.put(FlumeConstants.FILE_CONTENT_INCLUDE, ".*INCLUDE_RECORD.");
    params.put(FlumeConstants.FILE_CONTENT_EXCLUDE, ".*EXCLUDE_RECORD.");
    params.put(FlumeConstants.FIRST_LINE_PATTERN, first_regex_);
    params.put("read_buffer_size", "10");
    params.put("max_read_buffer_size", "200");
    params.put("max_record_size", "100");
    Context context = new Context(params);
    
    File first_file = new File(target_dir_ + "/" + "first.bigrecord.file");
    File first_file_handled = new File(target_dir_ + "/" + 
                                        "first.bigrecord.file.hd");
    CreateFIRSTBigRecordFile(first_file);
    
    MultiLineParser parser = new MultiLineParser();
    parser.Configure(context);
    int cnter = 0;
    int max_round = 20;
    Long offset = 0L;
    
    List<String> total_records = new ArrayList<String>();
    while (offset < first_file.length() && cnter++ < max_round) {
      List<String> records = parser.GetNextBatchRecords(first_file.toString(),
          offset);
      int record_cnter = 0;
      int byte_length = 0;
      for (String record : records) {
        record_cnter++;
        LOG.info("the \'" + record_cnter + "\' record=" + record);
        byte_length += record.getBytes().length;
      }
      offset += byte_length;
      total_records.addAll(records);
    }
    WriteFile(first_file_handled, total_records);  
    
    LOG.info("***********Summary Info**************");
    LOG.info("File: " + first_file);
    LOG.info("Processed Record Num:" + cnter);
    LOG.info("File Length: " + first_file.length());
    LOG.info("Process Bytes:" + offset);
    LOG.info("*************************************");
  }
}