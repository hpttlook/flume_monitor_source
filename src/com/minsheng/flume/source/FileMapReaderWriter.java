// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;

import java.util.Map;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FileMapReaderWriter {
  private static final Logger LOG = LoggerFactory
      .getLogger(FileMapReaderWriter.class);
  private static String format_string_ = "# ${inode}\t${length}\t{$offset}\t${file_name}";    
  private String file_name_;
   
  public FileMapReaderWriter() {
    file_name_ = null;
  }
  
  public void Configure(String name) {
    Preconditions.checkState(null != name && !name.isEmpty(),
        "Map meta record file must be specified");
    file_name_ = name;
    LOG.info("map_record_meta_file=" + file_name_);
  }
  
  public boolean ParseLine(String line, FileInfo file_info) {
    Preconditions.checkState(file_name_ != null, 
        "Plz call Configure to initialize before call other functions");
    String eles[] = line.split(FileInfo.FIELD_SEPERATOR);
    if (eles.length != FileInfo.NUM_FIELDS) {
      LOG.warn("Invalid record line:" + line);
      return false;
    }
    try {
    file_info.set_file_name(eles[FileInfo.FILE_NAME_INDEX]);
    file_info.set_file_length((Long.valueOf(eles[FileInfo.FILE_LENGTH_INDEX])));
    file_info.set_offset((Long.valueOf(eles[FileInfo.OFFSET_INDEX])));
    file_info.set_id((Integer.valueOf(eles[FileInfo.ID_INDEX])));
    } catch (NumberFormatException e) {
      LOG.warn("Invalid line:" + line);
      return false;
    }
    return true;
  }
  
  public static void PrintMap(Map<Integer, FileInfo> file_info_map) {
    int cnter = 0;
    LOG.debug("Total num file in file_map:" + file_info_map.size());
    for (FileInfo file_info : file_info_map.values()) {
      LOG.debug("idx = " + cnter + " info = " + file_info.toString());
    }
  }
  
  public synchronized void LoadMap(Map<Integer, FileInfo> file_info_map) {
    LOG.info("LoadFileMap from file: " + file_name_);
    BufferedReader file_reader = null;
    try {
      file_reader = new BufferedReader(new InputStreamReader(
          new FileInputStream(file_name_)));
      String line = null;
      while ((line = file_reader.readLine()) != null) {
        if (line.startsWith("#")) {
          continue;
        }
        FileInfo file_info = new FileInfo();
        if (ParseLine(line, file_info)) {
          file_info_map.put(file_info.get_id(), file_info);
        } else {
          LOG.warn("LoadMap invalid line, parse error: " + line);
        }
      }
      PrintMap(file_info_map);
    } catch(FileNotFoundException e) {
      LOG.info("Map record file not exist, skip loading " + file_name_);
    } catch (Exception e) {
      LOG.warn("Map record file read error due to " + e.toString());
    } finally {
      if (null != file_reader) {
        try {
          file_reader.close();
        } catch (IOException e) {
          LOG.warn("close file exception, " + e.toString());
        }
      }
    }
  }
  
  public synchronized void WriteMap(Map<Integer, FileInfo> file_info_map) {
    PrintWriter file_writter = null;
    int cnter = 0;
    try {
      File file = new File(file_name_);
      // create parent directory if not exist
      file.getParentFile().mkdirs();
      file_writter = new PrintWriter(file_name_);
	  file_writter.println(format_string_);
      synchronized (file_info_map) {
        for (FileInfo file_info : file_info_map.values()) {
          file_writter.println(file_info.GetWriteString());
          cnter++;
        }
      }
    } catch (FileNotFoundException e) {
      LOG.warn("File not found, you should never see this");
    } catch (Exception e) {
      LOG.warn("Write map meta failed due to " + e.toString());
    } finally {
      if (null != file_writter) {
        file_writter.close();
      }
      LOG.info("Write \'" + cnter + "\' records to map meta file" + file_name_ );
    }
  }
  
}