// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;

public class FileInfo {
  public static String FIELD_SEPERATOR = "\t";
  public static int NUM_FIELDS = 4;
  public static int ID_INDEX = 0;
  public static int FILE_LENGTH_INDEX = 1;
  public static int OFFSET_INDEX = 2;
  public static int FILE_NAME_INDEX = 3;
  public static int LIFE_SPAN = 200;
  
  private String file_name_ = null;
  private Long file_length_ ;
  private Long offset_;  
  // file_tag is the unique-identifier of a file,it may be the inode
  // in linux sysmte or the hash value the absolute file name;
  private Integer id_;
  
  // when file under monitor directory is deleted, keep delete_delay_round
  // then delete this meta
  private int life_span_ = LIFE_SPAN;
  
  public static Integer GetIdFromName(String file_name) {
    return new Integer(file_name.hashCode());
  }
  
  public FileInfo() {
    file_name_ = "";
    file_length_ = 0L;
    offset_ = 0L;
    id_ = 0;
  }
  
  public FileInfo(String name, Long length, Long offset) {
    this.file_name_ = name;
    this.file_length_ = length;
    this.offset_ = offset;
    this.id_ = GetIdFromName(file_name_);
  }
  
  public FileInfo(String name, Long length, Long offset, Integer id) {
    this.file_name_ = name;
    this.file_length_ = length;
    this.offset_ = offset;
    this.id_ = id;
  }
  
  public void DecLifeSpan() {
    this.life_span_ -= 1;
  }
  
  public int get_life_span() {
    return this.life_span_;
  }
  
  public String get_file_name() {
    return this.file_name_;
  }
  
  public Long get_file_length() {
    return this.file_length_;
  }
  
  public Long get_offset() {
    return offset_;
  }
  
  public Integer get_id() {
    return id_;
  }
  
  public void set_file_name(String name) {
    file_name_ = name;
  }
  
  public void set_file_length(Long len) {
    file_length_ = len;
  }
  
  public void set_offset(Long offset) {
    offset_ = offset;
  }
  
  public void set_id(Integer id) {
    id_ = id;
  }
  
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("inode=");
    builder.append(id_);
    builder.append(FIELD_SEPERATOR);
    builder.append("length=");
    builder.append(file_length_);
    builder.append(FIELD_SEPERATOR);
    builder.append("offset=");
    builder.append(offset_);
    builder.append(FIELD_SEPERATOR);
    builder.append("file_name=");
    builder.append(file_name_);
    return builder.toString();
  }
  
  
  public String GetWriteString() {
    StringBuilder builder = new StringBuilder();
    builder.append(id_);
    builder.append(FIELD_SEPERATOR);
    builder.append(file_length_);
    builder.append(FIELD_SEPERATOR);
    builder.append(offset_);
    builder.append(FIELD_SEPERATOR);
    builder.append(file_name_);
    return builder.toString();
  }
  
  public int hashCode() {
    return id_;
  }
}