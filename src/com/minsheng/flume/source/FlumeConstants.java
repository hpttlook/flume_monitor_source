// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;

public class FlumeConstants {
  public static String DIR_SEP = "/";
  public static String LINE_SEP = "\n";
  // \s means any space character('\t','\n',' ', '\f', '\r')
  public static String AUTO_DELETE_LINE_DEILMITER = "delete_line_delimiter";
  public static String SHELL_RESULT_REGEX = "\\s+";
  public static String INTEGER_REGEX = "[0-9]+";
  public static int SHELL_RESULT_FIELD_NUM = 6;
  // records > 10MB  are cut into pieces
  public static int MAX_RECORD_LENGH = 1024 * 1024 * 10;
  public static int READ_BUFFER_SIZE = 1024 * 1024 * 1; // 2MB
  // we assume all records is smaller than 20MB, if we meet such a record
  // the program just skip to process
  public static int MAX_READ_BUFFER_SIZE = 1024 * 1024 * 20;  
  public static String FILE_CHECK_INTERVAL = "file_check_interval_sec";
  public static String FILE_SEND_INTERVAL = "file_send_interval_sec";
  public static String FILE_NAME_INCLUDE = "file_name_include_pattern";
  public static String FILE_NAME_EXCLUDE = "file_name_exclude_pattern";
  public static String FIRST_LINE_PATTERN = "first_line_pattern";
  public static String LAST_LINE_PATTERN = "last_line_pattern";
  public static String FILE_CONTENT_INCLUDE = "file_content_include_pattern";
  public static String FILE_CONTENT_EXCLUDE = "file_content_exclude_pattern";
  public static String META_STORE_DIR = "meta_store_dir";
  public static String MONITOR_DIR = "monitor_dir";
  public static String SHELL_COMMAND[] = {"ls", "-il", "-o", "-g", 
                                          "--time-style=+%m", "TARGET_DIR"};
  static String[] GetShellCommand(String monitor_dir) {
    SHELL_COMMAND[SHELL_COMMAND.length - 1] = monitor_dir;
    return SHELL_COMMAND.clone();
  }
}