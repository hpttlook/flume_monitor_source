// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.minsheng.util.Shell;
import com.minsheng.util.StringUtil;

public class SimpleFileMonitor extends FileMonitor {
  private static final Logger LOG = LoggerFactory
      .getLogger(SimpleFileMonitor.class);
  private static int ID_SHELL_IDX = 0;
  private static int FILE_NAME_SHELL_IDX = 5;
  private static int FILE_LENGTH_SHELL_IDX = 3;
  private static int FILE_META_IDX = 1;

  private String default_file_name_include_str_ = ".*";
  private String default_file_name_exclude_str = "^[.].*";
  private String file_name_file_name_include_pattern_str_ = null;
  // file start with "." is excluded
  private String file_name_file_name_exclude_pattern_str_ = null;
  private Pattern file_name_include_pattern_ = null;
  private Pattern file_name_exclude_pattern_ = null;

  private String ls_output_total_ = "total";
  private Pattern integer_pattern_ = null;

  private Pattern shell_result_pattern_ = null;
  private String monitor_dir_ = null;
  private Map<Integer, FileInfo> file_info_map_ = new ConcurrentHashMap<Integer, FileInfo>();
  private String shell_command_[] = null;
  private Shell.ShellCommandExecutor shell_executor_ = null;
  private ScheduledExecutorService executor_service_ = null;
  private Runnable shell_runnable_ = null;
  private ScheduledFuture<?> shell_future_ = null;
  private Long check_interval_sec_ = null;
  // 
  private Long default_interval_sec = 5L;
  private String shell_output_ = null;
  
  private boolean can_fetch_file_map_ = false;

  public SimpleFileMonitor() {
    super();
  }

  @Override
  public void Configure(Context context) {
    // TODO Auto-generated method stub
    monitor_dir_ = context.getString(FlumeConstants.MONITOR_DIR);
    Preconditions.checkState(monitor_dir_ != null,
        "you must specified  \'monitor_dir\' in config file");
    check_interval_sec_ = context.getLong(FlumeConstants.FILE_CHECK_INTERVAL,
        default_interval_sec);
    file_name_file_name_include_pattern_str_ = context.getString(
        FlumeConstants.FILE_NAME_INCLUDE, default_file_name_include_str_);
    file_name_file_name_exclude_pattern_str_ = context.getString(
        FlumeConstants.FILE_NAME_EXCLUDE, default_file_name_exclude_str);

    shell_result_pattern_ = Pattern.compile(FlumeConstants.SHELL_RESULT_REGEX);
    file_name_include_pattern_ = Pattern
        .compile(file_name_file_name_include_pattern_str_);
    file_name_exclude_pattern_ = Pattern
        .compile(file_name_file_name_exclude_pattern_str_);

    integer_pattern_ = Pattern.compile(FlumeConstants.INTEGER_REGEX);

    shell_command_ = FlumeConstants.GetShellCommand(monitor_dir_);
    shell_executor_ = new Shell.ShellCommandExecutor(shell_command_);
    executor_service_ = Executors.newScheduledThreadPool(1);
  }

  @Override
  public void Start() {
    // TODO Auto-generated method stub
    StringBuilder builder = new StringBuilder();
    builder.append("Start SimpleFileMonitor with [dir=");
    builder.append(monitor_dir_);
    builder.append(", file_name_file_name_include_pattern_str_=");
    builder.append(file_name_file_name_include_pattern_str_);
    builder.append(",file_name_file_name_exclude_pattern_str_=");
    builder.append(file_name_file_name_exclude_pattern_str_);
    builder.append(",check_interval_sec_=");
    builder.append(check_interval_sec_);
    LOG.info(builder.toString());
    builder = null;

    shell_runnable_ = new ShellRunnable();
    shell_future_ = executor_service_.scheduleAtFixedRate(shell_runnable_, 0L,
        check_interval_sec_.longValue(), TimeUnit.SECONDS);
  }

  @Override
  public void Stop() {
    // TODO Auto-generated method stub
    LOG.info("Stop SimpleFileMonitor");
    shell_future_.cancel(true);
    executor_service_.shutdown();
    file_info_map_ = null;
  }

  // this function the latest_offset is synchronized at entry
  @Override
  public Map<Integer, FileInfo> GetLatestFileInfo(
      Map<Integer, FileInfo> latest_offset) {
    // TODO Auto-generated method stub
    if (!can_fetch_file_map_) {
      // wait for next round to update
      return latest_offset;
    }
    
    Map<Integer, FileInfo> new_file_map = null;
    synchronized (this) {
      if (latest_offset != null) {
        for (FileInfo file_info : latest_offset.values()) {
          if (this.file_info_map_.containsKey(file_info.get_id())) {
            // update offset from latest_offset map to current map
            this.file_info_map_.get(file_info.get_id()).set_offset(
                file_info.get_offset());
          } else {
            
            // file is delete from monitor dir
            // we run into a problem, inode will be reused when file deleted
            // so we need to delete file as soon as possible
            // if (file_info.get_life_span() > 0) {
            //  file_info.DecLifeSpan();
            //  this.file_info_map_.put(file_info.get_id(), file_info);
            // }
          }
        }
      }
      new_file_map = file_info_map_;
      // file_info_map_ = null;
    }
    return new_file_map;
  }
 
  @Override
  public String GetMonitorDir() {
    // TODO Auto-generated method stub
    return monitor_dir_;
  }

  public void UpdateMapFromShellResult() {
    Map<Integer, FileInfo> new_file_map = new ConcurrentHashMap<Integer, FileInfo>();
    shell_output_ = shell_executor_.getOutput();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Shelloutput****************\n" + shell_output_);
    }
    for (String line : shell_output_.split(FlumeConstants.LINE_SEP)) {
      // String eles[] = shell_result_pattern_.split(line);
      String eles[] = StringUtil.SplitAndTrim(shell_result_pattern_, line);

      /**
       * when use ls -il -o -g --time-style=+%Y the first line of the output is
       * a total infomation like: total 22 ID_SHLL_IDX ID_META_IDX xx
       * FILE_LENGTH_SHELL_IDX xx FILE_NAME_IDX ${inode} drwxrwx-- 1 ${filesize}
       * ${date} ${filename}
       * 
       * (1) for first line, just skip (2) for othern line, if length !=6, print
       * warn information (3) for directory, skip (4) check ${inode} ${filesize)
       * match integer pattern
       * */
      if (eles[ID_SHELL_IDX].toLowerCase().startsWith(ls_output_total_)) {
        LOG.debug("Skip first line -- {}", line);
        continue;
      }
      if (eles.length != FlumeConstants.SHELL_RESULT_FIELD_NUM) {
        LOG.warn("Check system env, Invalid shell result,fields = {} line={} ",
            eles.length, line);
        continue;
      }

      if (eles[FILE_META_IDX].startsWith("d")) {
        // this file is a directory, skip
        LOG.debug("Skip monitor directory: " + eles[FILE_NAME_SHELL_IDX]);
        continue;
      }

      if (!integer_pattern_.matcher(eles[ID_SHELL_IDX]).matches()
          || !integer_pattern_.matcher(eles[FILE_LENGTH_SHELL_IDX]).matches()) {
        if (LOG.isDebugEnabled()) {
          StringBuilder builder = new StringBuilder();
          builder.append("Skip invalid integer regex line:");
          builder.append(line);
          builder.append("id_shell_idx=");
          builder.append(eles[ID_SHELL_IDX]);
          builder.append(" file_length=");
          builder.append(eles[FILE_LENGTH_SHELL_IDX]);
          LOG.debug(builder.toString());
          builder = null;
        }
        continue;
      }

      Matcher include_matcher = file_name_include_pattern_
          .matcher(eles[FILE_NAME_SHELL_IDX]);
      Matcher exclude_matcher = file_name_exclude_pattern_
          .matcher(eles[FILE_NAME_SHELL_IDX]);

      if (include_matcher.matches() && !exclude_matcher.matches()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("File accepted, " + eles[FILE_NAME_SHELL_IDX]);
        }
        // file in white list and not in black list is accepted
        FileInfo file_info = new FileInfo();
        // we already use INTEGER_REGEX to check the ID and Length must be number
        // but just in case, we catch the exception
        try {
          file_info.set_id(Integer.valueOf(eles[ID_SHELL_IDX]));
          file_info.set_file_length(Long.valueOf(eles[FILE_LENGTH_SHELL_IDX]));
        } catch (NumberFormatException e) {
          LOG.warn("Invalid shell result, number format error," + line);
          continue;
        }
        StringBuilder abs_path_builder = new StringBuilder();
        abs_path_builder.append(monitor_dir_);
        abs_path_builder.append(FlumeConstants.DIR_SEP);
        abs_path_builder.append(eles[FILE_NAME_SHELL_IDX]);
        file_info.set_file_name(abs_path_builder.toString());
        file_info.set_offset(0L);
        new_file_map.put(file_info.get_id(), file_info);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("File rejected, " + eles[FILE_NAME_SHELL_IDX]);
        }
      }
    }
    synchronized (this) {
      file_info_map_ = new_file_map;
      can_fetch_file_map_ = true;
    }
  }

  class ShellRunnable implements Runnable {
    @Override
    public void run() {
      // TODO Auto-generated method stub
      try {
        shell_executor_.execute();
        UpdateMapFromShellResult();
      } catch (Exception e) {
        LOG.warn("Execute shell failed due to " + e.getMessage());
        e.printStackTrace();
      }
    }
  }
}