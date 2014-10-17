// Copyright (c) 2014 Minsheng.Corp. All rights reserved
// Author: peng.he.ia@gmail.com <he peng>
package com.minsheng.flume.source;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.minsheng.util.StringUtil;

public class MultiLineParser extends FileParser {
  private static final Logger LOG = LoggerFactory
      .getLogger(MultiLineParser.class);

  private int RECORD_INIT_SIZE = 1024;
  private int max_record_size_ = FlumeConstants.MAX_RECORD_LENGH;
  private int max_buffer_size_ = FlumeConstants.MAX_READ_BUFFER_SIZE;
  // start from 1MB
  private int buffer_size_ = FlumeConstants.READ_BUFFER_SIZE;
  private byte[] read_buffer_ = null;
  private String content_str_ = null;
  /*
   * when use ParseFirst mode, we need a signal to indicate: hi, body,this is
   * the last line (without the end line pattern), we should treat it as a
   * record, we do not need to wait data
   */
  private boolean is_end_of_file_ = false;

  /*
   * this pattern is for filter record; default all records are accepted;
   */
  
  private String default_content_include_str_ = "[\\s\\S]*"; // match all,
  private String file_content_include_pattern_str_ 
        = default_content_include_str_;
  // default no exclude
  private String file_content_exclude_pattern_str_ = null;
  private Pattern record_include_pattern_ = null;
  private Pattern record_exclude_pattern_ = null;

  /*
   * all combination:
   * 
   * MIX -- first & last --ret1 = line.match(first),ret2 = line.match(last) (1)
   * ret1 == true && ret2 == true current line is a new record (2) ret1 == true
   * && ret2 == false read next line and do the process again (3) ret1 == false
   * && ret2 == true current line is the last line in current record (4) ret1 ==
   * false && ret2 == false invalid line means maybe the "first_line_pattern"
   * and the "last_line_pattern" should be modified for matchingFIRST -- first
   * --ret1 = line.match(first) (1) ret1 == true current line is the start line
   * of a new record; it means the previous line is the last line of the current
   * record; (2) ret1 == false current line belongs to the current record, read
   * next to processLAST -- last --ret1 = line.match(last) (1) ret1 == true
   * current line is the last line of current record; and next line is the start
   * line of the next record; (2) ret1 == false current line belongs to the
   * current record; read next line to processNONE -- none --use default
   * strategy, treat every line as a new recorddefault is none
   */
  enum ParseType {
    MIX, FIRST, LAST, NONE
  };

  private ParseType parse_type_ = ParseType.NONE;
  private String first_line_pattern_str_ = null;
  private String last_line_pattern_str_ = null;
  private Pattern first_line_pattern_ = null;
  private Pattern last_line_pattern_ = null;

  /*
   * parse_type
   */

  public MultiLineParser() {
    super();
  }

  @Override
  public List<String> GetNextBatchRecords(String file_name, Long offset) {
    // TODO Auto-generated method stub
    RandomAccessFile file_reader = null;
    try {
      file_reader = new RandomAccessFile(file_name, "r");
      if (file_reader.length() < offset) {
        LOG.warn("File length small than read offset, truncate(rename)? offset="
            + offset + " file_length=" + file_reader.length());
        return new ArrayList<String>();
      }
      file_reader.seek(offset.longValue());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Random Read: file {}, start {}, buffer_size {}" + buffer_size_,
            file_name, offset);
      }
      int read_bytes = file_reader.read(read_buffer_, 0,
          read_buffer_.length);
      // if read_bytes == -1, means stream is at end of file,
      List<String> records = ParseRecord(read_bytes);
      if (records == null) {
        // the buffer is two large, we treat this as error
        LOG.error("SUPER-WARNING, we meet super-huge line, which is larger than " 
             + buffer_size_ + "MB, the following process will skip this file:" + 
             file_name + " read_bytes=" + read_bytes + " offset=" + offset);
        return new ArrayList<String>();
      }
      return records;
    } catch (FileNotFoundException e) {
      LOG.warn("target monitor file not exist, " + file_name);
    } catch (IOException e) {
      LOG.error("Read file error due to " + e.getMessage());
    } finally {
      if (file_reader != null) {
        try {
          file_reader.close();
          file_reader = null;
        } catch (IOException e) {

        }
      }
    }
    return new ArrayList<String>();
  }

  /*
   * MIX -- first & last --ret1 = line.match(first),ret2 = line.match(last) (1)
   * ret1 == true && ret2 == true current line is a new record (2) ret1 == true
   * && ret2 == false read next line and do the process again (3) ret1 == false
   * && ret2 == true current line is the last line in current record (4) ret1 ==
   * false && ret2 == false invalid line means maybe the "first_line_pattern"
   * and the "last_line_pattern" should be modified for matching
   */
  private List<String> ParseMIX(String lines[], int end_idx) {
    StringBuilder record = new StringBuilder(RECORD_INIT_SIZE);
    List<String> records = new ArrayList<String>();
    for (int i = 0; i < end_idx; i++) {
      boolean match_first = first_line_pattern_.matcher(lines[i]).matches();
      boolean match_last = last_line_pattern_.matcher(lines[i]).matches();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Process Line: " + lines[i]);
      }
      if (match_first) {
        if (record.length() > 0) {
          // means the previous line is also the end line of current record
          records.add(record.toString());
          if (LOG.isDebugEnabled()) {
            LOG.debug("MATCH START, Get a new record(which miss its end line):"
                + record.toString());
          }
        }
        // create a new record
        record = new StringBuilder(RECORD_INIT_SIZE);
        if (match_last) {
          // current line is a record(first last both matched);
          // records.add(lines[i] + FlumeConstants.LINE_SEP);
          // we use our Split method, no need to add LINE_SEP
          records.add(lines[i]);
          if (LOG.isDebugEnabled()) {
            LOG.debug("MATCH START-LAST, Get a new record:" + lines[i]);
          }
        } else {
          record.append(lines[i]);
          // record.append(FlumeConstants.LINE_SEP);
        }
      } else if (match_last) {
        record.append(lines[i]);
        // record.append(FlumeConstants.LINE_SEP);
        records.add(record.toString());
        if (LOG.isDebugEnabled()) {
          LOG.debug("MATCH LAST, Get a new record:" + record.toString());
        }

        record = null;
        record = new StringBuilder(RECORD_INIT_SIZE);
      } else {
        record.append(lines[i]);
        // this is a middle line, we recovery it's '\n' character
        // in parserecord, new Split called
        // record.append(FlumeConstants.LINE_SEP);
      }
    }
    
    HandleSpecialSituation(record, records);
    record = null;
    return records;
  }

  private List<String> ParseFIRST(String[] lines, int end_idx) {
    StringBuilder record = new StringBuilder(RECORD_INIT_SIZE);
    List<String> records = new ArrayList<String>();
    for (int i = 0; i < end_idx; i++) {
      boolean match_first = first_line_pattern_.matcher(lines[i]).matches();
      if (match_first) {
        if (record.length() > 0) {
          // means the previous line is also the end line of current record
          records.add(record.toString());
          // create a new record
          if (LOG.isDebugEnabled()) {
            LOG.debug("MATCH first, get a new record: " + record.toString());
          }
          record = null;
          record = new StringBuilder(RECORD_INIT_SIZE);
        }
        record.append(lines[i]);
        // record.append(FlumeConstants.LINE_SEP);
      } else {
        record.append(lines[i]);
        // record.append(FlumeConstants.LINE_SEP);
      }
    }
    
    HandleSpecialSituation(record, records);
    record = null;
    return records;
  }

  private List<String> ParseLAST(String[] lines, int end_idx) {
    StringBuilder record = new StringBuilder(RECORD_INIT_SIZE);
    List<String> records = new ArrayList<String>();
    for (int i = 0; i < end_idx; i++) {
      boolean match_last = last_line_pattern_.matcher(lines[i]).matches();
      if (match_last) {
        record.append(lines[i]);
        // record.append(FlumeConstants.LINE_SEP);
        records.add(record.toString());
        LOG.debug("MATCH last, get a new record" + record.toString());
        record = null;
        record = new StringBuilder(RECORD_INIT_SIZE);
      } else {
        record.append(lines[i]);
        // record.append(FlumeConstants.LINE_SEP);
      }
    }
    /*
     * means the last record is incomplete, we need more data to handle the last
     * record, just skip if (record.length() >0) {
     * 
     * }
     */
    record = null;
    return records;
  }
  
  
  private boolean ExpandReadBuffer() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ExpandReadBuffer called, current buffer size:" + buffer_size_);
    }
    if ((buffer_size_ << 1) > max_buffer_size_) {
      // this is the only place we return null instead of empty list
      return false;
    }
    read_buffer_ = null;
    buffer_size_ = buffer_size_ * 2;
    read_buffer_ = new byte[buffer_size_];
    return true;
  }
  
  private void HandleSpecialSituation(StringBuilder record, 
 List<String> records) {
    // end of file
    if (is_end_of_file_ && record.length() > 0) {
      // this is the end line of the record
      records.add(record.toString());
      if (LOG.isDebugEnabled()) {
        LOG.debug("end of file in First or mix mode, get a new record"
            + record.toString());
      }
    }
    
    /*
     * we processed all the lines, but we do not get a record until now, it
     * means this must be a very large record, currently we just get a piece of
     * this record. on assumption this situation is rarely
     */
    if (records.size() == 0) {
      if (record.length() > max_record_size_) {
        records.add(record.toString());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Big record, get a new record(part of) " + record.toString());
        }
      } 
    }
  }

  private List<String> ParseNONE(String[] lines, int end_idx) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ParseNONE mode, handle line num" + lines.length);
    }
    List<String> records = new ArrayList<String>();
    for (int i = 0; i < end_idx; i++) {
      // take each line as a record
      records.add(lines[i]);
    }
    return records;
  }

  private List<String> ParseRecord(int read_bytes) {
    if (-1 == read_bytes) {
      // no more data can be read
      return new ArrayList<String>();
    }

    // only used for ParseFirst
    is_end_of_file_ = false;
    if (read_bytes < read_buffer_.length) {
      is_end_of_file_ = true;
    }
    
    content_str_ = new String(read_buffer_, 0, read_bytes);
    /*
     * special situation: very-very big line, content in read_buffer_ is part of
     * a line, in this situation, we return null, and double the buffer size
     */
    if (!is_end_of_file_ && content_str_.indexOf(FlumeConstants.LINE_SEP) == -1) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("cannot find a linesep in this content, content_size: {}, content ={}",
            content_str_.length(), content_str_);
      }
      if (!ExpandReadBuffer()) {
        return null;
      }
      return new ArrayList<String>();
    }
    
    int last_line_sep = content_str_.lastIndexOf(FlumeConstants.LINE_SEP);
    
    /**when use String.split,it will split "123\n\n" to:
     *    "123"
     * whe use StringUtil.Split, it split "123\n\n"to:
     *   "123\n"
     *   "\n"
     * we need StringUtil.Split
     * */  
    // String[] lines = content_str_.split(FlumeConstants.LINE_SEP);
    String[] lines = StringUtil.Split(content_str_, FlumeConstants.LINE_SEP);

    /*
     * this means,the last line is a complete line, we can handle it otherwise,
     * it is just part of the line, drop it in this process round
     */
    int end_idx = lines.length;
    if (!is_end_of_file_ && last_line_sep != (content_str_.length() - 1)) {
      // the last line is not a complete line
      end_idx = end_idx - 1;
    }
   
    List<String> records = null;
    switch (parse_type_) {
    case MIX:
      records = ParseMIX(lines, end_idx);
      break;
    case FIRST:
      records = ParseFIRST(lines, end_idx);
      break;
    case LAST:
      records = ParseLAST(lines, end_idx);
      break;
    default:
      records = ParseNONE(lines, end_idx);
      break;
    }
    
    if (!is_end_of_file_ && records.size() == 0 && content_str_.length() < max_record_size_) {
      /**
       * this is a tough situation current data in lines is part of a record,
       * but current data is small than max_record_size, this will make it into
       * the following loop: (1) read data into buffer; (2) parse data, found no
       * new record( records.size == 0 current lines is a part of the record)
       * (3) current data is small than max record, so no record generate, and
       * buffer do not double the size; (4) next round, go to the (1),with no
       * change of all meata data(offset,buffersize) data
       * */
      if (LOG.isDebugEnabled()) {
        LOG.debug("read lines {}, but no record, expand buffer", lines.length);
      }
      if (!ExpandReadBuffer()) {
        return null;
      }
    }
    return records;
  }

  /*
   * @return false -- this record is valid, keep it true -- this record is
   * invalid, drop it
   */
  public boolean ShouldDrop(String record) {
    Matcher in_matcher = record_include_pattern_.matcher(record);

    if (!in_matcher.matches()) {
      // not in white list, drop it;(default pattern matches all)
      return true;
    }

    if (record_exclude_pattern_ != null) {
      Matcher ex_matcher = record_exclude_pattern_.matcher(record);
      if (ex_matcher.matches()) {
        // in black list, should drop
        return true;
      }
    }
    // in white list and not in black list, this record is legal, keep it
    return false;
  }

  @Override
  public void Configure(Context context) {
    // TODO Auto-generated method stub
    LOG.info("Config MultiLineParser");
    buffer_size_ = context.getInteger("read_buffer_size", 
        FlumeConstants.READ_BUFFER_SIZE).intValue();
    read_buffer_ = new byte[buffer_size_];
    max_buffer_size_ = context.getInteger("max_read_buffer_size", 
        FlumeConstants.MAX_READ_BUFFER_SIZE).intValue();
    max_record_size_ = context.getInteger("max_record_size",
        FlumeConstants.MAX_RECORD_LENGH);
    
    file_content_include_pattern_str_ = context.getString(
        FlumeConstants.FILE_CONTENT_INCLUDE, default_content_include_str_);
    file_content_exclude_pattern_str_ = context
        .getString(FlumeConstants.FILE_CONTENT_EXCLUDE);

    record_include_pattern_ = Pattern
        .compile(file_content_include_pattern_str_);
    if (file_content_exclude_pattern_str_ != null) {
      record_exclude_pattern_ = Pattern
          .compile(file_content_exclude_pattern_str_);
    }

    first_line_pattern_str_ = context
        .getString(FlumeConstants.FIRST_LINE_PATTERN);
    last_line_pattern_str_ = context
        .getString(FlumeConstants.LAST_LINE_PATTERN);
    if (first_line_pattern_str_ != null) {
      first_line_pattern_ = Pattern.compile(first_line_pattern_str_);
    }
    if (last_line_pattern_str_ != null) {
      last_line_pattern_ = Pattern.compile(last_line_pattern_str_);
    }

    if (first_line_pattern_ != null && last_line_pattern_ != null) {
      parse_type_ = ParseType.MIX;
    } else if (first_line_pattern_ != null) {
      parse_type_ = ParseType.FIRST;
    } else if (last_line_pattern_ != null) {
      parse_type_ = ParseType.LAST;
    } else {
      parse_type_ = ParseType.NONE;
    }

    StringBuilder builder = new StringBuilder();
    builder.append("Config MultiLineParser with [");
    builder.append("read_buffer_size(init)=");
    builder.append(buffer_size_);
    builder.append(",max_buffer_size=");
    builder.append(max_buffer_size_);
    builder.append(",max_record_size=");
    builder.append(max_record_size_);
    builder.append(",first_line_pattern_str_=");
    builder.append(first_line_pattern_str_);
    builder.append(",last_line_pattern_str_=");
    builder.append(last_line_pattern_str_);
    builder.append(",file_content_include_pattern_str_=");
    builder.append(file_content_include_pattern_str_);
    builder.append(",record_include_pattern_=");
    builder.append(record_include_pattern_ == null ? "null"
        : record_include_pattern_.toString());
    builder.append(",file_content_exclude_pattern_str_=");
    builder.append(file_content_exclude_pattern_str_);
    builder.append(",record_exclude_pattern_=");
    builder.append(record_exclude_pattern_ == null ? "null"
        : record_exclude_pattern_.toString());
    builder.append(", parse_type=");
    builder.append("" + parse_type_);
    builder.append("]");
    LOG.info(builder.toString());
    builder = null;
  }
}