package com.minsheng.util;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringUtil {
  public static String[] Split(String content, String sub_seq) {
    int start_index = 0;
    ArrayList<String> ret = new ArrayList<String>();

    int pos = -1; 
    while (start_index < content.length() && 
        (pos = content.indexOf(sub_seq, start_index)) != -1) {
      ret.add(content.substring(start_index, pos+sub_seq.length()));
      start_index = pos + sub_seq.length();
    }
    if (start_index < content.length()) {
      ret.add(content.substring(start_index));
    } 
    String[] result = new String[ret.size()];
    return ret.toArray(result);
  }
  
  
  public static String[] SplitAndTrim(Pattern pat, CharSequence input) {
    int index = 0;
    ArrayList<String> matchList = new ArrayList<String>();
    Matcher m = pat.matcher(input);

    while (m.find()) {
      String match = input.subSequence(index, m.start()).toString();
      if (!match.trim().isEmpty())
        matchList.add(match);
      index = m.end();
    }

    // If no match was found, return this
    if (index == 0)
        return new String[] {input.toString()};
    matchList.add(input.subSequence(index, input.length()).toString());
    int resultSize = matchList.size();
    String[] result = new String[resultSize];
    return matchList.subList(0, resultSize).toArray(result);
}
}