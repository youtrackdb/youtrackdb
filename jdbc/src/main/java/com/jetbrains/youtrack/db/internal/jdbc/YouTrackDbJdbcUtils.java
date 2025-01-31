package com.jetbrains.youtrack.db.internal.jdbc;

import java.util.regex.Pattern;

/**
 *
 */
public class YouTrackDbJdbcUtils {

  public static boolean like(final String str, final String expr) {
    var regex = quotemeta(expr);
    regex = regex.replace("_", ".").replace("%", ".*?");
    var p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
    return p.matcher(str).matches();
  }

  public static String quotemeta(String s) {
    if (s == null) {
      throw new IllegalArgumentException("String cannot be null");
    }

    var len = s.length();
    if (len == 0) {
      return "";
    }

    var sb = new StringBuilder(len * 2);
    for (var i = 0; i < len; i++) {
      var c = s.charAt(i);
      if ("[](){}.*+?$^|#\\".indexOf(c) != -1) {
        sb.append("\\");
      }
      sb.append(c);
    }
    return sb.toString();
  }
}
