package com.raysmond.wiki.util;

/**
 * Globally available utility classes, mostly for string manipulation.
 * 
 * @author Jim Menard, <a href="mailto:jimm@io.com">jimm@io.com</a>
 */
public class StringUtils {
  /**
   * Returns a string with HTML special characters replaced by their entity
   * equivalents.
   * 
   * @param str
   *          the string to escape
   * @return a new string without HTML special characters
   */
  public static String escapeHTML(String str) {
    if (str == null || str.length() == 0)
      return "";

    StringBuffer buf = new StringBuffer();
    int len = str.length();
    for (int i = 0; i < len; ++i) {
      char c = str.charAt(i);
      switch (c) {
      case '&':
        buf.append("&amp;");
        break;
      case '<':
        buf.append("&lt;");
        break;
      case '>':
        buf.append("&gt;");
        break;
      case '"':
        buf.append("&quot;");
        break;
      case '\'':
        buf.append("&apos;");
        break;
      default:
        buf.append(c);
        break;
      }
    }
    return buf.toString();
  }

  /**
   * Returns a new string where all newlines (&quot;\n&quot;, &quot;\r&quot;, or
   * &quot;\r\n&quot;) have been replaced by &quot;\n&quot; plus XHTML break
   * tags (&quot;\n&lt;br /&gt;&quot;).
   * <p>
   * We don't call <code>splitIntoLines</code> because that method does not
   * tell us if the string ended with a newline or not.
   * 
   * @param str
   *          any string
   * @return a new string with all newlines replaced by &quot;\n&lt;br
   *         /&gt;&quot;
   */
  public static String newlinesToXHTMLBreaks(String str) {
    if (str == null || str.length() == 0)
      return "";

    StringBuffer buf = new StringBuffer();
    int len = str.length();
    for (int i = 0; i < len; ++i) {
      char c = str.charAt(i);
      switch (c) {
      case '\n':
        buf.append("\n<br />");
        break;
      case '\r':
        if (i + 1 < len && str.charAt(i + 1) == '\n') // Look for '\n'
          ++i;
        buf.append("\n<br />");
        break;
      default:
        buf.append(c);
        break;
      }
    }
    return buf.toString();
  }

  /**
   * Returns a string with XML special characters replaced by their entity
   * equivalents.
   * 
   * @param str
   *          the string to escape
   * @return a new string without XML special characters
   */
  public static String escapeXML(String str) {
    return escapeHTML(str);
  }

  /**
   * Returns a string with XML entities replaced by their normal characters.
   * 
   * @param str
   *          the string to un-escape
   * @return a new normal string
   */
  public static String unescapeXML(String str) {
    if (str == null || str.length() == 0)
      return "";

    StringBuffer buf = new StringBuffer();
    int len = str.length();
    for (int i = 0; i < len; ++i) {
      char c = str.charAt(i);
      if (c == '&') {
        int pos = str.indexOf(";", i);
        if (pos == -1) { // Really evil
          buf.append('&');
        } else if (str.charAt(i + 1) == '#') {
          int val = Integer.parseInt(str.substring(i + 2, pos), 16);
          buf.append((char) val);
          i = pos;
        } else {
          String substr = str.substring(i, pos + 1);
          if (substr.equals("&amp;"))
            buf.append('&');
          else if (substr.equals("&lt;"))
            buf.append('<');
          else if (substr.equals("&gt;"))
            buf.append('>');
          else if (substr.equals("&quot;"))
            buf.append('"');
          else if (substr.equals("&apos;"))
            buf.append('\'');
          else
            // ????
            buf.append(substr);
          i = pos;
        }
      } else {
        buf.append(c);
      }
    }
    return buf.toString();
  }

  /**
   * Returns <var>str</var> with leading and trailing spaces trimmed or, if
   * <var>str</var> is <code>null</code>, returns <code>null</code>.
   * 
   * @return str trimmed or <code>null</code>
   */
  public static String nullOrTrimmed(String str) {
    return str == null ? str : str.trim();
  }

}