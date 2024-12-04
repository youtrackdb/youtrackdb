package com.orientechnologies.orient.core.serialization.serializer.record.string;

import com.orientechnologies.orient.core.metadata.schema.YTType;
import java.util.HashMap;
import java.util.Map;

public class OFieldTypesString {

  public static final String ATTRIBUTE_FIELD_TYPES = "@fieldTypes";

  /**
   * Parses the field type char returning the closer type. Default is STRING. b=binary if
   * iValue.length() >= 4 b=byte if iValue.length() <= 3 s=short, l=long f=float d=double a=date
   * t=datetime
   *
   * @param iValue    Value to parse
   * @param iCharType Char value indicating the type
   * @return The closest type recognized
   */
  public static YTType getType(final String iValue, final char iCharType) {
    if (iCharType == 'f') {
      return YTType.FLOAT;
    } else if (iCharType == 'c') {
      return YTType.DECIMAL;
    } else if (iCharType == 'l') {
      return YTType.LONG;
    } else if (iCharType == 'd') {
      return YTType.DOUBLE;
    } else if (iCharType == 'b') {
      if (iValue.length() >= 1 && iValue.length() <= 3) {
        return YTType.BYTE;
      } else {
        return YTType.BINARY;
      }
    } else if (iCharType == 'a') {
      return YTType.DATE;
    } else if (iCharType == 't') {
      return YTType.DATETIME;
    } else if (iCharType == 's') {
      return YTType.SHORT;
    } else if (iCharType == 'e') {
      return YTType.EMBEDDEDSET;
    } else if (iCharType == 'g') {
      return YTType.LINKBAG;
    } else if (iCharType == 'z') {
      return YTType.LINKLIST;
    } else if (iCharType == 'm') {
      return YTType.LINKMAP;
    } else if (iCharType == 'x') {
      return YTType.LINK;
    } else if (iCharType == 'n') {
      return YTType.LINKSET;
    } else if (iCharType == 'u') {
      return YTType.CUSTOM;
    }

    return YTType.STRING;
  }

  public static YTType getOTypeFromChar(final char iCharType) {
    if (iCharType == 'f') {
      return YTType.FLOAT;
    } else if (iCharType == 'c') {
      return YTType.DECIMAL;
    } else if (iCharType == 'l') {
      return YTType.LONG;
    } else if (iCharType == 'd') {
      return YTType.DOUBLE;
    } else if (iCharType == 'b') {
      return YTType.BINARY;
    } else if (iCharType == 'a') {
      return YTType.DATE;
    } else if (iCharType == 't') {
      return YTType.DATETIME;
    } else if (iCharType == 's') {
      return YTType.SHORT;
    } else if (iCharType == 'e') {
      return YTType.EMBEDDEDSET;
    } else if (iCharType == 'g') {
      return YTType.LINKBAG;
    } else if (iCharType == 'z') {
      return YTType.LINKLIST;
    } else if (iCharType == 'm') {
      return YTType.LINKMAP;
    } else if (iCharType == 'x') {
      return YTType.LINK;
    } else if (iCharType == 'n') {
      return YTType.LINKSET;
    } else if (iCharType == 'u') {
      return YTType.CUSTOM;
    }

    return YTType.STRING;
  }

  public static Map<String, Character> loadFieldTypesV0(
      Map<String, Character> fieldTypes, final String fieldValueAsString) {
    // LOAD THE FIELD TYPE MAP
    final String[] fieldTypesParts = fieldValueAsString.split(",");
    if (fieldTypesParts.length > 0) {
      if (fieldTypes == null) {
        fieldTypes = new HashMap<>();
      }
      String[] part;
      for (String f : fieldTypesParts) {
        part = f.split("=");
        if (part.length == 2) {
          fieldTypes.put(part[0], part[1].charAt(0));
        }
      }
    }
    return fieldTypes;
  }

  public static Map<String, Character> loadFieldTypes(final String fieldValueAsString) {
    Map<String, Character> fieldTypes = new HashMap<>();
    loadFieldTypesV0(fieldTypes, fieldValueAsString);
    return fieldTypes;
  }
}
