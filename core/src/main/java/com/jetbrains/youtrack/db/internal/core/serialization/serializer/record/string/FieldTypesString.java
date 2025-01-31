package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import java.util.HashMap;
import java.util.Map;

public class FieldTypesString {

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
  public static PropertyType getType(final String iValue, final char iCharType) {
    if (iCharType == 'f') {
      return PropertyType.FLOAT;
    } else if (iCharType == 'c') {
      return PropertyType.DECIMAL;
    } else if (iCharType == 'l') {
      return PropertyType.LONG;
    } else if (iCharType == 'd') {
      return PropertyType.DOUBLE;
    } else if (iCharType == 'b') {
      if (iValue.length() >= 1 && iValue.length() <= 3) {
        return PropertyType.BYTE;
      } else {
        return PropertyType.BINARY;
      }
    } else if (iCharType == 'a') {
      return PropertyType.DATE;
    } else if (iCharType == 't') {
      return PropertyType.DATETIME;
    } else if (iCharType == 's') {
      return PropertyType.SHORT;
    } else if (iCharType == 'e') {
      return PropertyType.EMBEDDEDSET;
    } else if (iCharType == 'g') {
      return PropertyType.LINKBAG;
    } else if (iCharType == 'z') {
      return PropertyType.LINKLIST;
    } else if (iCharType == 'm') {
      return PropertyType.LINKMAP;
    } else if (iCharType == 'x') {
      return PropertyType.LINK;
    } else if (iCharType == 'n') {
      return PropertyType.LINKSET;
    } else if (iCharType == 'u') {
      return PropertyType.CUSTOM;
    }

    return PropertyType.STRING;
  }

  public static PropertyType getOTypeFromChar(final char iCharType) {
    if (iCharType == 'f') {
      return PropertyType.FLOAT;
    } else if (iCharType == 'c') {
      return PropertyType.DECIMAL;
    } else if (iCharType == 'l') {
      return PropertyType.LONG;
    } else if (iCharType == 'd') {
      return PropertyType.DOUBLE;
    } else if (iCharType == 'b') {
      return PropertyType.BINARY;
    } else if (iCharType == 'a') {
      return PropertyType.DATE;
    } else if (iCharType == 't') {
      return PropertyType.DATETIME;
    } else if (iCharType == 's') {
      return PropertyType.SHORT;
    } else if (iCharType == 'e') {
      return PropertyType.EMBEDDEDSET;
    } else if (iCharType == 'g') {
      return PropertyType.LINKBAG;
    } else if (iCharType == 'z') {
      return PropertyType.LINKLIST;
    } else if (iCharType == 'm') {
      return PropertyType.LINKMAP;
    } else if (iCharType == 'x') {
      return PropertyType.LINK;
    } else if (iCharType == 'n') {
      return PropertyType.LINKSET;
    } else if (iCharType == 'u') {
      return PropertyType.CUSTOM;
    }

    return PropertyType.STRING;
  }

  public static Map<String, Character> loadFieldTypesV0(
      Map<String, Character> fieldTypes, final String fieldValueAsString) {
    // LOAD THE FIELD TYPE MAP
    final var fieldTypesParts = fieldValueAsString.split(",");
    if (fieldTypesParts.length > 0) {
      if (fieldTypes == null) {
        fieldTypes = new HashMap<>();
      }
      String[] part;
      for (var f : fieldTypesParts) {
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
