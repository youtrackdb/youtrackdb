/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClassImpl;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTProperty.ATTRIBUTES;
import com.orientechnologies.orient.core.metadata.schema.YTPropertyImpl;
import com.orientechnologies.orient.core.sql.parser.OAlterPropertyStatement;
import com.orientechnologies.orient.core.sql.parser.OExpression;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

/**
 * SQL ALTER PROPERTY command: Changes an attribute of an existent property in the target class.
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLAlterProperty extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_ALTER = "ALTER";
  public static final String KEYWORD_PROPERTY = "PROPERTY";

  private String className;
  private String fieldName;
  private ATTRIBUTES attribute;
  private String value;

  public OCommandExecutorSQLAlterProperty parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_ALTER)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_ALTER + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_PROPERTY)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_PROPERTY + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, false);
      if (pos == -1) {
        throw new YTCommandSQLParsingException(
            "Expected <class>.<property>. Use " + getSyntax(), parserText, oldPos);
      }

      String[] parts = word.toString().split("\\.");
      if (parts.length != 2) {
        if (parts[1].startsWith("`") && parts[parts.length - 1].endsWith("`")) {
          StringBuilder fullName = new StringBuilder();
          for (int i = 1; i < parts.length; i++) {
            if (i > 1) {
              fullName.append(".");
            }
            fullName.append(parts[i]);
          }
          parts = new String[]{parts[0], fullName.toString()};
        } else {
          throw new YTCommandSQLParsingException(
              "Expected <class>.<property>. Use " + getSyntax(), parserText, oldPos);
        }
      }

      className = decodeClassName(parts[0]);
      if (className == null) {
        throw new YTCommandSQLParsingException("Class not found", parserText, oldPos);
      }
      fieldName = decodeClassName(parts[1]);

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1) {
        throw new YTCommandSQLParsingException(
            "Missing property attribute to change. Use " + getSyntax(), parserText, oldPos);
      }

      final String attributeAsString = word.toString();

      try {
        attribute = YTProperty.ATTRIBUTES.valueOf(attributeAsString.toUpperCase(Locale.ENGLISH));
      } catch (IllegalArgumentException e) {
        throw YTException.wrapException(
            new YTCommandSQLParsingException(
                "Unknown property attribute '"
                    + attributeAsString
                    + "'. Supported attributes are: "
                    + Arrays.toString(YTProperty.ATTRIBUTES.values()),
                parserText,
                oldPos),
            e);
      }

      value = parserText.substring(pos + 1).trim();
      if (attribute.equals(ATTRIBUTES.NAME) || attribute.equals(ATTRIBUTES.LINKEDCLASS)) {
        value = decodeClassName(value);
      }

      if (value.length() == 0) {
        throw new YTCommandSQLParsingException(
            "Missing property value to change for attribute '"
                + attribute
                + "'. Use "
                + getSyntax(),
            parserText,
            oldPos);
      }

      if (preParsedStatement != null) {
        OExpression settingExp = ((OAlterPropertyStatement) preParsedStatement).settingValue;
        if (settingExp != null) {
          Object expValue = settingExp.execute((YTIdentifiable) null, context);
          if (expValue == null) {
            expValue = settingExp.toString();
          }
          if (expValue instanceof Date) {
            value = ODateHelper.getDateTimeFormatInstance().format((Date) expValue);
          } else {
            value = expValue.toString();
          }

          if (attribute.equals(ATTRIBUTES.NAME) || attribute.equals(ATTRIBUTES.LINKEDCLASS)) {
            value = decodeClassName(value);
          }
        }
      } else {
        if (value.equalsIgnoreCase("null")) {
          value = null;
        }
        if (value != null && isQuoted(value)) {
          value = removeQuotes(value);
        }
      }
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  private String removeQuotes(String s) {
    s = s.trim();
    return s.substring(1, s.length() - 1).replaceAll("\\\\\"", "\"");
  }

  private boolean isQuoted(String s) {
    s = s.trim();
    if (s.startsWith("\"") && s.endsWith("\"")) {
      return true;
    }
    if (s.startsWith("'") && s.endsWith("'")) {
      return true;
    }
    return s.startsWith("`") && s.endsWith("`");
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(YTGlobalConfiguration.DISTRIBUTED_COMMAND_QUICK_TASK_SYNCH_TIMEOUT);
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  /**
   * Execute the ALTER PROPERTY.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (attribute == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not yet been parsed");
    }

    var db = getDatabase();
    final YTClassImpl sourceClass =
        (YTClassImpl) db.getMetadata().getSchema().getClass(className);
    if (sourceClass == null) {
      throw new YTCommandExecutionException("Source class '" + className + "' not found");
    }

    final YTPropertyImpl prop = (YTPropertyImpl) sourceClass.getProperty(fieldName);
    if (prop == null) {
      throw new YTCommandExecutionException(
          "Property '" + className + "." + fieldName + "' not exists");
    }

    if ("null".equalsIgnoreCase(value)) {
      prop.set(db, attribute, null);
    } else {
      prop.set(db, attribute, value);
    }
    return null;
  }

  public String getSyntax() {
    return "ALTER PROPERTY <class>.<property> <attribute-name> <attribute-value>";
  }
}
