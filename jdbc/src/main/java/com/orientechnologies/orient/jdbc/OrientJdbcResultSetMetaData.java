/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.db.record.LinkList;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/**
 *
 */
public class OrientJdbcResultSetMetaData implements ResultSetMetaData {

  private static final Map<YTType, Integer> typesSqlTypes = new HashMap<>();

  static {
    typesSqlTypes.put(YTType.STRING, Types.VARCHAR);
    typesSqlTypes.put(YTType.INTEGER, Types.INTEGER);
    typesSqlTypes.put(YTType.FLOAT, Types.FLOAT);
    typesSqlTypes.put(YTType.SHORT, Types.SMALLINT);
    typesSqlTypes.put(YTType.BOOLEAN, Types.BOOLEAN);
    typesSqlTypes.put(YTType.LONG, Types.BIGINT);
    typesSqlTypes.put(YTType.DOUBLE, Types.DOUBLE);
    typesSqlTypes.put(YTType.DECIMAL, Types.DECIMAL);
    typesSqlTypes.put(YTType.DATE, Types.DATE);
    typesSqlTypes.put(YTType.DATETIME, Types.TIMESTAMP);
    typesSqlTypes.put(YTType.BYTE, Types.TINYINT);
    typesSqlTypes.put(YTType.SHORT, Types.SMALLINT);

    // NOT SURE ABOUT THE FOLLOWING MAPPINGS
    typesSqlTypes.put(YTType.BINARY, Types.BINARY);
    typesSqlTypes.put(YTType.EMBEDDED, Types.JAVA_OBJECT);
    typesSqlTypes.put(YTType.EMBEDDEDLIST, Types.ARRAY);
    typesSqlTypes.put(YTType.EMBEDDEDMAP, Types.JAVA_OBJECT);
    typesSqlTypes.put(YTType.EMBEDDEDSET, Types.ARRAY);
    typesSqlTypes.put(YTType.LINK, Types.JAVA_OBJECT);
    typesSqlTypes.put(YTType.LINKLIST, Types.ARRAY);
    typesSqlTypes.put(YTType.LINKMAP, Types.JAVA_OBJECT);
    typesSqlTypes.put(YTType.LINKSET, Types.ARRAY);
    typesSqlTypes.put(YTType.TRANSIENT, Types.NULL);
  }

  private final String[] fieldNames;
  private final OrientJdbcResultSet resultSet;

  public OrientJdbcResultSetMetaData(
      OrientJdbcResultSet orientJdbcResultSet, List<String> fieldNames) {
    resultSet = orientJdbcResultSet;
    this.fieldNames = fieldNames.toArray(new String[]{});
  }

  public static Integer getSqlType(final YTType iType) {
    return typesSqlTypes.get(iType);
  }

  public int getColumnCount() throws SQLException {

    return fieldNames.length;
  }

  @Override
  public String getCatalogName(final int column) throws SQLException {
    // return an empty String according to the method's documentation
    return "";
  }

  @Override
  public String getColumnClassName(final int column) throws SQLException {
    Object value = this.resultSet.getObject(column);
    if (value == null) {
      return null;
    }
    return value.getClass().getCanonicalName();
  }

  @Override
  public int getColumnDisplaySize(final int column) throws SQLException {
    return 0;
  }

  @Override
  public String getColumnLabel(final int column) throws SQLException {
    return getColumnName(column);
  }

  @Override
  public String getColumnName(final int column) throws SQLException {
    return fieldNames[column - 1];
  }

  @Override
  public int getColumnType(final int column) throws SQLException {
    final YTResult currentRecord = getCurrentRecord();

    if (column > fieldNames.length) {
      return Types.NULL;
    }

    String fieldName = fieldNames[column - 1];

    YTType otype =
        currentRecord
            .toEntity()
            .getSchemaType()
            .map(st -> st.getProperty(fieldName))
            .map(op -> op.getType())
            .orElse(null);

    if (otype == null) {
      Object value = currentRecord.getProperty(fieldName);

      if (value == null) {
        return Types.NULL;
      } else if (value instanceof YTBlob) {
        // Check if the type is a binary record or a collection of binary
        // records
        return Types.BINARY;
      } else if (value instanceof LinkList list) {
        // check if all the list items are instances of YTRecordBytes
        ListIterator<YTIdentifiable> iterator = list.listIterator();
        YTIdentifiable listElement;
        boolean stop = false;
        while (iterator.hasNext() && !stop) {
          listElement = iterator.next();
          if (!(listElement instanceof YTBlob)) {
            stop = true;
          }
        }
        if (!stop) {
          return Types.BLOB;
        }
      }
      return getSQLTypeFromJavaClass(value);
    } else {
      if (otype == YTType.EMBEDDED || otype == YTType.LINK) {
        Object value = currentRecord.getProperty(fieldName);
        if (value == null) {
          return Types.NULL;
        }
        // 1. Check if the type is another record or a collection of records
        if (value instanceof YTBlob) {
          return Types.BINARY;
        }
      } else {
        if (otype == YTType.EMBEDDEDLIST || otype == YTType.LINKLIST) {
          Object value = currentRecord.getProperty(fieldName);
          if (value == null) {
            return Types.NULL;
          }
          if (value instanceof LinkList list) {
            // check if all the list items are instances of YTRecordBytes
            ListIterator<YTIdentifiable> iterator = list.listIterator();
            YTIdentifiable listElement;
            boolean stop = false;
            while (iterator.hasNext() && !stop) {
              listElement = iterator.next();
              if (!(listElement instanceof YTBlob)) {
                stop = true;
              }
            }
            if (stop) {
              return typesSqlTypes.get(otype);
            } else {
              return Types.BLOB;
            }
          }
        }
      }
    }
    return typesSqlTypes.get(otype);
  }

  protected YTResult getCurrentRecord() throws SQLException {
    final YTResult currentRecord = resultSet.unwrap(YTResult.class);
    if (currentRecord == null) {
      throw new SQLException("No current record");
    }
    return currentRecord;
  }

  private int getSQLTypeFromJavaClass(final Object value) {
    if (value instanceof Boolean) {
      return typesSqlTypes.get(YTType.BOOLEAN);
    } else if (value instanceof Byte) {
      return typesSqlTypes.get(YTType.BYTE);
    } else if (value instanceof Date) {
      return typesSqlTypes.get(YTType.DATETIME);
    } else if (value instanceof Double) {
      return typesSqlTypes.get(YTType.DOUBLE);
    } else if (value instanceof BigDecimal) {
      return typesSqlTypes.get(YTType.DECIMAL);
    } else if (value instanceof Float) {
      return typesSqlTypes.get(YTType.FLOAT);
    } else if (value instanceof Integer) {
      return typesSqlTypes.get(YTType.INTEGER);
    } else if (value instanceof Long) {
      return typesSqlTypes.get(YTType.LONG);
    } else if (value instanceof Short) {
      return typesSqlTypes.get(YTType.SHORT);
    } else if (value instanceof String) {
      return typesSqlTypes.get(YTType.STRING);
    } else if (value instanceof List) {
      return typesSqlTypes.get(YTType.EMBEDDEDLIST);
    } else {
      return Types.JAVA_OBJECT;
    }
  }

  @Override
  public String getColumnTypeName(final int column) throws SQLException {
    final YTResult currentRecord = getCurrentRecord();

    String columnLabel = fieldNames[column - 1];

    return currentRecord
        .toEntity()
        .getSchemaType()
        .map(st -> st.getProperty(columnLabel))
        .map(p -> p.getType())
        .map(t -> t.toString())
        .orElse(null);
  }

  public int getPrecision(final int column) throws SQLException {
    return 0;
  }

  public int getScale(final int column) throws SQLException {
    return 0;
  }

  public String getSchemaName(final int column) throws SQLException {
    final YTResult currentRecord = getCurrentRecord();
    if (currentRecord == null) {
      return "";
    } else {
      return ((YTEntityImpl) currentRecord.toEntity()).getSession().getName();
    }
  }

  public String getTableName(final int column) throws SQLException {
    final YTProperty p = getProperty(column);
    return p != null ? p.getOwnerClass().getName() : null;
  }

  public boolean isAutoIncrement(final int column) throws SQLException {
    return false;
  }

  public boolean isCaseSensitive(final int column) throws SQLException {
    final YTProperty p = getProperty(column);
    return p != null && p.getCollate().getName().equalsIgnoreCase("ci");
  }

  public boolean isCurrency(final int column) throws SQLException {

    return false;
  }

  public boolean isDefinitelyWritable(final int column) throws SQLException {

    return false;
  }

  public int isNullable(final int column) throws SQLException {
    return columnNullableUnknown;
  }

  public boolean isReadOnly(final int column) throws SQLException {
    final YTProperty p = getProperty(column);
    return p != null && p.isReadonly();
  }

  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  public boolean isSigned(final int column) throws SQLException {
    final YTResult currentRecord = getCurrentRecord();
    YTType otype =
        currentRecord
            .toEntity()
            .getSchemaType()
            .map(st -> st.getProperty(fieldNames[column - 1]).getType())
            .orElse(null);

    return this.isANumericColumn(otype);
  }

  public boolean isWritable(final int column) throws SQLException {
    return !isReadOnly(column);
  }

  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return false;
  }

  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return null;
  }

  private boolean isANumericColumn(final YTType type) {
    return type == YTType.BYTE
        || type == YTType.DOUBLE
        || type == YTType.FLOAT
        || type == YTType.INTEGER
        || type == YTType.LONG
        || type == YTType.SHORT;
  }

  protected YTProperty getProperty(final int column) throws SQLException {

    String fieldName = getColumnName(column);

    return getCurrentRecord()
        .toEntity()
        .getSchemaType()
        .map(st -> st.getProperty(fieldName))
        .orElse(null);
  }
}
