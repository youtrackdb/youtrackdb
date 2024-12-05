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

import static java.lang.Boolean.parseBoolean;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTQueryParsingException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTInternalResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 *
 */
public class OrientJdbcStatement implements Statement {

  protected final OrientJdbcConnection connection;
  protected final YTDatabaseSessionInternal database;
  protected final List<String> batches;
  protected final int resultSetType;
  protected final int resultSetConcurrency;
  protected final int resultSetHoldability;
  protected final Properties info;
  //   protected CommandSQL               sql;
  protected String sql;
  //  protected       List<EntityImpl>      documents;
  protected boolean closed;
  protected YTResultSet oResultSet;
  protected OrientJdbcResultSet resultSet;

  public OrientJdbcStatement(final OrientJdbcConnection iConnection) {
    this(
        iConnection,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
  }

  /**
   * @param iConnection
   * @param resultSetType
   * @param resultSetConcurrency
   * @throws SQLException
   */
  public OrientJdbcStatement(
      OrientJdbcConnection iConnection, int resultSetType, int resultSetConcurrency) {
    this(iConnection, resultSetType, resultSetConcurrency, resultSetType);
  }

  /**
   * @param iConnection
   * @param resultSetType
   * @param resultSetConcurrency
   * @param resultSetHoldability
   */
  public OrientJdbcStatement(
      OrientJdbcConnection iConnection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) {
    this.connection = iConnection;
    this.database = (YTDatabaseSessionInternal) iConnection.getDatabase();
    database.activateOnCurrentThread();
    batches = new ArrayList<>();
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;
    info = connection.getInfo();
  }

  @Override
  public boolean execute(final String sqlCommand) throws SQLException {

    if ("".equals(sqlCommand)) {
      return false;
    }

    sql = mayCleanForSpark(sqlCommand);

    if (sql.equalsIgnoreCase("select 1")) {
      YTResultInternal element = new YTResultInternal(database);
      element.setProperty("1", 1);
      YTInternalResultSet rs = new YTInternalResultSet();
      rs.add(element);
      oResultSet = rs;
    } else {
      try {

        oResultSet = executeCommand(sql);

      } catch (YTQueryParsingException e) {
        throw new SQLSyntaxErrorException("Error while parsing query", e);
      } catch (YTException e) {
        throw new SQLException("Error while executing query", e);
      }
    }

    resultSet =
        new OrientJdbcResultSet(
            this, oResultSet, resultSetType, resultSetConcurrency, resultSetHoldability);
    return true;
  }

  public ResultSet executeQuery(final String sql) throws SQLException {
    if (execute(sql)) {
      return resultSet;
    } else {
      return null;
    }
  }

  @Override
  public int executeUpdate(final String sql) throws SQLException {
    return doExecuteUpdate(sql);
  }

  private int doExecuteUpdate(String sql) throws SQLException {
    try {
      oResultSet = executeCommand(sql);
      Optional<YTResult> res = oResultSet.stream().findFirst();

      if (res.isPresent()) {
        if (res.get().getProperty("count") != null) {
          return Math.toIntExact(res.get().getProperty("count"));
        } else {
          return 1;
        }
      } else {
        return 0;
      }
    } finally {
      if (oResultSet != null) {
        oResultSet.close();
      }
    }
  }

  protected YTResultSet executeCommand(String query) throws SQLException {

    try {
      return database.command(query);
    } catch (YTQueryParsingException e) {
      throw new SQLSyntaxErrorException("Error while parsing command", e);
    } catch (YTException e) {
      throw new SQLException("Error while executing command", e);
    }
  }

  public int executeUpdate(final String sql, int autoGeneratedKeys) throws SQLException {
    return 0;
  }

  public int executeUpdate(final String sql, int[] columnIndexes) throws SQLException {
    return 0;
  }

  public int executeUpdate(final String sql, String[] columnNames) throws SQLException {
    return 0;
  }

  public Connection getConnection() throws SQLException {
    return connection;
  }

  public void close() throws SQLException {
    if (connection.getAutoCommit() && database.getTransaction().isActive()) {
      database.commit();
    }

    closed = true;
  }

  public boolean execute(final String sql, int autoGeneratedKeys) throws SQLException {
    return false;
  }

  public boolean execute(final String sql, int[] columnIndexes) throws SQLException {
    return false;
  }

  public boolean execute(final String sql, String[] columnNames) throws SQLException {
    return false;
  }

  public void addBatch(final String sql) throws SQLException {
    batches.add(sql);
  }

  public void cancel() throws SQLException {
  }

  public void clearBatch() throws SQLException {
    batches.clear();
  }

  public void clearWarnings() throws SQLException {
  }

  public int[] executeBatch() throws SQLException {
    int[] results = new int[batches.size()];
    int i = 0;
    for (String sql : batches) {
      results[i++] = doExecuteUpdate(sql);
    }
    return results;
  }

  public int getFetchDirection() throws SQLException {

    return 0;
  }

  public void setFetchDirection(final int direction) throws SQLException {
  }

  public int getFetchSize() throws SQLException {

    return 0;
  }

  public void setFetchSize(final int rows) throws SQLException {
  }

  public ResultSet getGeneratedKeys() throws SQLException {

    return null;
  }

  public int getMaxFieldSize() throws SQLException {

    return 0;
  }

  public void setMaxFieldSize(final int max) throws SQLException {
  }

  public int getMaxRows() throws SQLException {

    return 0;
  }

  public void setMaxRows(final int max) throws SQLException {
  }

  public boolean getMoreResults() throws SQLException {

    return false;
  }

  public boolean getMoreResults(final int current) throws SQLException {

    return false;
  }

  public int getQueryTimeout() throws SQLException {

    return 0;
  }

  public void setQueryTimeout(final int seconds) throws SQLException {
  }

  public ResultSet getResultSet() throws SQLException {

    return resultSet;
  }

  public int getResultSetConcurrency() throws SQLException {

    return resultSet.getConcurrency();
  }

  public int getResultSetHoldability() throws SQLException {

    return resultSet.getHoldability();
  }

  public int getResultSetType() throws SQLException {

    return resultSet.getType();
  }

  public int getUpdateCount() throws SQLException {
    if (closed) {
      throw new SQLException("Statement already closed");
    }

    return -1;
  }

  public SQLWarning getWarnings() throws SQLException {

    return null;
  }

  public boolean isClosed() throws SQLException {

    return closed;
  }

  public boolean isPoolable() throws SQLException {

    return false;
  }

  public void setPoolable(final boolean poolable) throws SQLException {
  }

  public void setCursorName(final String name) throws SQLException {
  }

  public void setEscapeProcessing(final boolean enable) throws SQLException {
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    try {
      // the following if-then structure makes sense if the query can be a
      // subclass of CommandSQL.

      if (this.sql != null) {
        return this.sql.getClass().isAssignableFrom(iface);
      } else {
        return false;
      }
    } catch (NullPointerException e) {
      throw new SQLException(e);
    }
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return iface.cast(sql);
    } catch (ClassCastException e) {
      throw new SQLException(e);
    }
  }

  public void closeOnCompletion() throws SQLException {
  }

  public boolean isCloseOnCompletion() throws SQLException {
    return false;
  }

  protected String mayCleanForSpark(String sql) {
    // SPARK support
    if (parseBoolean(info.getProperty("spark", "false"))) {
      if (sql.endsWith("WHERE 1=0")) {
        sql = sql.replace("WHERE 1=0", " LIMIT 1");
      }
      return sql.replace('"', ' ');
    }
    return sql;
  }
}
