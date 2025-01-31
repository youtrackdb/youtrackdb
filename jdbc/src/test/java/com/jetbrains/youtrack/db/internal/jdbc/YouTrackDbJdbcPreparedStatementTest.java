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
package com.jetbrains.youtrack.db.internal.jdbc;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Test;

public class YouTrackDbJdbcPreparedStatementTest extends YouTrackDbJdbcDbPerMethodTemplateTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    conn.createStatement().execute("begin");
    var stmt =
        conn.prepareStatement("SELECT * FROM Item WHERE stringKey = ? OR intKey = ?");
    assertThat(stmt).isNotNull();
    stmt.close();
    assertThat(stmt.isClosed()).isTrue();
    conn.createStatement().execute("commit");
  }

  @Test
  public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
    var stmt = conn.prepareStatement("");
    assertThat(stmt.execute("")).isFalse();

    assertThat(stmt.getResultSet()).isNull();
    assertThat(stmt.getMoreResults()).isFalse();
  }

  @Test
  public void shouldExecuteSelectOne() throws SQLException {
    var stmt = conn.prepareStatement("select 1");
    assertThat(stmt.execute()).isTrue();
    assertThat(stmt.getResultSet()).isNotNull();
    var resultSet = stmt.getResultSet();
    resultSet.first();
    var one = resultSet.getInt("1");
    assertThat(one).isEqualTo(1);
    assertThat(stmt.getMoreResults()).isFalse();
  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsInserted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");

    conn.createStatement().execute("begin");
    var statement = conn.prepareStatement("INSERT INTO Insertable ( id ) VALUES (?)");
    statement.setString(1, "testval");
    var rowsInserted = statement.executeUpdate();
    conn.createStatement().execute("commit");

    assertThat(rowsInserted).isEqualTo(1);
  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsInsertedWhenMultipleInserted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");
    conn.createStatement().execute("begin");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1)");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(2)");

    var statement = conn.prepareStatement("UPDATE Insertable SET id = ?");
    statement.setString(1, "testval");
    var rowsInserted = statement.executeUpdate();

    conn.createStatement().execute("commit");

    assertThat(rowsInserted).isEqualTo(2);
  }

  @Test
  public void testInsertRIDReturning() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");

    conn.createStatement().execute("begin");
    var result =
        conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1) return @rid");
    conn.createStatement().execute("commit");

    assertThat(result.next()).isTrue();
    assertThat(result.getObject("@rid")).isNotNull();
  }

  @Test
  public void testExecuteUpdateReturnsNumberOfRowsDeleted() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS Insertable ");

    conn.createStatement().execute("begin");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(1)");
    conn.createStatement().executeQuery("INSERT INTO Insertable(id) VALUES(2)");
    conn.createStatement().execute("commit");

    conn.createStatement().execute("begin");
    var statement = conn.prepareStatement("DELETE FROM Insertable WHERE id > ?");
    statement.setInt(1, 0);
    var rowsDeleted = statement.executeUpdate();
    conn.createStatement().execute("commit");

    assertThat(rowsDeleted).isEqualTo(2);
  }

  @Test
  public void shouldExecutePreparedStatement() throws Exception {
    var stmt =
        conn.prepareStatement("SELECT  " + "FROM Item " + "WHERE stringKey = ? OR intKey = ?");

    assertThat(stmt).isNotNull();
    stmt.setString(1, "1");
    stmt.setInt(2, 1);

    var rs = stmt.executeQuery();
    assertThat(rs.next()).isTrue();

    // assertThat(rs.getInt("@version"), equalTo(0));

    assertThat(rs.getString("@class")).isEqualToIgnoringCase("Item");

    assertThat(rs.getString("stringKey")).isEqualTo("1");
    assertThat(rs.getInt("intKey")).isEqualTo(1);
    //
    // assertThat(rs.getDate("date").toString(), equalTo(new
    // java.sql.Date(System.currentTimeMillis()).toString()));
    // assertThat(rs.getDate("time").toString(), equalTo(new
    // java.sql.Date(System.currentTimeMillis()).toString()));

    stmt.close();
    assertThat(stmt.isClosed()).isTrue();
  }

  @Test
  public void shouldExecutePreparedStatementWithExecuteMethod() throws Exception {
    conn.createStatement().executeQuery("CREATE CLASS insertable");

    conn.createStatement().execute("begin");
    var stmt = conn.prepareStatement("INSERT INTO insertable SET id = ?, number = ?");
    stmt.setString(1, "someRandomUid");
    stmt.setInt(2, 42);
    stmt.execute();
    stmt.close();
    conn.createStatement().execute("commit");

    // Let's verify the previous process
    var resultSet =
        conn.createStatement()
            .executeQuery("SELECT count(*) AS num FROM insertable WHERE id = 'someRandomUid'");
    assertThat(resultSet.getLong(1)).isEqualTo(1);

    // without alias!
    resultSet =
        conn.createStatement()
            .executeQuery("SELECT count(*) FROM insertable WHERE id = 'someRandomUid'");
    assertThat(resultSet.getLong(1)).isEqualTo(1);
  }

  @Test
  public void shouldCreatePreparedStatementWithExtendConstructor() throws Exception {

    var stmt =
        conn.prepareStatement(
            "SELECT * FROM Item WHERE intKey = ?", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    stmt.setInt(1, 1);

    var rs = stmt.executeQuery();

    assertThat(rs.next()).isTrue();

    assertThat(rs.getString("@class")).isEqualToIgnoringCase("Item");

    assertThat(rs.getString("stringKey")).isEqualTo("1");
    assertThat(rs.getInt("intKey")).isEqualTo(1);
    //
  }

  @Test
  public void shouldCreatePreparedStatementWithExtendConstructorWithOutProjection()
      throws Exception {
    // same test as above, no projection at all
    var stmt =
        conn.prepareStatement(
            "SELECT FROM Item WHERE intKey = ?", TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    stmt.setInt(1, 1);

    var rs = stmt.executeQuery();

    assertThat(rs.next()).isTrue();

    assertThat(rs.getString("@class")).isEqualToIgnoringCase("Item");

    assertThat(rs.getString("stringKey")).isEqualTo("1");
    assertThat(rs.getInt("intKey")).isEqualTo(1);
    //
  }

  @Test(expected = SQLException.class)
  public void shouldThrowSqlExceptionOnError() throws SQLException {

    var query = "select sequence('?').next()";
    var stmt = conn.prepareStatement(query);
    stmt.setString(1, "theSequence");
    stmt.executeQuery();
  }
}
