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

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class YouTrackDbJdbcStatementTest extends YouTrackDbJdbcDbPerClassTemplateTest {

  @Test
  public void shouldCreateStatement() throws Exception {
    var stmt = conn.createStatement();
    assertThat(stmt).isNotNull();
    stmt.close();
    assertThat(stmt.isClosed()).isTrue();
  }

  @Test
  public void shouldReturnEmptyResultSetOnEmptyQuery() throws SQLException {
    var stmt = conn.createStatement();

    assertThat(stmt.execute("")).isFalse();
    assertThat(stmt.getResultSet()).isNull();
    assertThat(stmt.getMoreResults()).isFalse();
  }

  @Test
  public void shouldExectuteSelectOne() throws SQLException {

    var st = conn.createStatement();
    assertThat(st.execute("select 1")).isTrue();
    assertThat(st.getResultSet()).isNotNull();
    var resultSet = st.getResultSet();
    resultSet.first();
    assertThat(resultSet.getInt("1")).isEqualTo(1);
    assertThat(st.getMoreResults()).isFalse();
  }

  @Test(expected = SQLException.class)
  public void shouldThrowSqlExceptionOnError() throws SQLException {

    var query = String.format("select sequence('%s').next()", "theSequence");
    var stmt = conn.createStatement();
    stmt.executeQuery(query);
  }
}
