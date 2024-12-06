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

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.Test;

public class YouTrackDbJdbcDriverTest {

  @Test
  public void shouldAcceptsWellFormattedURLOnly() throws ClassNotFoundException, SQLException {

    Driver drv = new YouTrackDbJdbcDriver();

    assertThat(
        drv.acceptsURL("jdbc:youtrackdb:local:./working/db/YouTrackDbJdbcDriverTest")).isTrue();
    assertThat(drv.acceptsURL("local:./working/db/YouTrackDbJdbcDriverTest")).isFalse();
  }

  @Test
  public void shouldConnect() throws SQLException {

    Properties info = new Properties();
    info.put("user", "admin");
    info.put("password", "admin");
    info.setProperty("serverUser", "root");
    info.setProperty("serverPassword", "root");

    YouTrackDbJdbcConnection conn =
        (YouTrackDbJdbcConnection)
            DriverManager.getConnection("jdbc:youtrackdb:memory:YouTrackDbJdbcDriverTest", info);

    assertThat(conn).isNotNull();
    conn.close();
    assertThat(conn.isClosed()).isTrue();
  }
}
