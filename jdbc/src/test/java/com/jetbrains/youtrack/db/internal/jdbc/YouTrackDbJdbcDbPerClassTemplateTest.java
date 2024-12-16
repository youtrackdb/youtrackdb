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

import static com.jetbrains.youtrack.db.internal.jdbc.YouTrackDbCreationHelper.createSchemaDB;
import static com.jetbrains.youtrack.db.internal.jdbc.YouTrackDbCreationHelper.loadDB;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import java.util.Properties;
import javax.sql.DataSource;
import org.assertj.db.type.DataSourceWithLetterCase;
import org.assertj.db.type.lettercase.LetterCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class YouTrackDbJdbcDbPerClassTemplateTest {

  protected static YouTrackDbJdbcConnection conn;
  protected static DatabaseSessionInternal db;
  protected static YouTrackDB youTrackDB;
  protected static DataSource ds;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void prepareDatabase() throws Exception {

    String dbName = "perClassTestDatabase";
    Properties info = new Properties();
    info.put("user", "admin");
    info.put("password", "admin");
    info.put("serverUser", "admin");
    info.put("serverPassword", "admin");

    YouTrackDbDataSource ods =
        new YouTrackDbDataSource("jdbc:youtrackdb:" + "memory:" + dbName, "admin", "admin", info);
    ds =
        new DataSourceWithLetterCase(
            ods, LetterCase.TABLE_DEFAULT, LetterCase.TABLE_DEFAULT, LetterCase.TABLE_DEFAULT);
    conn = (YouTrackDbJdbcConnection) ds.getConnection();
    youTrackDB = conn.getYouTrackDb();

    db = (DatabaseSessionInternal) ((YouTrackDbJdbcConnection) ds.getConnection()).getDatabase();

    createSchemaDB(db);

    //    if (!new File("./src/test/resources/file.pdf").exists())
    //      LogManager.instance().warn(, "attachment will be not loaded!");

    loadDB(db, 20);

    db.close();
  }

  @AfterClass
  public static void closeConnection() throws Exception {
    if (conn != null && !conn.isClosed()) {
      conn.close();
    }
    youTrackDB.close();
  }
}
