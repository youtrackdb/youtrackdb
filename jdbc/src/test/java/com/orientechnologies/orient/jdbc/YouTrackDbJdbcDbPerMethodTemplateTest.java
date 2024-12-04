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

import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.createSchemaDB;
import static com.orientechnologies.orient.jdbc.OrientDbCreationHelper.loadDB;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import java.io.File;
import java.util.Properties;
import javax.sql.DataSource;
import org.assertj.db.type.DataSourceWithLetterCase;
import org.assertj.db.type.lettercase.LetterCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

public abstract class YouTrackDbJdbcDbPerMethodTemplateTest {

  @Rule
  public TestName name = new TestName();

  protected OrientJdbcConnection conn;
  protected ODatabaseSessionInternal db;
  protected YouTrackDB youTrackDB;
  protected DataSource ds;

  @Before
  public void prepareDatabase() throws Exception {

    String dbName = name.getMethodName();
    Properties info = new Properties();
    info.put("user", "admin");
    info.put("password", "admin");
    info.put("serverUser", "admin");
    info.put("serverPassword", "admin");

    OrientDataSource ods =
        new OrientDataSource("jdbc:orient:" + "memory:" + dbName, "admin", "admin", info);
    ds =
        new DataSourceWithLetterCase(
            ods, LetterCase.TABLE_DEFAULT, LetterCase.TABLE_DEFAULT, LetterCase.TABLE_DEFAULT);
    conn = (OrientJdbcConnection) ds.getConnection();
    youTrackDB = conn.getOrientDB();

    db = (ODatabaseSessionInternal) ((OrientJdbcConnection) ds.getConnection()).getDatabase();

    createSchemaDB(db);

    if (!new File("./src/test/resources/file.pdf").exists()) {
      OLogManager.instance().warn(this, "attachment will be not loaded!");
    }

    loadDB(db, 20);

    db.close();
  }

  @After
  public void closeConnection() throws Exception {
    if (conn != null && !conn.isClosed()) {
      conn.close();
    }
    youTrackDB.close();
  }
}
