/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilder;
import com.jetbrains.youtrack.db.internal.core.exception.YTSecurityAccessException;
import com.jetbrains.youtrack.db.internal.core.exception.YTSecurityException;
import com.jetbrains.youtrack.db.internal.core.exception.YTValidationException;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurity;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityRole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTSecurityUser;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTUser;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SecurityTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public SecurityTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    return builder.build();
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    database.close();
  }

  public void testWrongPassword() throws IOException {
    try {
      database = createSessionInstance("reader", "swdsds");
    } catch (YTException e) {
      Assert.assertTrue(
          e instanceof YTSecurityAccessException
              || e.getCause() != null
              && e.getCause()
              .toString()
              .contains("com.orientechnologies.core.exception.YTSecurityAccessException"));
    }
  }

  public void testSecurityAccessWriter() throws IOException {
    database = createSessionInstance("writer", "writer");

    try {
      database.begin();
      new EntityImpl().save("internal");
      database.commit();

      Assert.fail();
    } catch (YTSecurityAccessException e) {
      Assert.assertTrue(true);
    } finally {
      database.close();
    }
  }

  @Test
  public void testSecurityAccessReader() throws IOException {
    database = createSessionInstance("reader", "reader");

    try {
      database.createClassIfNotExist("Profile");

      database.begin();
      new EntityImpl("Profile")
          .fields(
              "nick",
              "error",
              "password",
              "I don't know",
              "lastAccessOn",
              new Date(),
              "registeredOn",
              new Date())
          .save();
      database.commit();
    } catch (YTSecurityAccessException e) {
      Assert.assertTrue(true);
    } finally {
      database.close();
    }
  }

  @Test
  public void testEncryptPassword() throws IOException {
    database = createSessionInstance("admin", "admin");

    database.begin();
    Long updated =
        database
            .command("update ouser set password = 'test' where name = 'reader'")
            .next()
            .getProperty("count");
    database.commit();

    Assert.assertEquals(updated.intValue(), 1);

    database.begin();
    YTResultSet result = database.query("select from ouser where name = 'reader'");
    Assert.assertNotEquals(result.next().getProperty("password"), "test");
    database.commit();

    // RESET OLD PASSWORD
    database.begin();
    updated =
        database
            .command("update ouser set password = 'reader' where name = 'reader'")
            .next()
            .getProperty("count");
    database.commit();
    Assert.assertEquals(updated.intValue(), 1);

    database.begin();
    result = database.query("select from ouser where name = 'reader'");
    Assert.assertNotEquals(result.next().getProperty("password"), "reader");
    database.commit();

    database.close();
  }

  public void testParentRole() {
    database = createSessionInstance("admin", "admin");

    database.begin();
    OSecurity security = database.getMetadata().getSecurity();
    ORole writer = security.getRole("writer");

    ORole writerChild =
        security.createRole("writerChild", writer, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
    writerChild.save(database);
    database.commit();

    try {
      database.begin();
      ORole writerGrandChild =
          security.createRole(
              "writerGrandChild", writerChild, OSecurityRole.ALLOW_MODES.ALLOW_ALL_BUT);
      writerGrandChild.save(database);
      database.commit();

      try {
        database.begin();
        YTUser child = security.createUser("writerChild", "writerChild", writerGrandChild);
        child.save(database);
        database.commit();

        try {
          database.begin();
          Assert.assertTrue(child.hasRole(database, "writer", true));
          Assert.assertFalse(child.hasRole(database, "wrter", true));
          database.commit();

          database.close();
          if (!database.isRemote()) {
            database = createSessionInstance("writerChild", "writerChild");

            database.begin();
            YTSecurityUser user = database.getUser();
            Assert.assertTrue(user.hasRole(database, "writer", true));
            Assert.assertFalse(user.hasRole(database, "wrter", true));
            database.commit();

            database.close();
          }
          database = createSessionInstance();
          security = database.getMetadata().getSecurity();
        } finally {
          database.begin();
          security.dropUser("writerChild");
          database.commit();
        }
      } finally {
        database.begin();
        security.dropRole("writerGrandChild");
        database.commit();
      }
    } finally {
      database.begin();
      security.dropRole("writerChild");
      database.commit();
    }
  }

  @Test
  public void testQuotedUserName() {
    database = createSessionInstance();

    database.begin();
    OSecurity security = database.getMetadata().getSecurity();

    ORole adminRole = security.getRole("admin");
    security.createUser("user'quoted", "foobar", adminRole);
    database.commit();
    database.close();

    database = createSessionInstance();
    database.begin();
    security = database.getMetadata().getSecurity();
    YTUser user = security.getUser("user'quoted");
    Assert.assertNotNull(user);
    security.dropUser(user.getName(database));
    database.commit();
    database.close();

    try {
      database = createSessionInstance("user'quoted", "foobar");
      Assert.fail();
    } catch (Exception e) {

    }
  }

  @Test
  public void testUserNoRole() {
    database = createSessionInstance();

    OSecurity security = database.getMetadata().getSecurity();

    database.begin();
    security.createUser("noRole", "noRole", (String[]) null);
    database.commit();

    database.close();

    try {
      database = createSessionInstance("noRole", "noRole");
      Assert.fail();
    } catch (YTSecurityAccessException e) {
      database = createSessionInstance();
      database.begin();
      security = database.getMetadata().getSecurity();
      security.dropUser("noRole");
      database.commit();
    }
  }

  @Test
  public void testAdminCanSeeSystemClusters() {
    database = createSessionInstance();

    database.begin();
    List<YTResult> result =
        database.command("select from ouser").stream().collect(Collectors.toList());
    Assert.assertFalse(result.isEmpty());
    database.commit();

    database.begin();
    Assert.assertTrue(database.browseClass("OUser").hasNext());
    database.commit();

    database.begin();
    Assert.assertTrue(database.browseCluster("OUser").hasNext());
    database.commit();
  }

  @Test
  @Ignore
  public void testOnlyAdminCanSeeSystemClusters() {
    database = createSessionInstance("reader", "reader");

    try {
      database.command(new CommandSQL("select from ouser")).execute(database);
    } catch (YTSecurityException e) {
    }

    try {
      Assert.assertFalse(database.browseClass("OUser").hasNext());
      Assert.fail();
    } catch (YTSecurityException e) {
    }

    try {
      Assert.assertFalse(database.browseCluster("OUser").hasNext());
      Assert.fail();
    } catch (YTSecurityException e) {
    }
  }

  @Test
  public void testCannotExtendClassWithNoUpdateProvileges() {
    database = createSessionInstance();
    database.getMetadata().getSchema().createClass("Protected");
    database.close();

    database = createSessionInstance("writer", "writer");

    try {
      database.command(new CommandSQL("alter class Protected superclass OUser")).execute(database);
      Assert.fail();
    } catch (YTSecurityException e) {
    } finally {
      database.close();

      database = createSessionInstance();
      database.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testSuperUserCanExtendClassWithNoUpdateProvileges() {
    database = createSessionInstance();
    database.getMetadata().getSchema().createClass("Protected");

    try {
      database.command("alter class Protected superclass OUser").close();
    } finally {
      database.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testEmptyUserName() {
    database = createSessionInstance();
    try {
      OSecurity security = database.getMetadata().getSecurity();
      String userName = "";
      try {
        database.begin();
        ORole reader = security.getRole("reader");
        security.createUser(userName, "foobar", reader);
        database.commit();
        Assert.fail();
      } catch (YTValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithAllSpaces() {
    database = createSessionInstance();
    try {
      OSecurity security = database.getMetadata().getSecurity();

      database.begin();
      ORole reader = security.getRole("reader");
      database.commit();
      final String userName = "  ";
      try {
        database.begin();
        security.createUser(userName, "foobar", reader);
        database.commit();
        Assert.fail();
      } catch (YTValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesOne() {
    database = createSessionInstance();
    try {
      OSecurity security = database.getMetadata().getSecurity();

      database.begin();
      ORole reader = security.getRole("reader");
      database.commit();
      final String userName = " sas";
      try {
        database.begin();
        security.createUser(userName, "foobar", reader);
        database.commit();
        Assert.fail();
      } catch (YTValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesTwo() {
    database = createSessionInstance();
    try {
      OSecurity security = database.getMetadata().getSecurity();

      database.begin();
      ORole reader = security.getRole("reader");
      final String userName = "sas ";
      try {
        security.createUser(userName, "foobar", reader);
        database.commit();
        Assert.fail();
      } catch (YTValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesThree() {
    database = createSessionInstance();
    try {
      OSecurity security = database.getMetadata().getSecurity();

      database.begin();
      ORole reader = security.getRole("reader");
      database.commit();
      final String userName = " sas ";
      try {
        database.begin();
        security.createUser(userName, "foobar", reader);
        database.commit();
        Assert.fail();
      } catch (YTValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      database.close();
    }
  }

  @Test
  public void testUserNameWithSpacesInTheMiddle() {
    database = createSessionInstance();
    try {
      OSecurity security = database.getMetadata().getSecurity();

      database.begin();
      ORole reader = security.getRole("reader");
      database.commit();
      final String userName = "s a s";
      database.begin();
      security.createUser(userName, "foobar", reader);
      database.commit();
      database.begin();
      Assert.assertNotNull(security.getUser(userName));
      security.dropUser(userName);
      database.commit();
      database.begin();
      Assert.assertNull(security.getUser(userName));
      database.commit();
    } finally {
      database.close();
    }
  }
}
