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
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Security;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
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
public class SecurityTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SecurityTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    db.close();
  }

  public void testWrongPassword() throws IOException {
    try {
      db = createSessionInstance("reader", "swdsds");
    } catch (BaseException e) {
      Assert.assertTrue(
          e instanceof SecurityAccessException
              || e.getCause() != null
              && e.getCause()
              .toString()
              .contains("com.orientechnologies.core.exception.SecurityAccessException"));
    }
  }

  public void testSecurityAccessWriter() throws IOException {
    db = createSessionInstance("writer", "writer");

    try {
      db.begin();
      (new EntityImpl(db)).save("internal");
      db.commit();

      Assert.fail();
    } catch (SecurityAccessException e) {
      Assert.assertTrue(true);
    } finally {
      db.close();
    }
  }

  @Test
  public void testSecurityAccessReader() throws IOException {
    db = createSessionInstance("reader", "reader");

    try {
      db.createClassIfNotExist("Profile");

      db.begin();
      ((EntityImpl) db.newEntity("Profile"))
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
      db.commit();
    } catch (SecurityAccessException e) {
      Assert.assertTrue(true);
    } finally {
      db.close();
    }
  }

  @Test
  public void testEncryptPassword() throws IOException {
    db = createSessionInstance("admin", "admin");

    db.begin();
    Long updated =
        db
            .command("update ouser set password = 'test' where name = 'reader'")
            .next()
            .getProperty("count");
    db.commit();

    Assert.assertEquals(updated.intValue(), 1);

    db.begin();
    ResultSet result = db.query("select from ouser where name = 'reader'");
    Assert.assertNotEquals(result.next().getProperty("password"), "test");
    db.commit();

    // RESET OLD PASSWORD
    db.begin();
    updated =
        db
            .command("update ouser set password = 'reader' where name = 'reader'")
            .next()
            .getProperty("count");
    db.commit();
    Assert.assertEquals(updated.intValue(), 1);

    db.begin();
    result = db.query("select from ouser where name = 'reader'");
    Assert.assertNotEquals(result.next().getProperty("password"), "reader");
    db.commit();

    db.close();
  }

  public void testParentRole() {
    db = createSessionInstance("admin", "admin");

    db.begin();
    Security security = db.getMetadata().getSecurity();
    Role writer = security.getRole("writer");

    Role writerChild =
        security.createRole("writerChild", writer);
    writerChild.save(db);
    db.commit();

    try {
      db.begin();
      Role writerGrandChild =
          security.createRole(
              "writerGrandChild", writerChild);
      writerGrandChild.save(db);
      db.commit();

      try {
        db.begin();
        SecurityUserImpl child = security.createUser("writerChild", "writerChild",
            writerGrandChild);
        child.save(db);
        db.commit();

        try {
          db.begin();
          Assert.assertTrue(child.hasRole(db, "writer", true));
          Assert.assertFalse(child.hasRole(db, "wrter", true));
          db.commit();

          db.close();
          if (!db.isRemote()) {
            db = createSessionInstance("writerChild", "writerChild");

            db.begin();
            SecurityUser user = db.geCurrentUser();
            Assert.assertTrue(user.hasRole(db, "writer", true));
            Assert.assertFalse(user.hasRole(db, "wrter", true));
            db.commit();

            db.close();
          }
          db = createSessionInstance();
          security = db.getMetadata().getSecurity();
        } finally {
          db.begin();
          security.dropUser("writerChild");
          db.commit();
        }
      } finally {
        db.begin();
        security.dropRole("writerGrandChild");
        db.commit();
      }
    } finally {
      db.begin();
      security.dropRole("writerChild");
      db.commit();
    }
  }

  @Test
  public void testQuotedUserName() {
    db = createSessionInstance();

    db.begin();
    Security security = db.getMetadata().getSecurity();

    Role adminRole = security.getRole("admin");
    security.createUser("user'quoted", "foobar", adminRole);
    db.commit();
    db.close();

    db = createSessionInstance();
    db.begin();
    security = db.getMetadata().getSecurity();
    SecurityUserImpl user = security.getUser("user'quoted");
    Assert.assertNotNull(user);
    security.dropUser(user.getName(db));
    db.commit();
    db.close();

    try {
      db = createSessionInstance("user'quoted", "foobar");
      Assert.fail();
    } catch (Exception e) {

    }
  }

  @Test
  public void testUserNoRole() {
    db = createSessionInstance();

    Security security = db.getMetadata().getSecurity();

    db.begin();
    security.createUser("noRole", "noRole", (String[]) null);
    db.commit();

    db.close();

    try {
      db = createSessionInstance("noRole", "noRole");
      Assert.fail();
    } catch (SecurityAccessException e) {
      db = createSessionInstance();
      db.begin();
      security = db.getMetadata().getSecurity();
      security.dropUser("noRole");
      db.commit();
    }
  }

  @Test
  public void testAdminCanSeeSystemClusters() {
    db = createSessionInstance();

    db.begin();
    List<Result> result =
        db.command("select from ouser").stream().collect(Collectors.toList());
    Assert.assertFalse(result.isEmpty());
    db.commit();

    db.begin();
    Assert.assertTrue(db.browseClass("OUser").hasNext());
    db.commit();

    db.begin();
    Assert.assertTrue(db.browseCluster("OUser").hasNext());
    db.commit();
  }

  @Test
  @Ignore
  public void testOnlyAdminCanSeeSystemClusters() {
    db = createSessionInstance("reader", "reader");

    try {
      db.command(new CommandSQL("select from ouser")).execute(db);
    } catch (SecurityException e) {
    }

    try {
      Assert.assertFalse(db.browseClass("OUser").hasNext());
      Assert.fail();
    } catch (SecurityException e) {
    }

    try {
      Assert.assertFalse(db.browseCluster("OUser").hasNext());
      Assert.fail();
    } catch (SecurityException e) {
    }
  }

  @Test
  public void testCannotExtendClassWithNoUpdateProvileges() {
    db = createSessionInstance();
    db.getMetadata().getSchema().createClass("Protected");
    db.close();

    db = createSessionInstance("writer", "writer");

    try {
      db.command(new CommandSQL("alter class Protected superclass OUser")).execute(db);
      Assert.fail();
    } catch (SecurityException e) {
    } finally {
      db.close();

      db = createSessionInstance();
      db.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testSuperUserCanExtendClassWithNoUpdateProvileges() {
    db = createSessionInstance();
    db.getMetadata().getSchema().createClass("Protected");

    try {
      db.command("alter class Protected superclass OUser").close();
    } finally {
      db.getMetadata().getSchema().dropClass("Protected");
    }
  }

  @Test
  public void testEmptyUserName() {
    db = createSessionInstance();
    try {
      Security security = db.getMetadata().getSecurity();
      String userName = "";
      try {
        db.begin();
        Role reader = security.getRole("reader");
        security.createUser(userName, "foobar", reader);
        db.commit();
        Assert.fail();
      } catch (ValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      db.close();
    }
  }

  @Test
  public void testUserNameWithAllSpaces() {
    db = createSessionInstance();
    try {
      Security security = db.getMetadata().getSecurity();

      db.begin();
      Role reader = security.getRole("reader");
      db.commit();
      final String userName = "  ";
      try {
        db.begin();
        security.createUser(userName, "foobar", reader);
        db.commit();
        Assert.fail();
      } catch (ValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      db.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesOne() {
    db = createSessionInstance();
    try {
      Security security = db.getMetadata().getSecurity();

      db.begin();
      Role reader = security.getRole("reader");
      db.commit();
      final String userName = " sas";
      try {
        db.begin();
        security.createUser(userName, "foobar", reader);
        db.commit();
        Assert.fail();
      } catch (ValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      db.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesTwo() {
    db = createSessionInstance();
    try {
      Security security = db.getMetadata().getSecurity();

      db.begin();
      Role reader = security.getRole("reader");
      final String userName = "sas ";
      try {
        security.createUser(userName, "foobar", reader);
        db.commit();
        Assert.fail();
      } catch (ValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      db.close();
    }
  }

  @Test
  public void testUserNameWithSurroundingSpacesThree() {
    db = createSessionInstance();
    try {
      Security security = db.getMetadata().getSecurity();

      db.begin();
      Role reader = security.getRole("reader");
      db.commit();
      final String userName = " sas ";
      try {
        db.begin();
        security.createUser(userName, "foobar", reader);
        db.commit();
        Assert.fail();
      } catch (ValidationException ve) {
        Assert.assertTrue(true);
      }
      Assert.assertNull(security.getUser(userName));
    } finally {
      db.close();
    }
  }

  @Test
  public void testUserNameWithSpacesInTheMiddle() {
    db = createSessionInstance();
    try {
      Security security = db.getMetadata().getSecurity();

      db.begin();
      Role reader = security.getRole("reader");
      db.commit();
      final String userName = "s a s";
      db.begin();
      security.createUser(userName, "foobar", reader);
      db.commit();
      db.begin();
      Assert.assertNotNull(security.getUser(userName));
      security.dropUser(userName);
      db.commit();
      db.begin();
      Assert.assertNull(security.getUser(userName));
      db.commit();
    } finally {
      db.close();
    }
  }
}
