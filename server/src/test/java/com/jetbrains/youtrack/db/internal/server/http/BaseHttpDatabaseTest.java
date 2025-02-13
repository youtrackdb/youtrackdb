package com.jetbrains.youtrack.db.internal.server.http;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Test HTTP "query" command.
 */
public abstract class BaseHttpDatabaseTest extends BaseHttpTest {

  @Before
  public void createDatabase() throws Exception {
    serverDirectory =
        Paths.get(System.getProperty("buildDirectory", "target"))
            .resolve(this.getClass().getSimpleName() + "Server")
            .toFile()
            .getCanonicalPath();

    super.startServer();
    var pass = new EntityImpl(null);
    pass.setProperty("adminPassword", "admin");
    Assert.assertEquals(
        200,
        post("database/" + getDatabaseName() + "/memory")
            .payload(pass.toJSON(), CONTENT.JSON)
            .setUserName("root")
            .setUserPassword("root")
            .getResponse()
            .getCode());

    onAfterDatabaseCreated();
  }

  @After
  public void dropDatabase() throws Exception {
    Assert.assertEquals(
        204,
        delete("database/" + getDatabaseName())
            .setUserName("root")
            .setUserPassword("root")
            .getResponse()
            .getCode());
    super.stopServer();

    onAfterDatabaseDropped();
  }

  protected void onAfterDatabaseCreated() throws Exception {
  }

  protected void onAfterDatabaseDropped() throws Exception {
  }
}
