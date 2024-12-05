package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class CountRealationshipsTest {

  private static final String SERVER_DIRECTORY = "./target/cluster";
  private OServer server;
  private YouTrackDB youTrackDB;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(getClass().getResourceAsStream("orientdb-server-config-tree-ridbag.xml"));
    server.activate();

    youTrackDB = new YouTrackDB("remote:localhost", "root", "root",
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        CountRealationshipsTest.class.getSimpleName());
  }

  @Test
  public void test() throws Exception {
    YTDatabaseSession g =
        youTrackDB.open(CountRealationshipsTest.class.getSimpleName(), "admin", "admin");
    g.begin();
    Vertex vertex1 = g.newVertex("V");
    vertex1.save();
    Vertex vertex2 = g.newVertex("V");
    vertex2.save();
    g.commit();

    vertex1 = g.load(vertex1.getIdentity());
    vertex2 = g.load(vertex2.getIdentity());

    int version = vertex1.getProperty("@version");
    assertEquals(0, countEdges(vertex1, ODirection.OUT));
    assertEquals(0, countEdges(vertex1, ODirection.OUT));
    System.out.println("Version: " + version);
    System.out.println("vertex1 out: " + countEdges(vertex1, ODirection.OUT));
    System.out.println("vertex2 in: " + countEdges(vertex2, ODirection.IN));
    /*
     * output: Version: 1 vertex1 out: 0 vertex2 in: 0
     */

    g.begin();

    vertex2 = g.load(vertex2.getIdentity());
    vertex1 = g.load(vertex1.getIdentity());

    vertex1.addEdge(vertex2);
    vertex1.save();

    version = vertex1.getProperty("@version");
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    System.out.println("Pre-commit:");
    System.out.println("Version: " + version);
    System.out.println("vertex1 out: " + countEdges(vertex1, ODirection.OUT));
    System.out.println("vertex2 in: " + countEdges(vertex2, ODirection.IN));
    /*
     * output: Pre-commit: Version: 1 vertex1 out: 1 vertex2 in: 1
     */

    g.commit();

    vertex1 = g.load(vertex1.getIdentity());
    vertex2 = g.load(vertex2.getIdentity());

    version = vertex1.getProperty("@version");
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    System.out.println("Post-commit:");
    System.out.println("Version: " + version);
    System.out.println("vertex1 out: " + countEdges(vertex1, ODirection.OUT));
    System.out.println("vertex2 in: " + countEdges(vertex2, ODirection.IN));
    /*
     * output: Post-commit: Version: 2 vertex1 out: 0 <- INCORRECT vertex2 in: 0 <- INCORRECT
     */

    g.close();

    g = youTrackDB.open(CountRealationshipsTest.class.getSimpleName(), "admin", "admin");
    vertex1 = g.load(vertex1.getIdentity());
    vertex2 = g.load(vertex2.getIdentity());

    version = vertex1.getProperty("@version");
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    assertEquals(1, countEdges(vertex1, ODirection.OUT));
    System.out.println("Reload in new transaction:");
    System.out.println("Version: " + version);
    System.out.println("vertex1 out: " + countEdges(vertex1, ODirection.OUT));
    System.out.println("vertex2 in: " + countEdges(vertex2, ODirection.IN));
    /*
     * output: Reload in new transaction: Version: 2 vertex1 out: 1 vertex2 in: 1
     */
  }

  private int countEdges(Vertex v, ODirection dir) throws Exception {
    int c = 0;
    for (var oEdge : v.getEdges(dir)) {
      c++;
    }
    return c;
  }

  @After
  public void after() {
    youTrackDB.close();
    server.shutdown();
    YouTrackDBManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    YouTrackDBManager.instance().startup();
  }
}
