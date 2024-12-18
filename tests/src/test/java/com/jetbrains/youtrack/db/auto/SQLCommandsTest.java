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

import com.jetbrains.youtrack.db.internal.core.command.script.CommandScript;
import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.cache.local.WOWCache;
import com.jetbrains.youtrack.db.internal.core.storage.cluster.ClusterPositionMap;
import com.jetbrains.youtrack.db.internal.core.storage.cluster.PaginatedCluster;
import com.jetbrains.youtrack.db.internal.core.storage.disk.LocalPaginatedStorage;
import java.io.File;
import java.util.Collection;
import java.util.Locale;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLCommandsTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SQLCommandsTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void createProperty() {
    Schema schema = db.getMetadata().getSchema();
    if (!schema.existsClass("account")) {
      schema.createClass("account");
    }

    db.command("create property account.timesheet string").close();

    Assert.assertEquals(
        db.getMetadata().getSchema().getClass("account").getProperty("timesheet").getType(),
        PropertyType.STRING);
  }

  @Test(dependsOnMethods = "createProperty")
  public void createLinkedClassProperty() {
    db.command("create property account.knows embeddedmap account").close();

    Assert.assertEquals(
        db.getMetadata().getSchema().getClass("account").getProperty("knows").getType(),
        PropertyType.EMBEDDEDMAP);
    Assert.assertEquals(
        db
            .getMetadata()
            .getSchema()
            .getClass("account")
            .getProperty("knows")
            .getLinkedClass(),
        db.getMetadata().getSchema().getClass("account"));
  }

  @Test(dependsOnMethods = "createLinkedClassProperty")
  public void createLinkedTypeProperty() {
    db.command("create property account.tags embeddedlist string").close();

    Assert.assertEquals(
        db.getMetadata().getSchema().getClass("account").getProperty("tags").getType(),
        PropertyType.EMBEDDEDLIST);
    Assert.assertEquals(
        db.getMetadata().getSchema().getClass("account").getProperty("tags").getLinkedType(),
        PropertyType.STRING);
  }

  @Test(dependsOnMethods = "createLinkedTypeProperty")
  public void removeProperty() {
    db.command("drop property account.timesheet").close();
    db.command("drop property account.tags").close();

    Assert.assertFalse(
        db.getMetadata().getSchema().getClass("account").existsProperty("timesheet"));
    Assert.assertFalse(
        db.getMetadata().getSchema().getClass("account").existsProperty("tags"));
  }

  @Test(dependsOnMethods = "removeProperty")
  public void testSQLScript() {
    String cmd = "";
    cmd += "select from ouser limit 1;begin;";
    cmd += "let a = create vertex set script = true\n";
    cmd += "let b = select from v limit 1;";
    cmd += "create edge from $a to $b;";
    cmd += "commit;";
    cmd += "return $a;";

    Object result = db.command(new CommandScript("sql", cmd)).execute(db);

    Assert.assertTrue(result instanceof Identifiable);
    Assert.assertTrue(((Identifiable) result).getRecord(db) instanceof EntityImpl);
    Assert.assertTrue(
        db.bindToSession((EntityImpl) ((Identifiable) result).getRecord(db))
            .field("script"));
  }

  public void testClusterRename() {
    if (db.getURL().startsWith("memory:")) {
      return;
    }

    Collection<String> names = db.getClusterNames();
    Assert.assertFalse(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    db.command("create cluster testClusterRename").close();

    names = db.getClusterNames();
    Assert.assertTrue(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    db.command("alter cluster testClusterRename name testClusterRename42").close();
    names = db.getClusterNames();

    Assert.assertTrue(names.contains("testClusterRename42".toLowerCase(Locale.ENGLISH)));
    Assert.assertFalse(names.contains("testClusterRename".toLowerCase(Locale.ENGLISH)));

    if (!remoteDB && databaseType.equals(DatabaseType.PLOCAL)) {
      String storagePath = db.getStorage().getConfiguration().getDirectory();

      final WOWCache wowCache =
          (WOWCache) ((LocalPaginatedStorage) db.getStorage()).getWriteCache();

      File dataFile =
          new File(
              storagePath,
              wowCache.nativeFileNameById(
                  wowCache.fileIdByName("testClusterRename42" + PaginatedCluster.DEF_EXTENSION)));
      File mapFile =
          new File(
              storagePath,
              wowCache.nativeFileNameById(
                  wowCache.fileIdByName(
                      "testClusterRename42" + ClusterPositionMap.DEF_EXTENSION)));

      Assert.assertTrue(dataFile.exists());
      Assert.assertTrue(mapFile.exists());
    }
  }
}
