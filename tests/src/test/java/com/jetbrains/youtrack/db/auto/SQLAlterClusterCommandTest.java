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

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-cluster-alter")
public class SQLAlterClusterCommandTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SQLAlterClusterCommandTest(boolean remote) {
    super(remote);
  }

  @Test
  public void testCreateCluster() {
    var expectedClusters = db.getClusters();
    try {
      db.command("create cluster europe");
      Assert.assertEquals(db.getClusters(), expectedClusters + 1);
    } finally {
      db.command("drop cluster europe");
    }
    Assert.assertEquals(db.getClusters(), expectedClusters);
  }

  @Test
  public void testAlterClusterName() {
    try {
      db.command("create cluster europe");
      db.command("ALTER CLUSTER europe NAME \"my_orient\"");

      var clusterId = db.getClusterIdByName("my_orient");
      Assert.assertEquals(clusterId, 18);
    } finally {
      db.command("drop cluster my_orient");
    }
  }
}
