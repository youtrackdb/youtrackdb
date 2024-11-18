/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "db")
public class DbClosedTest extends DocumentDBBaseTest {

  @Parameters(value = {"remote"})
  public DbClosedTest(boolean remote) {
    super(remote, "db-closed-test");
  }

  public void testDoubleDb() {
    ODatabaseSession db = acquireSession();

    // now I am getting another db instance
    ODatabaseSession dbAnother = acquireSession();
    dbAnother.close();

    db.activateOnCurrentThread();
    db.close();
  }

  public void testDoubleDbWindowsPath() {
    ODatabaseSession db = acquireSession();

    // now I am getting another db instance
    ODatabaseSession dbAnother = acquireSession();
    dbAnother.close();

    db.activateOnCurrentThread();
    db.close();
  }

  @Test
  public void testRemoteConns() {
    if (remoteDB) {
      return;
    }

    final int max = OGlobalConfiguration.NETWORK_MAX_CONCURRENT_SESSIONS.getValueAsInteger();
    for (int i = 0; i < max * 2; ++i) {
      final ODatabaseSession db = acquireSession();
      db.close();
    }
  }
}
