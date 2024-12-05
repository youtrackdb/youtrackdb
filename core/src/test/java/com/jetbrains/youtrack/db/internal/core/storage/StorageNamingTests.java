/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 */

package com.jetbrains.youtrack.db.internal.core.storage;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseType;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.exception.YTInvalidDatabaseNameException;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class StorageNamingTests {

  @Test
  public void testSpecialLettersOne() {
    try (YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      try {
        youTrackDB.create("name%", ODatabaseType.MEMORY);
        Assert.fail();
      } catch (YTInvalidDatabaseNameException e) {
        // skip
      }
    }
  }

  @Test
  public void testSpecialLettersTwo() {
    try (YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      try {
        youTrackDB.create("na.me", ODatabaseType.MEMORY);
        Assert.fail();
      } catch (YTInvalidDatabaseNameException e) {
        // skip
      }
    }
  }

  @Test
  public void testSpecialLettersThree() {
    try (YouTrackDB youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig())) {
      youTrackDB.create("na_me$", ODatabaseType.MEMORY);
      youTrackDB.drop("na_me$");
    }
  }

  @Test
  public void commaInPathShouldBeAllowed() {
    AbstractPaginatedStorage.checkName("/path/with/,/but/not/in/the/name");
    AbstractPaginatedStorage.checkName("/,,,/,/,/name");
  }

  @Test(expected = YTInvalidDatabaseNameException.class)
  public void commaInNameShouldThrow() {
    AbstractPaginatedStorage.checkName("/path/with/,/name/with,");
  }

  @Test(expected = YTInvalidDatabaseNameException.class)
  public void name() throws Exception {
    AbstractPaginatedStorage.checkName("/name/with,");
  }
}
