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

package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.exception.OInvalidDatabaseNameException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class StorageNamingTests {

  @Test
  public void testSpecialLettersOne() {
    try (OxygenDB oxygenDB = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig())) {
      try {
        oxygenDB.create("name%", ODatabaseType.MEMORY);
        Assert.fail();
      } catch (OInvalidDatabaseNameException e) {
        // skip
      }
    }
  }

  @Test
  public void testSpecialLettersTwo() {
    try (OxygenDB oxygenDB = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig())) {
      try {
        oxygenDB.create("na.me", ODatabaseType.MEMORY);
        Assert.fail();
      } catch (OInvalidDatabaseNameException e) {
        // skip
      }
    }
  }

  @Test
  public void testSpecialLettersThree() {
    try (OxygenDB oxygenDB = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig())) {
      oxygenDB.create("na_me$", ODatabaseType.MEMORY);
      oxygenDB.drop("na_me$");
    }
  }

  @Test
  public void commaInPathShouldBeAllowed() {
    OAbstractPaginatedStorage.checkName("/path/with/,/but/not/in/the/name");
    OAbstractPaginatedStorage.checkName("/,,,/,/,/name");
  }

  @Test(expected = OInvalidDatabaseNameException.class)
  public void commaInNameShouldThrow() {
    OAbstractPaginatedStorage.checkName("/path/with/,/name/with,");
  }

  @Test(expected = OInvalidDatabaseNameException.class)
  public void name() throws Exception {
    OAbstractPaginatedStorage.checkName("/name/with,");
  }
}
