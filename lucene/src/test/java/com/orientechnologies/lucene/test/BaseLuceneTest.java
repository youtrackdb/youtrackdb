/*
 *
 *
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.test;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import java.io.IOException;
import java.io.InputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 *
 */
public abstract class BaseLuceneTest {

  @Rule
  public TestName name = new TestName();

  protected ODatabaseSessionInternal db;
  protected OxygenDB context;

  protected ODatabaseType type;
  protected String dbName;

  @Before
  public void setupDatabase() throws Throwable {
    final String config =
        System.getProperty("oxygendb.test.env", ODatabaseType.MEMORY.name().toLowerCase());
    String path;

    if ("ci".equals(config) || "release".equals(config)) {
      type = ODatabaseType.PLOCAL;
      path = "embedded:./target/databases";
    } else {
      type = ODatabaseType.MEMORY;
      path = "embedded:.";
    }
    context = new OxygenDB(path, OxygenDBConfig.defaultConfig());
    dbName = getClass().getSimpleName() + "_" + name.getMethodName();

    if (context.exists(dbName)) {
      context.drop(dbName);
    }
    context.execute(
        "create database ? " + type.toString() + " users(admin identified by 'admin' role admin) ",
        dbName);

    db = (ODatabaseSessionInternal) context.open(dbName, "admin", "admin");
    db.set(ATTRIBUTES.MINIMUMCLUSTERS, 8);
  }

  public ODatabaseSessionInternal openDatabase() {
    return (ODatabaseSessionInternal) context.open(dbName, "admin", "admin");
  }

  public void createDatabase() {
    context.execute(
        "create database ? " + type + " users(admin identified by 'admin' role admin) ", dbName);
  }

  @After
  public void dropDatabase() {
    db.activateOnCurrentThread();
    context.drop(dbName);
  }

  protected String getScriptFromStream(final InputStream scriptStream) {
    try {
      return OIOUtils.readStreamAsString(scriptStream);
    } catch (final IOException e) {
      throw new RuntimeException("Could not read script stream.", e);
    }
  }
}
