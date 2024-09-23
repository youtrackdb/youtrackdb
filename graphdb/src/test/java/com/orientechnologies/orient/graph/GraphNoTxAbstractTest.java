/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import java.util.Locale;
import java.util.Objects;
import org.junit.AfterClass;

/**
 * Base class for tests against Non transactonal Graphs.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class GraphNoTxAbstractTest {
  protected static OrientGraph graph;
  protected static OrientDB orientDB;

  public static ENV getEnvironment() {
    String envName = System.getProperty("orientdb.test.env", "dev").toUpperCase(Locale.ENGLISH);
    ENV result = null;
    try {
      result = ENV.valueOf(envName);
    } catch (IllegalArgumentException e) {
    }

    if (result == null) result = ENV.DEV;

    return result;
  }

  public static String getStorageType() {
    if (getEnvironment().equals(ENV.DEV)) {
      return "memory";
    }

    return "plocal";
  }

  public static void init(final String dbName) {
    final String storageType = getStorageType();
    final String buildDirectory = "./target/";

    final String url = System.getProperty("url");
    orientDB =
        new OrientDB(
            Objects.requireNonNullElseGet(url, () -> storageType + ":" + buildDirectory),
            OrientDBConfig.defaultConfig());
    if (orientDB.exists(dbName)) {
      orientDB.drop(dbName);
    }

    orientDB.create(
        dbName, ODatabaseType.valueOf(storageType.toUpperCase()), "admin", "admin", "admin");
    graph =
        new OrientGraph((ODatabaseDocumentInternal) orientDB.open(dbName, "admin", "admin"));
    graph.setAutoStartTx(false);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (graph != null) {
      graph.shutdown();
      graph = null;
    }

    if (orientDB != null) {
      orientDB.close();
      orientDB = null;
    }
  }

  public enum ENV {
    DEV,
    RELEASE,
    CI
  }
}
