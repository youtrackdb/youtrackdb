/*
 *
 *  *  Copyright YouTrackDB
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
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import org.junit.Assert;
import org.junit.Test;

public class OCommandExecutorSQLDropPropertyTest extends DBTestBase {

  @Test
  public void test() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass foo = schema.createClass("Foo");

    foo.createProperty(db, "name", YTType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command("DROP PROPERTY Foo.name").close();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty(db, "name", YTType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command("DROP PROPERTY `Foo`.name").close();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty(db, "name", YTType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command("DROP PROPERTY Foo.`name`").close();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));

    foo.createProperty(db, "name", YTType.STRING);
    Assert.assertTrue(schema.getClass("Foo").existsProperty("name"));
    db.command("DROP PROPERTY `Foo`.`name`").close();
    Assert.assertFalse(schema.getClass("Foo").existsProperty("name"));
  }

  @Test
  public void testIfExists() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass testIfExistsClass = schema.createClass("testIfExists");

    testIfExistsClass.createProperty(db, "name", YTType.STRING);
    Assert.assertTrue(schema.getClass("testIfExists").existsProperty("name"));
    db.command("DROP PROPERTY testIfExists.name if exists").close();
    Assert.assertFalse(schema.getClass("testIfExists").existsProperty("name"));

    db.command("DROP PROPERTY testIfExists.name if exists").close();
    Assert.assertFalse(schema.getClass("testIfExists").existsProperty("name"));
  }
}
