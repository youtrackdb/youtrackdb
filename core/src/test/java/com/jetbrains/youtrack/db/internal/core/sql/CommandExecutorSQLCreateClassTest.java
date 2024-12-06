/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CommandExecutorSQLCreateClassTest extends DbTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    final Schema schema = db.getMetadata().getSchema();
    schema.createClass("User", schema.getClass("V"));
  }

  @Test
  public void testCreateWithSuperclasses() throws Exception {

    db.command("create class `UserVertex` extends `V` , `User`").close();

    SchemaClass userVertex = db.getMetadata().getSchema().getClass("UserVertex");

    Assert.assertNotNull(userVertex);

    List<String> superClassesNames = userVertex.getSuperClassesNames();

    Assert.assertEquals(2, superClassesNames.size());

    Assert.assertTrue(superClassesNames.contains("User"));
    Assert.assertTrue(superClassesNames.contains("V"));
  }
}
