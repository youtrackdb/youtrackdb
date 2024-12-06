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
package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilder;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class AbstractClassTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public AbstractClassTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    return builder.build();
  }

  @BeforeClass
  public void createSchema() throws IOException {
    SchemaClass abstractPerson =
        database.getMetadata().getSchema().createAbstractClass("AbstractPerson");
    abstractPerson.createProperty(database, "name", PropertyType.STRING);

    Assert.assertTrue(abstractPerson.isAbstract());
    Assert.assertEquals(abstractPerson.getClusterIds().length, 1);
    Assert.assertEquals(abstractPerson.getDefaultClusterId(), -1);
  }

  @Test
  public void testCannotCreateInstances() {
    try {
      database.begin();
      new EntityImpl("AbstractPerson").save();
      database.begin();
    } catch (BaseException e) {
      Throwable cause = e;

      while (cause.getCause() != null) {
        cause = cause.getCause();
      }

      Assert.assertTrue(cause instanceof SchemaException);
    }
  }
}
