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

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class AbstractClassTest extends BaseDBTest {

  @Parameters(value = "remote")
  public AbstractClassTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }

  @BeforeClass
  public void createSchema() throws IOException {
    var abstractPerson =
        session.getMetadata().getSchema().createAbstractClass("AbstractPerson");
    abstractPerson.createProperty(session, "name", PropertyType.STRING);

    Assert.assertTrue(abstractPerson.isAbstract(session));
    Assert.assertEquals(abstractPerson.getClusterIds(session).length, 1);
  }

  @Test
  public void testCannotCreateInstances() {
    try {
      session.begin();
      session.newEntity("AbstractPerson");
      session.begin();
    } catch (BaseException e) {
      Throwable cause = e;

      while (cause.getCause() != null) {
        cause = cause.getCause();
      }

      Assert.assertTrue(cause instanceof SchemaException);
    }
  }
}
