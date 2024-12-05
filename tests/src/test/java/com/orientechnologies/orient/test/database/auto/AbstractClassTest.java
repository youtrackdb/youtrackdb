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

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBConfigBuilder;
import com.orientechnologies.orient.core.exception.YTSchemaException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
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
    builder.addConfig(YTGlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    return builder.build();
  }

  @BeforeClass
  public void createSchema() throws IOException {
    YTClass abstractPerson =
        database.getMetadata().getSchema().createAbstractClass("AbstractPerson");
    abstractPerson.createProperty(database, "name", YTType.STRING);

    Assert.assertTrue(abstractPerson.isAbstract());
    Assert.assertEquals(abstractPerson.getClusterIds().length, 1);
    Assert.assertEquals(abstractPerson.getDefaultClusterId(), -1);
  }

  @Test
  public void testCannotCreateInstances() {
    try {
      database.begin();
      new YTEntityImpl("AbstractPerson").save();
      database.begin();
    } catch (YTException e) {
      Throwable cause = e;

      while (cause.getCause() != null) {
        cause = cause.getCause();
      }

      Assert.assertTrue(cause instanceof YTSchemaException);
    }
  }
}
