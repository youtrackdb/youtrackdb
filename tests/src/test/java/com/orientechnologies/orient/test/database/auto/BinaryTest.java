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

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.YTRecordBytes;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class BinaryTest extends DocumentDBBaseTest {

  private YTRID rid;

  @Parameters(value = "remote")
  public BinaryTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testMixedCreateEmbedded() {
    database.begin();
    YTDocument doc = new YTDocument();
    doc.field("binary", "Binary data".getBytes());

    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    Assert.assertEquals(new String((byte[]) doc.field("binary", YTType.BINARY)), "Binary data");
    database.rollback();
  }

  @Test
  public void testBasicCreateExternal() {
    database.begin();
    YTBlob record = new YTRecordBytes(database, "This is a test".getBytes());
    record.save();
    database.commit();

    rid = record.getIdentity();
  }

  @Test(dependsOnMethods = "testBasicCreateExternal")
  public void testBasicReadExternal() {
    YTRecordAbstract record = database.load(rid);

    Assert.assertEquals("This is a test", new String(record.toStream()));
  }

  @Test(dependsOnMethods = "testBasicReadExternal")
  public void testMixedCreateExternal() {
    database.begin();

    YTDocument doc = new YTDocument();
    doc.field("binary", new YTRecordBytes(database, "Binary data".getBytes()));

    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    rid = doc.getIdentity();
  }

  @Test(dependsOnMethods = "testMixedCreateExternal")
  public void testMixedReadExternal() {
    YTDocument doc = rid.getRecord();
    Assert.assertEquals("Binary data",
        new String(((YTRecordAbstract) doc.field("binary")).toStream()));
  }
}
