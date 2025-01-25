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
 *
 */

package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class DocumentTest extends DbTestBase {

  @Test
  public void testFromMapNotSaved() {
    final EntityImpl doc = new EntityImpl(db);
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    Map<String, Object> map = doc.toMap();

    Assert.assertEquals(2, map.size());
    Assert.assertEquals("Jay", map.get("name"));
    Assert.assertEquals("Miner", map.get("surname"));
  }

  @Test
  public void testFromMapWithClass() {
    db.begin();
    final EntityImpl doc = (EntityImpl) db.newEntity("OUser");
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    Map<String, Object> map = doc.toMap();

    Assert.assertEquals(4, map.size());
    Assert.assertEquals("Jay", map.get("name"));
    Assert.assertEquals("Miner", map.get("surname"));
    Assert.assertEquals("OUser", map.get("@class"));
    Assert.assertTrue(map.containsKey("@rid"));
    db.rollback();
  }

  @Test
  public void testFromMapWithClassAndRid() {
    db.begin();
    final EntityImpl doc = (EntityImpl) db.newEntity("V");
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    doc.save();
    db.commit();

    Map<String, Object> map = db.bindToSession(doc).toMap();

    Assert.assertEquals(4, map.size());
    Assert.assertEquals("Jay", map.get("name"));
    Assert.assertEquals("Miner", map.get("surname"));
    Assert.assertEquals("V", map.get("@class"));
    Assert.assertTrue(map.containsKey("@rid"));
  }

  @Test
  public void testConversionOnTypeSet() {
    EntityImpl doc = (EntityImpl) db.newEntity();

    doc.field("some", 3);
    doc.setFieldType("some", PropertyType.STRING);
    Assert.assertEquals(PropertyType.STRING, doc.getPropertyType("some"));
    Assert.assertEquals("3", doc.field("some"));
  }

  @Test
  public void testEval() {
    EntityImpl doc = (EntityImpl) db.newEntity();

    doc.field("amount", 300);

    Number amountPlusVat = (Number) doc.eval("amount * 120 / 100");

    Assert.assertEquals(360L, amountPlusVat.longValue());
  }

  @Test
  public void testEvalInContext() {
    EntityImpl doc = (EntityImpl) db.newEntity();

    doc.field("amount", 300);

    BasicCommandContext context = new BasicCommandContext();
    context.setVariable("vat", 20);
    context.setDatabase(db);
    Number amountPlusVat = (Number) doc.eval("amount * (100 + $vat) / 100", context);

    Assert.assertEquals(360L, amountPlusVat.longValue());
  }
}
