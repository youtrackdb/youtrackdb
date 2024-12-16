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

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class DocumentTest extends DbTestBase {

  @Test
  public void testFromMapNotSaved() {
    final EntityImpl doc = new EntityImpl();
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    Map<String, Object> map = doc.toMap();

    Assert.assertEquals(map.size(), 2);
    Assert.assertEquals(map.get("name"), "Jay");
    Assert.assertEquals(map.get("surname"), "Miner");
  }

  @Test
  public void testFromMapWithClass() {
    final EntityImpl doc = new EntityImpl("OUser");
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    Map<String, Object> map = doc.toMap();

    Assert.assertEquals(map.size(), 3);
    Assert.assertEquals(map.get("name"), "Jay");
    Assert.assertEquals(map.get("surname"), "Miner");
    Assert.assertEquals(map.get("@class"), "OUser");
  }

  @Test
  public void testFromMapWithClassAndRid() {
    db.begin();
    final EntityImpl doc = new EntityImpl("V");
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    doc.save();
    db.commit();

    Map<String, Object> map = db.bindToSession(doc).toMap();

    Assert.assertEquals(map.size(), 4);
    Assert.assertEquals(map.get("name"), "Jay");
    Assert.assertEquals(map.get("surname"), "Miner");
    Assert.assertEquals(map.get("@class"), "V");
    Assert.assertTrue(map.containsKey("@rid"));
  }

  @Test
  public void testConversionOnTypeSet() {
    EntityImpl doc = new EntityImpl();

    doc.field("some", 3);
    doc.setFieldType("some", PropertyType.STRING);
    Assert.assertEquals(doc.fieldType("some"), PropertyType.STRING);
    Assert.assertEquals(doc.field("some"), "3");
  }

  @Test
  public void testEval() {
    EntityImpl doc = new EntityImpl();

    doc.field("amount", 300);

    Number amountPlusVat = (Number) doc.eval("amount * 120 / 100");

    Assert.assertEquals(amountPlusVat.longValue(), 360L);
  }

  @Test
  public void testEvalInContext() {
    EntityImpl doc = new EntityImpl();

    doc.field("amount", 300);

    BasicCommandContext context = new BasicCommandContext();
    context.setVariable("vat", 20);
    context.setDatabase(db);
    Number amountPlusVat = (Number) doc.eval("amount * (100 + $vat) / 100", context);

    Assert.assertEquals(amountPlusVat.longValue(), 360L);
  }
}
