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

package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class DocumentTest extends DBTestBase {

  @Test
  public void testFromMapNotSaved() {
    final YTDocument doc = new YTDocument();
    doc.field("name", "Jay");
    doc.field("surname", "Miner");
    Map<String, Object> map = doc.toMap();

    Assert.assertEquals(map.size(), 2);
    Assert.assertEquals(map.get("name"), "Jay");
    Assert.assertEquals(map.get("surname"), "Miner");
  }

  @Test
  public void testFromMapWithClass() {
    final YTDocument doc = new YTDocument("OUser");
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
    final YTDocument doc = new YTDocument("V");
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
    YTDocument doc = new YTDocument();

    doc.field("some", 3);
    doc.setFieldType("some", YTType.STRING);
    Assert.assertEquals(doc.fieldType("some"), YTType.STRING);
    Assert.assertEquals(doc.field("some"), "3");
  }

  @Test
  public void testEval() {
    YTDocument doc = new YTDocument();

    doc.field("amount", 300);

    Number amountPlusVat = (Number) doc.eval("amount * 120 / 100");

    Assert.assertEquals(amountPlusVat.longValue(), 360L);
  }

  @Test
  public void testEvalInContext() {
    YTDocument doc = new YTDocument();

    doc.field("amount", 300);

    OBasicCommandContext context = new OBasicCommandContext();
    context.setVariable("vat", 20);
    context.setDatabase(db);
    Number amountPlusVat = (Number) doc.eval("amount * (100 + $vat) / 100", context);

    Assert.assertEquals(amountPlusVat.longValue(), 360L);
  }
}
