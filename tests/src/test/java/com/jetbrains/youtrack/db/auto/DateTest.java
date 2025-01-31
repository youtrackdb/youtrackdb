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

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class DateTest extends BaseDBTest {

  @Parameters(value = "remote")
  public DateTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testDateConversion() throws ParseException {
    final var begin = System.currentTimeMillis();

    db.createClass("Order");
    db.begin();
    var doc1 = ((EntityImpl) db.newEntity("Order"));
    doc1.field("context", "test");
    doc1.field("date", new Date());
    doc1.save();

    var doc2 = ((EntityImpl) db.newEntity("Order"));
    doc2.field("context", "test");
    doc2.field("date", System.currentTimeMillis());
    doc2.save();
    db.commit();

    db.begin();
    doc2 = db.bindToSession(doc2);
    Assert.assertTrue(doc2.field("date", PropertyType.DATE) instanceof Date);
    Assert.assertTrue(doc2.field("date", Date.class) instanceof Date);

    var result =
        db.command("select * from Order where date >= ? and context = 'test'", begin);

    Assert.assertEquals(result.stream().count(), 2);
    db.rollback();
  }

  @Test
  public void testDatePrecision() throws ParseException {
    final var begin = System.currentTimeMillis();

    var dateAsString =
        db.getStorage().getConfiguration().getDateFormatInstance().format(begin);

    db.begin();
    var doc = ((EntityImpl) db.newEntity("Order"));
    doc.field("context", "testPrecision");
    doc.field("date", DateHelper.now(), PropertyType.DATETIME);
    doc.save();
    db.commit();

    var result =
        db
            .command(
                "select * from Order where date >= ? and context = 'testPrecision'", dateAsString)
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testDateTypes() throws ParseException {
    var doc = ((EntityImpl) db.newEntity());
    doc.field("context", "test");
    doc.field("date", System.currentTimeMillis(), PropertyType.DATE);

    Assert.assertTrue(doc.field("date") instanceof Date);
  }

  /**
   * https://github.com/orientechnologies/orientjs/issues/48
   */
  @Test
  public void testDateGregorianCalendar() throws ParseException {
    db.command("CREATE CLASS TimeTest EXTENDS V").close();

    final var df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    final var date = df.parse("1200-11-11 00:00:00.000");

    db.begin();
    db
        .command("CREATE VERTEX TimeTest SET firstname = ?, birthDate = ?", "Robert", date)
        .close();
    db.commit();

    var result = db.query("select from TimeTest where firstname = ?", "Robert");
    Assert.assertEquals(result.next().getProperty("birthDate"), date);
    Assert.assertFalse(result.hasNext());
  }
}
