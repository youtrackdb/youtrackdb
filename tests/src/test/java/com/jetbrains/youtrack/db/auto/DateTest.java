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

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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

    session.createClass("Order");
    session.begin();
    var doc1 = ((EntityImpl) session.newEntity("Order"));
    doc1.field("context", "test");
    doc1.field("date", new Date());

    var doc2 = ((EntityImpl) session.newEntity("Order"));
    doc2.field("context", "test");
    doc2.field("date", System.currentTimeMillis());

    session.commit();

    session.begin();
    doc2 = session.bindToSession(doc2);
    Assert.assertNotNull(doc2.getDate("date"));

    var result =
        session.command("select * from Order where date >= ? and context = 'test'", begin);

    Assert.assertEquals(result.stream().count(), 2);
    session.rollback();
  }

  @Test
  public void testDatePrecision() throws ParseException {
    final var begin = System.currentTimeMillis();

    var dateAsString =
        session.getStorage().getConfiguration().getDateFormatInstance().format(begin);

    session.begin();
    var doc = ((EntityImpl) session.newEntity("Order"));
    doc.field("context", "testPrecision");
    doc.field("date", DateHelper.now(), PropertyType.DATETIME);

    session.commit();

    var result =
        session
            .command(
                "select * from Order where date >= ? and context = 'testPrecision'", dateAsString)
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testDateTypes() throws ParseException {
    var doc = ((EntityImpl) session.newEntity());
    doc.field("context", "test");
    doc.field("date", System.currentTimeMillis(), PropertyType.DATE);

    Assert.assertTrue(doc.field("date") instanceof Date);
  }

  /**
   * https://github.com/orientechnologies/orientjs/issues/48
   */
  @Test
  public void testDateGregorianCalendar() throws ParseException {
    session.command("CREATE CLASS TimeTest EXTENDS V").close();

    final var df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    final var date = df.parse("1200-11-11 00:00:00.000");

    session.begin();
    session
        .command("CREATE VERTEX TimeTest SET firstname = ?, birthDate = ?", "Robert", date)
        .close();
    session.commit();

    var result = session.query("select from TimeTest where firstname = ?", "Robert");
    Assert.assertEquals(result.next().getProperty("birthDate"), date);
    Assert.assertFalse(result.hasNext());
  }
}
