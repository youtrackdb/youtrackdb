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

import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
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
public class DateTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public DateTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testDateConversion() throws ParseException {
    final long begin = System.currentTimeMillis();

    database.createClass("Order");
    database.begin();
    EntityImpl doc1 = new EntityImpl("Order");
    doc1.field("context", "test");
    doc1.field("date", new Date());
    doc1.save();

    EntityImpl doc2 = new EntityImpl("Order");
    doc2.field("context", "test");
    doc2.field("date", System.currentTimeMillis());
    doc2.save();
    database.commit();

    database.begin();
    doc2 = database.bindToSession(doc2);
    Assert.assertTrue(doc2.field("date", PropertyType.DATE) instanceof Date);
    Assert.assertTrue(doc2.field("date", Date.class) instanceof Date);

    ResultSet result =
        database.command("select * from Order where date >= ? and context = 'test'", begin);

    Assert.assertEquals(result.stream().count(), 2);
    database.rollback();
  }

  @Test
  public void testDatePrecision() throws ParseException {
    final long begin = System.currentTimeMillis();

    String dateAsString =
        database.getStorage().getConfiguration().getDateFormatInstance().format(begin);

    database.begin();
    EntityImpl doc = new EntityImpl("Order");
    doc.field("context", "testPrecision");
    doc.field("date", DateHelper.now(), PropertyType.DATETIME);
    doc.save();
    database.commit();

    List<Result> result =
        database
            .command(
                "select * from Order where date >= ? and context = 'testPrecision'", dateAsString)
            .stream()
            .collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testDateTypes() throws ParseException {
    EntityImpl doc = new EntityImpl();
    doc.field("context", "test");
    doc.field("date", System.currentTimeMillis(), PropertyType.DATE);

    Assert.assertTrue(doc.field("date") instanceof Date);
  }

  /**
   * https://github.com/orientechnologies/orientjs/issues/48
   */
  @Test
  public void testDateGregorianCalendar() throws ParseException {
    database.command("CREATE CLASS TimeTest EXTENDS V").close();

    final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    final Date date = df.parse("1200-11-11 00:00:00.000");

    database.begin();
    database
        .command("CREATE VERTEX TimeTest SET firstname = ?, birthDate = ?", "Robert", date)
        .close();
    database.commit();

    ResultSet result = database.query("select from TimeTest where firstname = ?", "Robert");
    Assert.assertEquals(result.next().getProperty("birthDate"), date);
    Assert.assertFalse(result.hasNext());
  }
}
