package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class DateBinaryComparatorTest extends DBTestBase {

  private final String dateFormat = "yyyy-MM-dd";
  private final String dateValue = "2017-07-18";

  public void beforeTest() throws Exception {
    super.beforeTest();
    initSchema();
  }

  private void initSchema() {
    YTClass testClass = db.getMetadata().getSchema().createClass("Test");
    testClass.createProperty(db, "date", YTType.DATE);
    db.begin();
    EntityImpl document = new EntityImpl(testClass.getName());

    try {
      document.field("date", new SimpleDateFormat(dateFormat).parse(dateValue));
      document.save();
      db.commit();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testDateJavaClassPreparedStatement() throws ParseException {
    String str = "SELECT FROM Test WHERE date = :dateParam";
    Map<String, Object> params = new HashMap<String, Object>();
    params.put("dateParam", new SimpleDateFormat(dateFormat).parse(dateValue));

    try (YTResultSet result = db.query(str, params)) {
      assertEquals(1, result.stream().count());
    }
  }
}
