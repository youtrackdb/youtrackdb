package com.orientechnologies.core.sql.orderby;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResult;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.junit.Ignore;
import org.junit.Test;

public class TestOrderBy extends DBTestBase {

  @Test
  public void testGermanOrderBy() {
    db.set(ATTRIBUTES.LOCALECOUNTRY, Locale.GERMANY.getCountry());
    db.set(ATTRIBUTES.LOCALELANGUAGE, Locale.GERMANY.getLanguage());
    db.getMetadata().getSchema().createClass("test");

    db.begin();
    YTRecord res1 = db.save(new YTEntityImpl("test").field("name", "Ähhhh"));
    YTRecord res2 = db.save(new YTEntityImpl("test").field("name", "Ahhhh"));
    YTRecord res3 = db.save(new YTEntityImpl("test").field("name", "Zebra"));
    db.commit();

    List<YTResult> queryRes =
        db.query("select from test order by name").stream().collect(Collectors.toList());
    assertEquals(queryRes.get(0).getIdentity().get(), res2.getIdentity());
    assertEquals(queryRes.get(1).getIdentity().get(), res1.getIdentity());
    assertEquals(queryRes.get(2).getIdentity().get(), res3.getIdentity());

    queryRes =
        db.query("select from test order by name desc ").stream().collect(Collectors.toList());
    assertEquals(queryRes.get(0).getIdentity().get(), res3.getIdentity());
    assertEquals(queryRes.get(1).getIdentity().get(), res1.getIdentity());
    assertEquals(queryRes.get(2).getIdentity().get(), res2.getIdentity());
  }

  @Test
  @Ignore
  public void testGermanOrderByIndex() {
    db.set(ATTRIBUTES.LOCALECOUNTRY, Locale.GERMANY.getCountry());
    db.set(ATTRIBUTES.LOCALELANGUAGE, Locale.GERMANY.getLanguage());
    YTClass clazz = db.getMetadata().getSchema().createClass("test");
    clazz.createProperty(db, "name", YTType.STRING).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    YTRecord res1 = db.save(new YTEntityImpl("test").field("name", "Ähhhh"));
    YTRecord res2 = db.save(new YTEntityImpl("test").field("name", "Ahhhh"));
    YTRecord res3 = db.save(new YTEntityImpl("test").field("name", "Zebra"));
    List<YTResult> queryRes =
        db.query("select from test order by name").stream().collect(Collectors.toList());
    assertEquals(queryRes.get(0).getIdentity().get(), res2.getIdentity());
    assertEquals(queryRes.get(1).getIdentity().get(), res1.getIdentity());
    assertEquals(queryRes.get(2).getIdentity().get(), res3.getIdentity());

    queryRes =
        db.query("select from test order by name desc ").stream().collect(Collectors.toList());
    assertEquals(queryRes.get(0), res3);
    assertEquals(queryRes.get(1), res1);
    assertEquals(queryRes.get(2), res2);
  }
}
