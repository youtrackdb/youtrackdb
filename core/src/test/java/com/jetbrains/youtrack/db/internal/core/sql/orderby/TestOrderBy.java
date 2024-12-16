package com.jetbrains.youtrack.db.internal.core.sql.orderby;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.Result;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.junit.Ignore;
import org.junit.Test;

public class TestOrderBy extends DbTestBase {

  @Test
  public void testGermanOrderBy() {
    db.set(DatabaseSession.ATTRIBUTES.LOCALE_COUNTRY, Locale.GERMANY.getCountry());
    db.set(DatabaseSession.ATTRIBUTES.LOCALE_LANGUAGE, Locale.GERMANY.getLanguage());
    db.getMetadata().getSchema().createClass("test");

    db.begin();
    Record res1 = db.save(new EntityImpl("test").field("name", "Ähhhh"));
    Record res2 = db.save(new EntityImpl("test").field("name", "Ahhhh"));
    Record res3 = db.save(new EntityImpl("test").field("name", "Zebra"));
    db.commit();

    List<Result> queryRes =
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
    db.set(DatabaseSession.ATTRIBUTES.LOCALE_COUNTRY, Locale.GERMANY.getCountry());
    db.set(DatabaseSession.ATTRIBUTES.LOCALE_LANGUAGE, Locale.GERMANY.getLanguage());
    SchemaClass clazz = db.getMetadata().getSchema().createClass("test");
    clazz.createProperty(db, "name", PropertyType.STRING).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    Record res1 = db.save(new EntityImpl("test").field("name", "Ähhhh"));
    Record res2 = db.save(new EntityImpl("test").field("name", "Ahhhh"));
    Record res3 = db.save(new EntityImpl("test").field("name", "Zebra"));
    List<Result> queryRes =
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
