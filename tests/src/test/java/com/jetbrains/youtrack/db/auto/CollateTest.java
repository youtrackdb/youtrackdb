package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.collate.CaseInsensitiveCollate;
import com.jetbrains.youtrack.db.internal.core.collate.DefaultCollate;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class CollateTest extends BaseDBTest {

  @Parameters(value = "remote")
  public CollateTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testQuery() {
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("collateTest");

    var csp = clazz.createProperty(session, "csp", PropertyType.STRING);
    csp.setCollate(session, DefaultCollate.NAME);

    var cip = clazz.createProperty(session, "cip", PropertyType.STRING);
    cip.setCollate(session, CaseInsensitiveCollate.NAME);

    for (var i = 0; i < 10; i++) {
      var document = ((EntityImpl) session.newEntity("collateTest"));

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      session.begin();

      session.commit();
    }

    @SuppressWarnings("deprecation")
    List<EntityImpl> result =
        session.query(
            new SQLSynchQuery<EntityImpl>("select from collateTest where csp = 'VAL'"));
    Assert.assertEquals(result.size(), 5);

    for (var document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    //noinspection deprecation
    result =
        session.query(
            new SQLSynchQuery<EntityImpl>("select from collateTest where cip = 'VaL'"));
    Assert.assertEquals(result.size(), 10);

    for (var document : result) {
      Assert.assertEquals((document.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }
  }

  public void testQueryNotNullCi() {
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("collateTestNotNull");

    var csp = clazz.createProperty(session, "bar", PropertyType.STRING);
    csp.setCollate(session, CaseInsensitiveCollate.NAME);

    var document = ((EntityImpl) session.newEntity("collateTestNotNull"));
    document.field("bar", "baz");

    session.begin();

    session.commit();

    document = ((EntityImpl) session.newEntity("collateTestNotNull"));
    document.field("nobar", true);

    session.begin();

    session.commit();

    @SuppressWarnings("deprecation")
    List<EntityImpl> result =
        session.query(
            new SQLSynchQuery<EntityImpl>("select from collateTestNotNull where bar is null"));
    Assert.assertEquals(result.size(), 1);

    //noinspection deprecation
    result =
        session.query(
            new SQLSynchQuery<EntityImpl>(
                "select from collateTestNotNull where bar is not null"));
    Assert.assertEquals(result.size(), 1);
  }

  public void testIndexQuery() {
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("collateIndexTest");

    var csp = clazz.createProperty(session, "csp", PropertyType.STRING);
    csp.setCollate(session, DefaultCollate.NAME);

    var cip = clazz.createProperty(session, "cip", PropertyType.STRING);
    cip.setCollate(session, CaseInsensitiveCollate.NAME);

    clazz.createIndex(session, "collateIndexCSP", SchemaClass.INDEX_TYPE.NOTUNIQUE, "csp");
    clazz.createIndex(session, "collateIndexCIP", SchemaClass.INDEX_TYPE.NOTUNIQUE, "cip");

    for (var i = 0; i < 10; i++) {
      var document = ((EntityImpl) session.newEntity("collateIndexTest"));

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      session.begin();

      session.commit();
    }

    var query = "select from collateIndexTest where csp = 'VAL'";
    @SuppressWarnings("deprecation")
    List<EntityImpl> result = session.query(new SQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (var document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    @SuppressWarnings("deprecation")
    EntityImpl explain = session.command(new CommandSQL("explain " + query)).execute(session);
    Assert.assertTrue(explain.<Set<String>>field("involvedIndexes").contains("collateIndexCSP"));

    query = "select from collateIndexTest where cip = 'VaL'";
    //noinspection deprecation
    result = session.query(new SQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 10);

    for (var document : result) {
      Assert.assertEquals((document.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }

    //noinspection deprecation
    explain = session.command(new CommandSQL("explain " + query)).execute(session);
    Assert.assertTrue(explain.<Set<String>>field("involvedIndexes").contains("collateIndexCIP"));
  }

  public void testIndexQueryCollateWasChanged() {
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("collateWasChangedIndexTest");

    var cp = clazz.createProperty(session, "cp", PropertyType.STRING);
    cp.setCollate(session, DefaultCollate.NAME);

    clazz.createIndex(session, "collateWasChangedIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "cp");

    for (var i = 0; i < 10; i++) {
      var document = ((EntityImpl) session.newEntity("collateWasChangedIndexTest"));

      if (i % 2 == 0) {
        document.field("cp", "VAL");
      } else {
        document.field("cp", "val");
      }

      session.begin();

      session.commit();
    }

    var query = "select from collateWasChangedIndexTest where cp = 'VAL'";
    @SuppressWarnings("deprecation")
    List<EntityImpl> result = session.query(new SQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (var document : result) {
      Assert.assertEquals(document.field("cp"), "VAL");
    }

    @SuppressWarnings("deprecation")
    EntityImpl explain = session.command(new CommandSQL("explain " + query)).execute(session);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("collateWasChangedIndex"));

    cp = clazz.getProperty(session, "cp");
    cp.setCollate(session, CaseInsensitiveCollate.NAME);

    query = "select from collateWasChangedIndexTest where cp = 'VaL'";
    //noinspection deprecation
    result = session.query(new SQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 10);

    for (var document : result) {
      Assert.assertEquals((document.<String>field("cp")).toUpperCase(Locale.ENGLISH), "VAL");
    }

    //noinspection deprecation
    explain = session.command(new CommandSQL("explain " + query)).execute(session);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("collateWasChangedIndex"));
  }

  public void testCompositeIndexQueryCS() {
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("CompositeIndexQueryCSTest");

    var csp = clazz.createProperty(session, "csp", PropertyType.STRING);
    csp.setCollate(session, DefaultCollate.NAME);

    var cip = clazz.createProperty(session, "cip", PropertyType.STRING);
    cip.setCollate(session, CaseInsensitiveCollate.NAME);

    clazz.createIndex(session, "collateCompositeIndexCS", SchemaClass.INDEX_TYPE.NOTUNIQUE, "csp",
        "cip");

    for (var i = 0; i < 10; i++) {
      var document = ((EntityImpl) session.newEntity("CompositeIndexQueryCSTest"));

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      session.begin();

      session.commit();
    }

    var query = "select from CompositeIndexQueryCSTest where csp = 'VAL'";
    @SuppressWarnings("deprecation")
    List<EntityImpl> result = session.query(new SQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (var document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    @SuppressWarnings("deprecation")
    EntityImpl explain = session.command(new CommandSQL("explain " + query)).execute(session);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("collateCompositeIndexCS"));

    query = "select from CompositeIndexQueryCSTest where csp = 'VAL' and cip = 'VaL'";
    //noinspection deprecation
    result = session.query(new SQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (var document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
      Assert.assertEquals((document.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }

    //noinspection deprecation
    explain = session.command(new CommandSQL("explain " + query)).execute(session);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("collateCompositeIndexCS"));

    if (!session.getStorage().isRemote()) {
      final var indexManager = session.getMetadata().getIndexManagerInternal();
      final var index = indexManager.getIndex(session, "collateCompositeIndexCS");

      final Collection<RID> value;
      try (var stream = index.getInternal()
          .getRids(session, new CompositeKey("VAL", "VaL"))) {
        value = stream.toList();
      }

      Assert.assertEquals(value.size(), 5);
      for (var identifiable : value) {
        final EntityImpl record = identifiable.getRecord(session);
        Assert.assertEquals(record.field("csp"), "VAL");
        Assert.assertEquals((record.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
      }
    }
  }

  public void testCompositeIndexQueryCollateWasChanged() {
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("CompositeIndexQueryCollateWasChangedTest");

    var csp = clazz.createProperty(session, "csp", PropertyType.STRING);
    csp.setCollate(session, DefaultCollate.NAME);

    clazz.createProperty(session, "cip", PropertyType.STRING);

    clazz.createIndex(session,
        "collateCompositeIndexCollateWasChanged", SchemaClass.INDEX_TYPE.NOTUNIQUE, "csp", "cip");

    for (var i = 0; i < 10; i++) {
      var document = ((EntityImpl) session.newEntity("CompositeIndexQueryCollateWasChangedTest"));
      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      session.begin();

      session.commit();
    }

    var query = "select from CompositeIndexQueryCollateWasChangedTest where csp = 'VAL'";
    @SuppressWarnings("deprecation")
    List<EntityImpl> result = session.query(new SQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (var document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    @SuppressWarnings("deprecation")
    EntityImpl explain = session.command(new CommandSQL("explain " + query)).execute(session);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("collateCompositeIndexCollateWasChanged"));

    csp = clazz.getProperty(session, "csp");
    csp.setCollate(session, CaseInsensitiveCollate.NAME);

    query = "select from CompositeIndexQueryCollateWasChangedTest where csp = 'VaL'";
    //noinspection deprecation
    result = session.query(new SQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 10);

    for (var document : result) {
      Assert.assertEquals(document.<String>field("csp").toUpperCase(Locale.ENGLISH), "VAL");
    }

    //noinspection deprecation
    explain = session.command(new CommandSQL("explain " + query)).execute(session);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("collateCompositeIndexCollateWasChanged"));
  }

  public void collateThroughSQL() {
    final Schema schema = session.getMetadata().getSchema();
    var clazz = schema.createClass("collateTestViaSQL");

    clazz.createProperty(session, "csp", PropertyType.STRING);
    clazz.createProperty(session, "cip", PropertyType.STRING);

    //noinspection deprecation
    session
        .command(
            new CommandSQL(
                "create index collateTestViaSQL.index on collateTestViaSQL (cip COLLATE CI)"
                    + " NOTUNIQUE"))
        .execute(session);

    for (var i = 0; i < 10; i++) {
      var document = ((EntityImpl) session.newEntity("collateTestViaSQL"));

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      session.begin();

      session.commit();
    }

    @SuppressWarnings("deprecation")
    List<EntityImpl> result =
        session.query(
            new SQLSynchQuery<EntityImpl>("select from collateTestViaSQL where csp = 'VAL'"));
    Assert.assertEquals(result.size(), 5);

    for (var document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    //noinspection deprecation
    result =
        session.query(
            new SQLSynchQuery<EntityImpl>("select from collateTestViaSQL where cip = 'VaL'"));
    Assert.assertEquals(result.size(), 10);

    for (var document : result) {
      Assert.assertEquals((document.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }
  }
}
