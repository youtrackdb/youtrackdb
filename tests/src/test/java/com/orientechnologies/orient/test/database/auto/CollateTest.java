package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.collate.OCaseInsensitiveCollate;
import com.jetbrains.youtrack.db.internal.core.collate.ODefaultCollate;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OCompositeKey;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.OCommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLSynchQuery;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class CollateTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public CollateTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testQuery() {
    final YTSchema schema = database.getMetadata().getSchema();
    YTClass clazz = schema.createClass("collateTest");

    YTProperty csp = clazz.createProperty(database, "csp", YTType.STRING);
    csp.setCollate(database, ODefaultCollate.NAME);

    YTProperty cip = clazz.createProperty(database, "cip", YTType.STRING);
    cip.setCollate(database, OCaseInsensitiveCollate.NAME);

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("collateTest");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      database.begin();
      document.save();
      database.commit();
    }

    @SuppressWarnings("deprecation")
    List<EntityImpl> result =
        database.query(
            new OSQLSynchQuery<EntityImpl>("select from collateTest where csp = 'VAL'"));
    Assert.assertEquals(result.size(), 5);

    for (EntityImpl document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    //noinspection deprecation
    result =
        database.query(
            new OSQLSynchQuery<EntityImpl>("select from collateTest where cip = 'VaL'"));
    Assert.assertEquals(result.size(), 10);

    for (EntityImpl document : result) {
      Assert.assertEquals((document.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }
  }

  public void testQueryNotNullCi() {
    final YTSchema schema = database.getMetadata().getSchema();
    YTClass clazz = schema.createClass("collateTestNotNull");

    YTProperty csp = clazz.createProperty(database, "bar", YTType.STRING);
    csp.setCollate(database, OCaseInsensitiveCollate.NAME);

    EntityImpl document = new EntityImpl("collateTestNotNull");
    document.field("bar", "baz");

    database.begin();
    document.save();
    database.commit();

    document = new EntityImpl("collateTestNotNull");
    document.field("nobar", true);

    database.begin();
    document.save();
    database.commit();

    @SuppressWarnings("deprecation")
    List<EntityImpl> result =
        database.query(
            new OSQLSynchQuery<EntityImpl>("select from collateTestNotNull where bar is null"));
    Assert.assertEquals(result.size(), 1);

    //noinspection deprecation
    result =
        database.query(
            new OSQLSynchQuery<EntityImpl>(
                "select from collateTestNotNull where bar is not null"));
    Assert.assertEquals(result.size(), 1);
  }

  public void testIndexQuery() {
    final YTSchema schema = database.getMetadata().getSchema();
    YTClass clazz = schema.createClass("collateIndexTest");

    YTProperty csp = clazz.createProperty(database, "csp", YTType.STRING);
    csp.setCollate(database, ODefaultCollate.NAME);

    YTProperty cip = clazz.createProperty(database, "cip", YTType.STRING);
    cip.setCollate(database, OCaseInsensitiveCollate.NAME);

    clazz.createIndex(database, "collateIndexCSP", YTClass.INDEX_TYPE.NOTUNIQUE, "csp");
    clazz.createIndex(database, "collateIndexCIP", YTClass.INDEX_TYPE.NOTUNIQUE, "cip");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("collateIndexTest");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      database.begin();
      document.save();
      database.commit();
    }

    String query = "select from collateIndexTest where csp = 'VAL'";
    @SuppressWarnings("deprecation")
    List<EntityImpl> result = database.query(new OSQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (EntityImpl document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    @SuppressWarnings("deprecation")
    EntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(explain.<Set<String>>field("involvedIndexes").contains("collateIndexCSP"));

    query = "select from collateIndexTest where cip = 'VaL'";
    //noinspection deprecation
    result = database.query(new OSQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 10);

    for (EntityImpl document : result) {
      Assert.assertEquals((document.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }

    //noinspection deprecation
    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(explain.<Set<String>>field("involvedIndexes").contains("collateIndexCIP"));
  }

  public void testIndexQueryCollateWasChanged() {
    final YTSchema schema = database.getMetadata().getSchema();
    YTClass clazz = schema.createClass("collateWasChangedIndexTest");

    YTProperty cp = clazz.createProperty(database, "cp", YTType.STRING);
    cp.setCollate(database, ODefaultCollate.NAME);

    clazz.createIndex(database, "collateWasChangedIndex", YTClass.INDEX_TYPE.NOTUNIQUE, "cp");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("collateWasChangedIndexTest");

      if (i % 2 == 0) {
        document.field("cp", "VAL");
      } else {
        document.field("cp", "val");
      }

      database.begin();
      document.save();
      database.commit();
    }

    String query = "select from collateWasChangedIndexTest where cp = 'VAL'";
    @SuppressWarnings("deprecation")
    List<EntityImpl> result = database.query(new OSQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (EntityImpl document : result) {
      Assert.assertEquals(document.field("cp"), "VAL");
    }

    @SuppressWarnings("deprecation")
    EntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("collateWasChangedIndex"));

    cp = clazz.getProperty("cp");
    cp.setCollate(database, OCaseInsensitiveCollate.NAME);

    query = "select from collateWasChangedIndexTest where cp = 'VaL'";
    //noinspection deprecation
    result = database.query(new OSQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 10);

    for (EntityImpl document : result) {
      Assert.assertEquals((document.<String>field("cp")).toUpperCase(Locale.ENGLISH), "VAL");
    }

    //noinspection deprecation
    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("collateWasChangedIndex"));
  }

  public void testCompositeIndexQueryCS() {
    final YTSchema schema = database.getMetadata().getSchema();
    YTClass clazz = schema.createClass("CompositeIndexQueryCSTest");

    YTProperty csp = clazz.createProperty(database, "csp", YTType.STRING);
    csp.setCollate(database, ODefaultCollate.NAME);

    YTProperty cip = clazz.createProperty(database, "cip", YTType.STRING);
    cip.setCollate(database, OCaseInsensitiveCollate.NAME);

    clazz.createIndex(database, "collateCompositeIndexCS", YTClass.INDEX_TYPE.NOTUNIQUE, "csp",
        "cip");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("CompositeIndexQueryCSTest");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      database.begin();
      document.save();
      database.commit();
    }

    String query = "select from CompositeIndexQueryCSTest where csp = 'VAL'";
    @SuppressWarnings("deprecation")
    List<EntityImpl> result = database.query(new OSQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (EntityImpl document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    @SuppressWarnings("deprecation")
    EntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("collateCompositeIndexCS"));

    query = "select from CompositeIndexQueryCSTest where csp = 'VAL' and cip = 'VaL'";
    //noinspection deprecation
    result = database.query(new OSQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (EntityImpl document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
      Assert.assertEquals((document.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }

    //noinspection deprecation
    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("collateCompositeIndexCS"));

    if (!database.getStorage().isRemote()) {
      final OIndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
      final OIndex index = indexManager.getIndex(database, "collateCompositeIndexCS");

      final Collection<YTRID> value;
      try (Stream<YTRID> stream = index.getInternal()
          .getRids(database, new OCompositeKey("VAL", "VaL"))) {
        value = stream.toList();
      }

      Assert.assertEquals(value.size(), 5);
      for (YTRID identifiable : value) {
        final EntityImpl record = identifiable.getRecord();
        Assert.assertEquals(record.field("csp"), "VAL");
        Assert.assertEquals((record.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
      }
    }
  }

  public void testCompositeIndexQueryCollateWasChanged() {
    final YTSchema schema = database.getMetadata().getSchema();
    YTClass clazz = schema.createClass("CompositeIndexQueryCollateWasChangedTest");

    YTProperty csp = clazz.createProperty(database, "csp", YTType.STRING);
    csp.setCollate(database, ODefaultCollate.NAME);

    clazz.createProperty(database, "cip", YTType.STRING);

    clazz.createIndex(database,
        "collateCompositeIndexCollateWasChanged", YTClass.INDEX_TYPE.NOTUNIQUE, "csp", "cip");

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("CompositeIndexQueryCollateWasChangedTest");
      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      database.begin();
      document.save();
      database.commit();
    }

    String query = "select from CompositeIndexQueryCollateWasChangedTest where csp = 'VAL'";
    @SuppressWarnings("deprecation")
    List<EntityImpl> result = database.query(new OSQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 5);

    for (EntityImpl document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    @SuppressWarnings("deprecation")
    EntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("collateCompositeIndexCollateWasChanged"));

    csp = clazz.getProperty("csp");
    csp.setCollate(database, OCaseInsensitiveCollate.NAME);

    query = "select from CompositeIndexQueryCollateWasChangedTest where csp = 'VaL'";
    //noinspection deprecation
    result = database.query(new OSQLSynchQuery<EntityImpl>(query));
    Assert.assertEquals(result.size(), 10);

    for (EntityImpl document : result) {
      Assert.assertEquals(document.<String>field("csp").toUpperCase(Locale.ENGLISH), "VAL");
    }

    //noinspection deprecation
    explain = database.command(new OCommandSQL("explain " + query)).execute(database);
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("collateCompositeIndexCollateWasChanged"));
  }

  public void collateThroughSQL() {
    final YTSchema schema = database.getMetadata().getSchema();
    YTClass clazz = schema.createClass("collateTestViaSQL");

    clazz.createProperty(database, "csp", YTType.STRING);
    clazz.createProperty(database, "cip", YTType.STRING);

    //noinspection deprecation
    database
        .command(
            new OCommandSQL(
                "create index collateTestViaSQL.index on collateTestViaSQL (cip COLLATE CI)"
                    + " NOTUNIQUE"))
        .execute(database);

    for (int i = 0; i < 10; i++) {
      EntityImpl document = new EntityImpl("collateTestViaSQL");

      if (i % 2 == 0) {
        document.field("csp", "VAL");
        document.field("cip", "VAL");
      } else {
        document.field("csp", "val");
        document.field("cip", "val");
      }

      database.begin();
      document.save();
      database.commit();
    }

    @SuppressWarnings("deprecation")
    List<EntityImpl> result =
        database.query(
            new OSQLSynchQuery<EntityImpl>("select from collateTestViaSQL where csp = 'VAL'"));
    Assert.assertEquals(result.size(), 5);

    for (EntityImpl document : result) {
      Assert.assertEquals(document.field("csp"), "VAL");
    }

    //noinspection deprecation
    result =
        database.query(
            new OSQLSynchQuery<EntityImpl>("select from collateTestViaSQL where cip = 'VaL'"));
    Assert.assertEquals(result.size(), 10);

    for (EntityImpl document : result) {
      Assert.assertEquals((document.<String>field("cip")).toUpperCase(Locale.ENGLISH), "VAL");
    }
  }
}
