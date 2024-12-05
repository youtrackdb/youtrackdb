package com.orientechnologies.orient.core.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableSchema;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cluster.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.Locale;
import org.junit.Assert;
import org.junit.Test;

public class ClassTest extends BaseMemoryInternalDatabase {

  public static final String SHORTNAME_CLASS_NAME = "TestShortName";

  @Test
  public void testShortName() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass(SHORTNAME_CLASS_NAME);
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());

    final OStorage storage = db.getStorage();

    if (storage instanceof OAbstractPaginatedStorage paginatedStorage) {
      final OWriteCache writeCache = paginatedStorage.getWriteCache();
      Assert.assertTrue(
          writeCache.exists(
              SHORTNAME_CLASS_NAME.toLowerCase(Locale.ENGLISH) + OPaginatedCluster.DEF_EXTENSION));
    }

    String shortName = "shortname";
    oClass.setShortName(db, shortName);
    assertEquals(shortName, oClass.getShortName());
    assertEquals(shortName, queryShortName());

    // FAILS, saves null value and stores "null" string (not null value) internally
    shortName = "null";
    oClass.setShortName(db, shortName);
    assertEquals(shortName, oClass.getShortName());
    assertEquals(shortName, queryShortName());

    oClass.setShortName(db, null);
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());

    oClass.setShortName(db, "");
    Assert.assertNull(oClass.getShortName());
    Assert.assertNull(queryShortName());
  }

  @Test
  public void testShortNameSnapshot() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass(SHORTNAME_CLASS_NAME);
    Assert.assertNull(oClass.getShortName());

    String shortName = "shortName";
    oClass.setShortName(db, shortName);
    assertEquals(shortName, oClass.getShortName());
    YTClass shorted = schema.getClass(shortName);
    Assert.assertNotNull(shorted);
    assertEquals(shortName, shorted.getShortName());
    OMetadataInternal intern = db.getMetadata();
    YTImmutableSchema immSchema = intern.getImmutableSchemaSnapshot();
    shorted = immSchema.getClass(shortName);
    Assert.assertNotNull(shorted);
    assertEquals(shortName, shorted.getShortName());
  }

  @Test
  public void testRename() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("ClassName");

    final OStorage storage = db.getStorage();
    final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) storage;
    final OWriteCache writeCache = paginatedStorage.getWriteCache();
    Assert.assertTrue(writeCache.exists("classname" + OPaginatedCluster.DEF_EXTENSION));

    oClass.setName(db, "ClassNameNew");

    assertFalse(writeCache.exists("classname" + OPaginatedCluster.DEF_EXTENSION));
    Assert.assertTrue(writeCache.exists("classnamenew" + OPaginatedCluster.DEF_EXTENSION));

    oClass.setName(db, "ClassName");

    assertFalse(writeCache.exists("classnamenew" + OPaginatedCluster.DEF_EXTENSION));
    Assert.assertTrue(writeCache.exists("classname" + OPaginatedCluster.DEF_EXTENSION));
  }

  @Test
  public void testRenameClusterAlreadyExists() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classOne = schema.createClass("ClassOne");
    YTClass classTwo = schema.createClass("ClassTwo");

    final int clusterId = db.addCluster("classthree");
    classTwo.addClusterId(db, clusterId);

    db.begin();
    YTEntityImpl document = new YTEntityImpl("ClassTwo");
    document.save("classthree");

    document = new YTEntityImpl("ClassTwo");
    document.save();

    document = new YTEntityImpl("ClassOne");
    document.save();
    db.commit();

    assertEquals(db.countClass("ClassTwo"), 2);
    assertEquals(db.countClass("ClassOne"), 1);

    classOne.setName(db, "ClassThree");

    final OStorage storage = db.getStorage();
    final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) storage;
    final OWriteCache writeCache = paginatedStorage.getWriteCache();

    Assert.assertTrue(writeCache.exists("classone" + OPaginatedCluster.DEF_EXTENSION));

    assertEquals(db.countClass("ClassTwo"), 2);
    assertEquals(db.countClass("ClassThree"), 1);

    classOne.setName(db, "ClassOne");
    Assert.assertTrue(writeCache.exists("classone" + OPaginatedCluster.DEF_EXTENSION));

    assertEquals(db.countClass("ClassTwo"), 2);
    assertEquals(db.countClass("ClassOne"), 1);
  }

  @Test
  public void testOClassAndOPropertyDescription() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("DescriptionTest");
    YTProperty property = oClass.createProperty(db, "property", YTType.STRING);
    oClass.setDescription(db, "DescriptionTest-class-description");
    property.setDescription(db, "DescriptionTest-property-description");
    assertEquals(oClass.getDescription(), "DescriptionTest-class-description");
    assertEquals(property.getDescription(), "DescriptionTest-property-description");
    oClass = oSchema.getClass("DescriptionTest");
    property = oClass.getProperty("property");
    assertEquals(oClass.getDescription(), "DescriptionTest-class-description");
    assertEquals(property.getDescription(), "DescriptionTest-property-description");

    oClass = db.getMetadata().getImmutableSchemaSnapshot().getClass("DescriptionTest");
    property = oClass.getProperty("property");
    assertEquals(oClass.getDescription(), "DescriptionTest-class-description");
    assertEquals(property.getDescription(), "DescriptionTest-property-description");
  }

  private String queryShortName() {
    String selectShortNameSQL =
        "select shortName from ( select expand(classes) from metadata:schema )"
            + " where name = \""
            + SHORTNAME_CLASS_NAME
            + "\"";
    try (YTResultSet result = db.query(selectShortNameSQL)) {
      String name = result.next().getProperty("shortName");
      assertFalse(result.hasNext());
      return name;
    }
  }
}
