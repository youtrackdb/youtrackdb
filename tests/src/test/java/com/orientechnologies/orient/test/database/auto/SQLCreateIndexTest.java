package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyListIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyRidBagIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLCreateIndexTest extends DocumentDBBaseTest {

  private static final OType EXPECTED_PROP1_TYPE = OType.DOUBLE;
  private static final OType EXPECTED_PROP2_TYPE = OType.INTEGER;

  @Parameters(value = "remote")
  public SQLCreateIndexTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass("sqlCreateIndexTestClass");
    oClass.createProperty(database, "prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty(database, "prop2", EXPECTED_PROP2_TYPE);
    oClass.createProperty(database, "prop3", OType.EMBEDDEDMAP, OType.INTEGER);
    oClass.createProperty(database, "prop5", OType.EMBEDDEDLIST, OType.INTEGER);
    oClass.createProperty(database, "prop6", OType.EMBEDDEDLIST);
    oClass.createProperty(database, "prop7", OType.EMBEDDEDMAP);
    oClass.createProperty(database, "prop8", OType.INTEGER);
    oClass.createProperty(database, "prop9", OType.LINKBAG);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (database.isClosed()) {
      database = createSessionInstance();
    }

    database.command("delete from sqlCreateIndexTestClass").close();
    database.command("drop class sqlCreateIndexTestClass").close();

    super.afterClass();
  }

  @Test
  public void testOldSyntax() throws Exception {
    database.command("CREATE INDEX sqlCreateIndexTestClass.prop1 UNIQUE").close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop1");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields().get(0), "prop1");
    Assert.assertEquals(indexDefinition.getTypes()[0], EXPECTED_PROP1_TYPE);
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testCreateCompositeIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexCompositeIndex ON sqlCreateIndexTestClass (prop1, prop2)"
                + " UNIQUE")
        .close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new OType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testCreateEmbeddedMapIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexEmbeddedMapIndex ON sqlCreateIndexTestClass (prop3) UNIQUE")
        .close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testOldStileCreateEmbeddedMapIndex() throws Exception {
    database.command("CREATE INDEX sqlCreateIndexTestClass.prop3 UNIQUE").close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop3");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexOne() throws Exception {
    try {
      database
          .command(
              "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass"
                  + " (prop3 by ttt) UNIQUE")
          .close();
      Assert.fail();
    } catch (OCommandSQLParsingException e) {
    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapWrongSpecifierIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexTwo() throws Exception {
    try {
      database
          .command(
              "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass"
                  + " (prop3 b value) UNIQUE")
          .close();
      Assert.fail();
    } catch (OCommandSQLParsingException e) {

    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapWrongSpecifierIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateEmbeddedMapWrongSpecifierIndexThree() throws Exception {
    try {
      database
          .command(
              "CREATE INDEX sqlCreateIndexEmbeddedMapWrongSpecifierIndex ON sqlCreateIndexTestClass"
                  + " (prop3 by value t) UNIQUE")
          .close();
      Assert.fail();
    } catch (OCommandSQLParsingException e) {

    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapWrongSpecifierIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateEmbeddedMapByKeyIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexEmbeddedMapByKeyIndex ON sqlCreateIndexTestClass (prop3 by"
                + " key) UNIQUE")
        .close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapByKeyIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateEmbeddedMapByValueIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexEmbeddedMapByValueIndex ON sqlCreateIndexTestClass (prop3"
                + " by value) UNIQUE")
        .close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapByValueIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.INTEGER});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateEmbeddedListIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexEmbeddedListIndex ON sqlCreateIndexTestClass (prop5)"
                + " NOTUNIQUE")
        .close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedListIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyListIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop5"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateRidBagIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexRidBagIndex ON sqlCreateIndexTestClass (prop9) NOTUNIQUE")
        .close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexRidBagIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyRidBagIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop9"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.LINK});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateOldStileEmbeddedListIndex() throws Exception {
    database.command("CREATE INDEX sqlCreateIndexTestClass.prop5 NOTUNIQUE").close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop5");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyListIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop5"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateOldStileRidBagIndex() throws Exception {
    database.command("CREATE INDEX sqlCreateIndexTestClass.prop9 NOTUNIQUE").close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop9");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyRidBagIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop9"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.LINK});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  @Test
  public void testCreateEmbeddedListWithoutLinkedTypeIndex() throws Exception {
    try {
      database
          .command(
              "CREATE INDEX sqlCreateIndexEmbeddedListWithoutLinkedTypeIndex ON"
                  + " sqlCreateIndexTestClass (prop6) UNIQUE")
          .close();
      Assert.fail();
    } catch (OIndexException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Linked type was not provided. You should provide linked type for embedded"
                      + " collections that are going to be indexed."));
    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedListWithoutLinkedTypeIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateEmbeddedMapWithoutLinkedTypeIndex() throws Exception {
    try {
      database
          .command(
              "CREATE INDEX sqlCreateIndexEmbeddedMapWithoutLinkedTypeIndex ON"
                  + " sqlCreateIndexTestClass (prop7 by value) UNIQUE")
          .close();
      Assert.fail();
    } catch (OIndexException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Linked type was not provided. You should provide linked type for embedded"
                      + " collections that are going to be indexed."));
    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapWithoutLinkedTypeIndex");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  @Test
  public void testCreateCompositeIndexWithTypes() throws Exception {
    final String query =
        "CREATE INDEX sqlCreateIndexCompositeIndex2 ON sqlCreateIndexTestClass (prop1,"
            + " prop2) UNIQUE "
            + EXPECTED_PROP1_TYPE
            + ", "
            + EXPECTED_PROP2_TYPE;

    database.command(query).close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndex2");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new OType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testCreateCompositeIndexWithWrongTypes() throws Exception {
    final String query =
        "CREATE INDEX sqlCreateIndexCompositeIndex3 ON sqlCreateIndexTestClass (prop1,"
            + " prop2) UNIQUE "
            + EXPECTED_PROP1_TYPE
            + ", "
            + EXPECTED_PROP1_TYPE;

    try {
      database.command(new OCommandSQL(query)).execute(database);
      Assert.fail();
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Error on execution of command: sql.CREATE INDEX sqlCreateIndexCompositeIndex3"
                      + " ON"));

      Throwable cause = e;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }

      Assert.assertEquals(cause.getClass(), IllegalArgumentException.class);
    }
    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndex3");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  public void testCompositeIndexWithMetadata() {
    database
        .command(
            "CREATE INDEX sqlCreateIndexCompositeIndexWithMetadata ON sqlCreateIndexTestClass"
                + " (prop1, prop2) UNIQUE metadata {v1:23, v2:\"val2\"}")
        .close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndexWithMetadata");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new OType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");

    ODocument metadata = index.getMetadata();

    Assert.assertEquals(metadata.<Object>field("v1"), 23);
    Assert.assertEquals(metadata.field("v2"), "val2");
  }

  public void testOldIndexWithMetadata() {
    database
        .command(
            "CREATE INDEX sqlCreateIndexTestClass.prop8 NOTUNIQUE  metadata {v1:23, v2:\"val2\"}")
        .close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop8");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OPropertyIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop8"));
    Assert.assertEquals(indexDefinition.getTypes(), new OType[]{OType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");

    ODocument metadata = index.getMetadata();

    Assert.assertEquals(metadata.<Object>field("v1"), 23);
    Assert.assertEquals(metadata.field("v2"), "val2");
  }

  public void testCreateCompositeIndexWithTypesAndMetadata() throws Exception {
    final String query =
        "CREATE INDEX sqlCreateIndexCompositeIndex2WithConfig ON sqlCreateIndexTestClass"
            + " (prop1, prop2) UNIQUE "
            + EXPECTED_PROP1_TYPE
            + ", "
            + EXPECTED_PROP2_TYPE
            + " metadata {v1:23, v2:\"val2\"}";

    database.command(query).close();

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndex2WithConfig");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new OType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");

    ODocument metadata = index.getMetadata();
    Assert.assertEquals(metadata.<Object>field("v1"), 23);
    Assert.assertEquals(metadata.field("v2"), "val2");
  }
}
