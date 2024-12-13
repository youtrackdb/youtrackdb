package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.index.CompositeIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.PropertyIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.PropertyRidBagIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.index.PropertyListIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.PropertyMapIndexDefinition;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLCreateIndexTest extends BaseDBTest {

  private static final PropertyType EXPECTED_PROP1_TYPE = PropertyType.DOUBLE;
  private static final PropertyType EXPECTED_PROP2_TYPE = PropertyType.INTEGER;

  @Parameters(value = "remote")
  public SQLCreateIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.createClass("sqlCreateIndexTestClass");
    oClass.createProperty(database, "prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty(database, "prop2", EXPECTED_PROP2_TYPE);
    oClass.createProperty(database, "prop3", PropertyType.EMBEDDEDMAP, PropertyType.INTEGER);
    oClass.createProperty(database, "prop5", PropertyType.EMBEDDEDLIST, PropertyType.INTEGER);
    oClass.createProperty(database, "prop6", PropertyType.EMBEDDEDLIST);
    oClass.createProperty(database, "prop7", PropertyType.EMBEDDEDMAP);
    oClass.createProperty(database, "prop8", PropertyType.INTEGER);
    oClass.createProperty(database, "prop9", PropertyType.LINKBAG);
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

    final Index index =
        database
            .getMetadata()
            .getIndexManager()
            .getIndex("sqlCreateIndexTestClass.prop1");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyIndexDefinition);
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

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndex");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new PropertyType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testCreateEmbeddedMapIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexEmbeddedMapIndex ON sqlCreateIndexTestClass (prop3) UNIQUE")
        .close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapIndex");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testOldStileCreateEmbeddedMapIndex() throws Exception {
    database.command("CREATE INDEX sqlCreateIndexTestClass.prop3 UNIQUE").close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop3");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.KEY);
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
    } catch (CommandSQLParsingException e) {
    }
    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
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
    } catch (CommandSQLParsingException e) {

    }
    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
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
    } catch (CommandSQLParsingException e) {

    }
    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
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

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapByKeyIndex");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.STRING});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateEmbeddedMapByValueIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexEmbeddedMapByValueIndex ON sqlCreateIndexTestClass (prop3"
                + " by value) UNIQUE")
        .close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedMapByValueIndex");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop3"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.INTEGER});
    Assert.assertEquals(index.getType(), "UNIQUE");
    Assert.assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateEmbeddedListIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexEmbeddedListIndex ON sqlCreateIndexTestClass (prop5)"
                + " NOTUNIQUE")
        .close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexEmbeddedListIndex");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyListIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop5"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateRidBagIndex() throws Exception {
    database
        .command(
            "CREATE INDEX sqlCreateIndexRidBagIndex ON sqlCreateIndexTestClass (prop9) NOTUNIQUE")
        .close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexRidBagIndex");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyRidBagIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop9"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.LINK});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateOldStileEmbeddedListIndex() throws Exception {
    database.command("CREATE INDEX sqlCreateIndexTestClass.prop5 NOTUNIQUE").close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop5");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyListIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop5"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");
  }

  public void testCreateOldStileRidBagIndex() throws Exception {
    database.command("CREATE INDEX sqlCreateIndexTestClass.prop9 NOTUNIQUE").close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop9");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyRidBagIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop9"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.LINK});
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
    } catch (IndexException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Linked type was not provided. You should provide linked type for embedded"
                      + " collections that are going to be indexed."));
    }
    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
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
    } catch (IndexException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Linked type was not provided. You should provide linked type for embedded"
                      + " collections that are going to be indexed."));
    }
    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
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

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndex2");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new PropertyType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
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
      database.command(new CommandSQL(query)).execute(database);
      Assert.fail();
    } catch (CommandExecutionException e) {
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
    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndex3");

    Assert.assertNull(index, "Index created while wrong query was executed");
  }

  public void testCompositeIndexWithMetadata() {
    database
        .command(
            "CREATE INDEX sqlCreateIndexCompositeIndexWithMetadata ON sqlCreateIndexTestClass"
                + " (prop1, prop2) UNIQUE metadata {v1:23, v2:\"val2\"}")
        .close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndexWithMetadata");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new PropertyType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");

    var metadata = index.getMetadata();

    Assert.assertEquals(metadata.get("v1"), 23);
    Assert.assertEquals(metadata.get("v2"), "val2");
  }

  public void testOldIndexWithMetadata() {
    database
        .command(
            "CREATE INDEX sqlCreateIndexTestClass.prop8 NOTUNIQUE  metadata {v1:23, v2:\"val2\"}")
        .close();

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexTestClass.prop8");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof PropertyIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), List.of("prop8"));
    Assert.assertEquals(indexDefinition.getTypes(), new PropertyType[]{PropertyType.INTEGER});
    Assert.assertEquals(index.getType(), "NOTUNIQUE");

    var metadata = index.getMetadata();

    Assert.assertEquals(metadata.get("v1"), 23);
    Assert.assertEquals(metadata.get("v2"), "val2");
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

    final Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("sqlCreateIndexTestClass")
            .getClassIndex(database, "sqlCreateIndexCompositeIndex2WithConfig");

    Assert.assertNotNull(index);

    final IndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new PropertyType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");

    var metadata = index.getMetadata();
    Assert.assertEquals(metadata.get("v1"), 23);
    Assert.assertEquals(metadata.get("v2"), "val2");
  }
}
