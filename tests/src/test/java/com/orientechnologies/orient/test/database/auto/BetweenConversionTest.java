package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 9/12/14
 */
@Test
public class BetweenConversionTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public BetweenConversionTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass clazz = schema.createClass("BetweenConversionTest");
    clazz.createProperty(database, "a", YTType.INTEGER);
    clazz.createProperty(database, "ai", YTType.INTEGER);

    clazz.createIndex(database, "BetweenConversionTestIndex", YTClass.INDEX_TYPE.NOTUNIQUE, "ai");

    for (int i = 0; i < 10; i++) {
      YTEntityImpl document = new YTEntityImpl("BetweenConversionTest");
      document.field("a", i);
      document.field("ai", i);

      if (i < 5) {
        document.field("vl", "v1");
      } else {
        document.field("vl", "v2");
      }

      YTEntityImpl ed = new YTEntityImpl();
      ed.field("a", i);

      document.field("d", ed);

      database.begin();
      document.save();
      database.commit();
    }
  }

  public void testBetweenRightLeftIncluded() {
    final String query = "select from BetweenConversionTest where a >= 1 and a <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightLeftIncludedReverseOrder() {
    final String query = "select from BetweenConversionTest where a <= 3 and a >= 1";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightIncluded() {
    final String query = "select from BetweenConversionTest where a > 1 and a <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightIncludedReverse() {
    final String query = "select from BetweenConversionTest where a <= 3 and a > 1";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenLeftIncluded() {
    final String query = "select from BetweenConversionTest where a >= 1 and a < 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenLeftIncludedReverseOrder() {
    final String query = "select from BetweenConversionTest where  a < 3 and a >= 1";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetween() {
    final String query = "select from BetweenConversionTest where a > 1 and a < 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(List.of(2));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightLeftIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai >= 1 and ai <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where ai <= 3 and ai >= 1";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai > 1 and ai <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where ai <= 3 and ai > 1";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenLeftIncludedIndex() {
    final String query = "select from BetweenConversionTest where ai >= 1 and ai < 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenLeftIncludedReverseOrderIndex() {
    final String query = "select from BetweenConversionTest where  ai < 3 and ai >= 1";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 2);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenIndex() {
    final String query = "select from BetweenConversionTest where ai > 1 and ai < 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(List.of(2));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedDeepQuery() {
    final String query =
        "select from BetweenConversionTest where (vl = 'v1' and (vl <> 'v3' and (vl <> 'v2' and ((a"
            + " >= 1 and a <= 7) and vl = 'v1'))) and vl <> 'v4')";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
  }

  public void testBetweenRightLeftIncludedDeepQueryIndex() {
    final String query =
        "select from BetweenConversionTest where (vl = 'v1' and (vl <> 'v3' and (vl <> 'v2' and"
            + " ((ai >= 1 and ai <= 7) and vl = 'v1'))) and vl <> 'v4')";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("ai")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertEquals(explain.<Object>field("rangeQueryConvertedInBetween"), 1);
    Assert.assertTrue(
        ((Set<String>) explain.field("involvedIndexes")).contains("BetweenConversionTestIndex"));
  }

  public void testBetweenRightLeftIncludedDifferentFields() {
    final String query = "select from BetweenConversionTest where a >= 1 and ai <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenNotRangeQueryRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and a = 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(List.of(3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenNotRangeQueryLeft() {
    final String query = "select from BetweenConversionTest where a = 1 and a <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 1);
    List<Integer> values = new ArrayList<Integer>(List.of(1));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenRightLeftIncludedBothFieldsLeft() {
    final String query = "select from BetweenConversionTest where a >= ai and a <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 4);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenRightLeftIncludedBothFieldsRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and a <= ai";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 9);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenRightLeftIncludedFieldChainLeft() {
    final String query = "select from BetweenConversionTest where d.a >= 1 and a <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }

  public void testBetweenRightLeftIncludedFieldChainRight() {
    final String query = "select from BetweenConversionTest where a >= 1 and d.a <= 3";
    final List<YTEntityImpl> result = database.query(new OSQLSynchQuery<YTEntityImpl>(query));

    Assert.assertEquals(result.size(), 3);
    List<Integer> values = new ArrayList<Integer>(Arrays.asList(1, 2, 3));

    for (YTEntityImpl document : result) {
      Assert.assertTrue(values.remove((Integer) document.field("a")));
    }

    Assert.assertTrue(values.isEmpty());

    YTEntityImpl explain = database.command(new OCommandSQL("explain " + query)).execute(database);

    Assert.assertNull(explain.field("rangeQueryConvertedInBetween"));
  }
}
