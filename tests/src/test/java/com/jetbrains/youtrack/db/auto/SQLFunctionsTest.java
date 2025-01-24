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
package com.jetbrains.youtrack.db.auto;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.security.SecurityManager;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionAbstract;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLFunctionsTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SQLFunctionsTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    generateCompanyData();
    generateProfiles();
    generateGraphData();
  }

  @Test
  public void queryMax() {
    ResultSet result = db.command("select max(id) as max from Account");

    assertNotNull(result.next().getProperty("max"));
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryMaxInline() {
    var result =
        db.query("select max(1,2,7,0,-2,3) as max").toList();

    Assert.assertEquals(result.size(), 1);
    for (var r : result) {
      Assert.assertNotNull(r.getProperty("max"));

      Assert.assertEquals(((Number) r.getProperty("max")).intValue(), 7);
    }
  }

  @Test
  public void queryMin() {
    ResultSet result = db.command("select min(id) as min from Account");

    Result d = result.next();
    Assert.assertNotNull(d.getProperty("min"));

    Assert.assertEquals(((Number) d.getProperty("min")).longValue(), 0L);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryMinInline() {
    var resultSet =
        db.query("select min(1,2,7,0,-2,3) as min").toList();

    Assert.assertEquals(resultSet.size(), 1);
    for (var r : resultSet) {
      Assert.assertNotNull(r.getProperty("min"));

      Assert.assertEquals(((Number) r.getProperty("min")).intValue(), -2);
    }
  }

  @Test
  public void querySum() {
    ResultSet result = db.command("select sum(id) as sum from Account");
    Result d = result.next();
    Assert.assertNotNull(d.getProperty("sum"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryCount() {
    ResultSet result = db.command("select count(*) as total from Account");
    Result d = result.next();
    Assert.assertNotNull(d.getProperty("total"));
    Assert.assertTrue(((Number) d.getProperty("total")).longValue() > 0);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  public void queryCountExtendsRestricted() {
    SchemaClass restricted = db.getMetadata().getSchema().getClass("ORestricted");
    Assert.assertNotNull(restricted);

    db.getMetadata().getSchema().createClass("QueryCountExtendsRestrictedClass", restricted);

    db.begin();
    SecurityUserImpl admin = db.getMetadata().getSecurity().getUser("admin");
    SecurityUserImpl reader = db.getMetadata().getSecurity().getUser("reader");

    @SuppressWarnings("deprecation")
    Role byPassRestrictedRole =
        db
            .getMetadata()
            .getSecurity()
            .createRole("byPassRestrictedRole");
    byPassRestrictedRole.addRule(db,
        Rule.ResourceGeneric.BYPASS_RESTRICTED, null, Role.PERMISSION_READ);
    byPassRestrictedRole.save(db);

    db
        .getMetadata()
        .getSecurity()
        .createUser("superReader", "superReader", "reader", "byPassRestrictedRole");

    EntityImpl docAdmin = ((EntityImpl) db.newEntity("QueryCountExtendsRestrictedClass"));
    docAdmin.setProperty(
        "_allowRead",
        new HashSet<Identifiable>(
            Collections.singletonList(admin.getIdentity().getIdentity())));

    docAdmin.save();
    db.commit();

    db.begin();
    EntityImpl docReader = ((EntityImpl) db.newEntity("QueryCountExtendsRestrictedClass"));
    docReader.setProperty("_allowRead",
        new HashSet<>(Collections.singletonList(reader.getIdentity())));
    docReader.save();
    db.commit();

    var resultSet =
        db.query("select count(*) from QueryCountExtendsRestrictedClass").toList();
    var count = resultSet.getFirst();
    Assert.assertEquals(count.<Object>getProperty("count(*)"), 2L);

    db.close();
    //noinspection deprecation
    db = createSessionInstance();

    resultSet =
        db.query("select count(*) as count from QueryCountExtendsRestrictedClass").toList();
    count = resultSet.getFirst();
    Assert.assertEquals(count.<Object>getProperty("count"), 2L);

    db.close();
    db = createSessionInstance("superReader", "superReader");

    resultSet =
        db.query("select count(*) as count from QueryCountExtendsRestrictedClass").toList();
    count = resultSet.getFirst();
    Assert.assertEquals(count.<Object>getProperty("count"), 2L);
  }

  @Test
  public void queryCountWithConditions() {
    SchemaClass indexed = db.getMetadata().getSchema().getOrCreateClass("Indexed");
    indexed.createProperty(db, "key", PropertyType.STRING);
    indexed.createIndex(db, "keyed", SchemaClass.INDEX_TYPE.NOTUNIQUE, "key");

    db.begin();
    db.<EntityImpl>newInstance("Indexed").setProperty("key", "one");
    db.<EntityImpl>newInstance("Indexed").setProperty("key", "two");
    db.commit();

    var resultSet =
        db.query("select count(*) as total from Indexed where key > 'one'").toList();

    Assert.assertEquals(resultSet.size(), 1);
    for (var result : resultSet) {
      Assert.assertNotNull(result.getProperty("total"));
      Assert.assertTrue(((Number) result.getProperty("total")).longValue() > 0);
    }
  }

  @Test
  public void queryDistinct() {
    var resultSet =
        db.query("select distinct(name) as name from City").toList();
    Assert.assertTrue(resultSet.size() > 1);

    Set<String> cities = new HashSet<>();
    for (var city : resultSet) {
      String cityName = city.getProperty("name");
      Assert.assertFalse(cities.contains(cityName));
      cities.add(cityName);
    }
  }

  @Test
  public void queryFunctionRenamed() {
    var result =
        db.query("select distinct(name) as dist from City").toList();

    Assert.assertTrue(result.size() > 1);
    for (var city : result) {
      Assert.assertTrue(city.hasProperty("dist"));
    }
  }

  @Test
  public void queryUnionAllAsAggregationNotRemoveDuplicates() {
    var result = db.query("select from City").toList();
    int count = result.size();

    result =
        db.query("select unionAll(name) as name from City").toList();
    Collection<Object> citiesFound = result.getFirst().getProperty("name");
    Assert.assertEquals(citiesFound.size(), count);
  }

  @Test
  public void querySetNotDuplicates() {
    var result =
        db.query("select set(name) as name from City").toList();

    Assert.assertEquals(result.size(), 1);

    Collection<Object> citiesFound = result.getFirst().getProperty("name");
    Assert.assertTrue(citiesFound.size() > 1);

    Set<String> cities = new HashSet<>();
    for (Object city : citiesFound) {
      Assert.assertFalse(cities.contains(city.toString()));
      cities.add(city.toString());
    }
  }

  @Test
  public void queryList() {
    var result =
        db.query("select list(name) as names from City").toList();

    Assert.assertFalse(result.isEmpty());

    for (var d : result) {
      List<Object> citiesFound = d.getProperty("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  public void testSelectMap() {
    var result =
        db
            .query("select list( 1, 4, 5.00, 'john', map( 'kAA', 'vAA' ) ) as myresult")
            .toList();

    Assert.assertEquals(result.size(), 1);

    var document = result.getFirst();
    @SuppressWarnings("rawtypes")
    List myresult = document.getProperty("myresult");
    Assert.assertNotNull(myresult);

    Assert.assertTrue(myresult.remove(Integer.valueOf(1)));
    Assert.assertTrue(myresult.remove(Integer.valueOf(4)));
    Assert.assertTrue(myresult.remove(Float.valueOf(5)));
    Assert.assertTrue(myresult.remove("john"));

    Assert.assertEquals(myresult.size(), 1);

    Assert.assertTrue(myresult.getFirst() instanceof Map, "The object is: " + myresult.getClass());
    @SuppressWarnings("rawtypes")
    Map map = (Map) myresult.getFirst();

    String value = (String) map.get("kAA");
    Assert.assertEquals(value, "vAA");

    Assert.assertEquals(map.size(), 1);
  }

  @Test
  public void querySet() {
    var result =
        db.query("select set(name) as names from City").toList();

    Assert.assertFalse(result.isEmpty());

    for (var d : result) {
      Set<Object> citiesFound = d.getProperty("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void queryMap() {
    var result =
        db.query("select map(name, country.name) as names from City").toList();

    Assert.assertFalse(result.isEmpty());

    for (var d : result) {
      Map<Object, Object> citiesFound = d.getProperty("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void queryUnionAllAsInline() {
    var result =
        db.query("select unionAll(out, in) as edges from V").toList();

    Assert.assertTrue(result.size() > 1);
    for (var d : result) {
      Assert.assertEquals(d.getPropertyNames().size(), 1);
      Assert.assertTrue(d.hasProperty("edges"));
    }
  }

  @Test
  public void queryComposedAggregates() {
    var result =
        db
            .query(
                "select MIN(id) as min, max(id) as max, AVG(id) as average, sum(id) as total"
                    + " from Account").toList();

    Assert.assertEquals(result.size(), 1);
    for (var d : result) {
      Assert.assertNotNull(d.getProperty("min"));
      Assert.assertNotNull(d.getProperty("max"));
      Assert.assertNotNull(d.getProperty("average"));
      Assert.assertNotNull(d.getProperty("total"));

      Assert.assertTrue(
          ((Number) d.getProperty("max")).longValue() > ((Number) d.getProperty(
              "average")).longValue());
      Assert.assertTrue(
          ((Number) d.getProperty("average")).longValue() >= ((Number) d.getProperty(
              "min")).longValue());
      Assert.assertTrue(
          ((Number) d.getProperty("total")).longValue() >= ((Number) d.getProperty(
              "max")).longValue(),
          "Total " + d.getProperty("total") + " max " + d.getProperty("max"));
    }
  }

  @Test
  public void queryFormat() {
    var result =
        db
            .query(
                "select format('%d - %s (%s)', nr, street, type, dummy ) as output from"
                    + " Account").toList();

    Assert.assertTrue(result.size() > 1);
    for (var d : result) {
      Assert.assertNotNull(d.getProperty("output"));
    }
  }

  @Test
  public void querySysdateNoFormat() {
    ResultSet result = db.command("select sysdate() as date from Account");

    Assert.assertTrue(result.hasNext());
    while (result.hasNext()) {
      Result d = result.next();
      Assert.assertNotNull(d.getProperty("date"));
    }
  }

  @Test
  public void querySysdateWithFormat() {
    var result =
        db.query("select sysdate('dd-MM-yyyy') as date from Account")
            .toList();

    Assert.assertTrue(result.size() > 1);
    for (var d : result) {
      Assert.assertNotNull(d.getProperty("date"));
    }
  }

  @Test
  public void queryDate() {
    ResultSet result = db.command("select count(*) as tot from Account");

    int tot = ((Number) result.next().getProperty("tot")).intValue();
    assertFalse(result.hasNext());

    db.begin();
    long updated =
        db.command("update Account set created = date()").next().getProperty("count");
    db.commit();

    Assert.assertEquals(updated, tot);

    String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);

    result =
        db.query(
            "select from Account where created <= date('"
                + dateFormat.format(new Date())
                + "', \""
                + pattern
                + "\")");

    Assert.assertEquals(result.stream().count(), tot);
    result =
        db.query(
            "select from Account where created <= date('"
                + dateFormat.format(new Date())
                + "', \""
                + pattern
                + "\")");
    while (result.hasNext()) {
      Result d = result.next();
      Assert.assertNotNull(d.getProperty("created"));
    }
  }

  @Test(expectedExceptions = CommandSQLParsingException.class)
  public void queryUndefinedFunction() {
    db.query("select blaaaa(salary) as max from Account")
        .toList();
  }

  @Test
  public void queryCustomFunction() {
    SQLEngine.getInstance()
        .registerFunction(
            "bigger",
            new SQLFunctionAbstract("bigger", 2, 2) {
              @Override
              public String getSyntax(DatabaseSession session) {
                return "bigger(<first>, <second>)";
              }

              @Override
              public Object execute(
                  Object iThis,
                  Identifiable iCurrentRecord,
                  Object iCurrentResult,
                  final Object[] iParams,
                  CommandContext iContext) {
                if (iParams[0] == null || iParams[1] == null)
                // CHECK BOTH EXPECTED PARAMETERS
                {
                  return null;
                }

                if (!(iParams[0] instanceof Number) || !(iParams[1] instanceof Number))
                // EXCLUDE IT FROM THE RESULT SET
                {
                  return null;
                }

                // USE DOUBLE TO AVOID LOSS OF PRECISION
                final double v1 = ((Number) iParams[0]).doubleValue();
                final double v2 = ((Number) iParams[1]).doubleValue();

                return Math.max(v1, v2);
              }
            });

    var result =
        db.query("select from Account where bigger(id,1000) = 1000").toList();

    Assert.assertFalse(result.isEmpty());
    for (var d : result) {
      Assert.assertTrue((Integer) d.getProperty("id") <= 1000);
    }

    SQLEngine.getInstance().unregisterFunction("bigger");
  }

  @Test
  public void queryAsLong() {
    long moreThanInteger = 1 + (long) Integer.MAX_VALUE;
    String sql =
        "select numberString.asLong() as value from ( select '"
            + moreThanInteger
            + "' as numberString from Account ) limit 1";
    var result = db.query(sql).toList();

    Assert.assertEquals(result.size(), 1);
    for (var d : result) {
      Assert.assertNotNull(d.getProperty("value"));
      Assert.assertTrue(d.getProperty("value") instanceof Long);
      Assert.assertEquals(d.<Object>getProperty("value"), moreThanInteger);
    }
  }

  @Test
  public void testHashMethod() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    var result =
        db
            .query("select name, name.hash() as n256, name.hash('sha-512') as n512 from OUser")
            .toList();

    Assert.assertFalse(result.isEmpty());
    for (var d : result) {
      final String name = d.getProperty("name");

      Assert.assertEquals(SecurityManager.createHash(name, "SHA-256"), d.getProperty("n256"));
      Assert.assertEquals(SecurityManager.createHash(name, "SHA-512"), d.getProperty("n512"));
    }
  }

  @Test
  public void testFirstFunction() {
    List<Long> sequence = new ArrayList<>(100);
    for (long i = 0; i < 100; ++i) {
      sequence.add(i);
    }

    db.begin();
    db.newEntity("V").setProperty("sequence", sequence, PropertyType.EMBEDDEDLIST);
    var newSequence = new ArrayList<>(sequence);
    newSequence.removeFirst();
    db.newEntity("V").setProperty("sequence", newSequence, PropertyType.EMBEDDEDLIST);
    db.commit();

    var result =
        db.query(
                "select first(sequence) as first from V where sequence is not null order by first")
            .toList();

    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0).<Object>getProperty("first"), 0L);
    Assert.assertEquals(result.get(1).<Object>getProperty("first"), 1L);
  }

  @Test
  public void testLastFunction() {
    List<Long> sequence = new ArrayList<>(100);
    for (long i = 0; i < 100; ++i) {
      sequence.add(i);
    }

    db.begin();
    db.newEntity("V").setProperty("sequence2", sequence);

    var newSequence = new ArrayList<>(sequence);
    newSequence.remove(sequence.size() - 1);

    db.newEntity("V").setProperty("sequence2", newSequence);
    db.commit();

    var result =
        db.query(
                "select last(sequence2) as last from V where sequence2 is not null order by last desc")
            .toList();

    Assert.assertEquals(result.size(), 2);

    Assert.assertEquals(result.get(0).<Object>getProperty("last"), 99L);
    Assert.assertEquals(result.get(1).<Object>getProperty("last"), 98L);
  }

  @Test
  public void querySplit() {
    String sql = "select v.split('-') as value from ( select '1-2-3' as v ) limit 1";

    var result = db.query(sql).toList();

    Assert.assertEquals(result.size(), 1);
    for (var d : result) {
      Assert.assertNotNull(d.getProperty("value"));
      Assert.assertTrue(d.getProperty("value").getClass().isArray());

      Object[] array = d.getProperty("value");

      Assert.assertEquals(array.length, 3);
      Assert.assertEquals(array[0], "1");
      Assert.assertEquals(array[1], "2");
      Assert.assertEquals(array[2], "3");
    }
  }
}
