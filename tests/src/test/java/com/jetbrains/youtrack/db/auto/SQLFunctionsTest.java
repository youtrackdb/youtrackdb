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
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserIml;
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
    ResultSet result = database.command("select max(id) as max from Account");

    assertNotNull(result.next().getProperty("max"));
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryMaxInline() {
    List<EntityImpl> result =
        database.query("select max(1,2,7,0,-2,3) as max").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("max"));

      Assert.assertEquals(((Number) d.field("max")).intValue(), 7);
    }
  }

  @Test
  public void queryMin() {
    ResultSet result = database.command("select min(id) as min from Account");

    Result d = result.next();
    Assert.assertNotNull(d.getProperty("min"));

    Assert.assertEquals(((Number) d.getProperty("min")).longValue(), 0L);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryMinInline() {
    List<EntityImpl> result =
        database.query("select min(1,2,7,0,-2,3) as min").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("min"));

      Assert.assertEquals(((Number) d.field("min")).intValue(), -2);
    }
  }

  @Test
  public void querySum() {
    ResultSet result = database.command("select sum(id) as sum from Account");
    Result d = result.next();
    Assert.assertNotNull(d.getProperty("sum"));
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void queryCount() {
    ResultSet result = database.command("select count(*) as total from Account");
    Result d = result.next();
    Assert.assertNotNull(d.getProperty("total"));
    Assert.assertTrue(((Number) d.getProperty("total")).longValue() > 0);
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  public void queryCountExtendsRestricted() {
    SchemaClass restricted = database.getMetadata().getSchema().getClass("ORestricted");
    Assert.assertNotNull(restricted);

    database.getMetadata().getSchema().createClass("QueryCountExtendsRestrictedClass", restricted);

    database.begin();
    SecurityUserIml admin = database.getMetadata().getSecurity().getUser("admin");
    SecurityUserIml reader = database.getMetadata().getSecurity().getUser("reader");

    @SuppressWarnings("deprecation")
    Role byPassRestrictedRole =
        database
            .getMetadata()
            .getSecurity()
            .createRole("byPassRestrictedRole", Role.ALLOW_MODES.DENY_ALL_BUT);
    byPassRestrictedRole.addRule(database,
        Rule.ResourceGeneric.BYPASS_RESTRICTED, null, Role.PERMISSION_READ);
    byPassRestrictedRole.save(dbName);

    database
        .getMetadata()
        .getSecurity()
        .createUser("superReader", "superReader", "reader", "byPassRestrictedRole");

    EntityImpl docAdmin = new EntityImpl("QueryCountExtendsRestrictedClass");
    docAdmin.field(
        "_allowRead",
        new HashSet<Identifiable>(
            Collections.singletonList(admin.getIdentity(database).getIdentity())));

    docAdmin.save();
    database.commit();

    database.begin();
    EntityImpl docReader = new EntityImpl("QueryCountExtendsRestrictedClass");
    docReader.field("_allowRead",
        new HashSet<>(Collections.singletonList(reader.getIdentity(database))));
    docReader.save();
    database.commit();

    List<EntityImpl> result =
        database.query("select count(*) from QueryCountExtendsRestrictedClass").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();
    EntityImpl count = result.get(0);
    Assert.assertEquals(2L, count.<Object>field("count(*)"));

    database.close();
    //noinspection deprecation
    database = createSessionInstance();

    result =
        database.query("select count(*) as count from QueryCountExtendsRestrictedClass").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();
    count = result.get(0);
    Assert.assertEquals(2L, count.<Object>field("count"));

    database.close();
    database = createSessionInstance("superReader", "superReader");

    result =
        database.query("select count(*) as count from QueryCountExtendsRestrictedClass").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();
    count = result.get(0);
    Assert.assertEquals(2L, count.<Object>field("count"));
  }

  @Test
  public void queryCountWithConditions() {
    SchemaClass indexed = database.getMetadata().getSchema().getOrCreateClass("Indexed");
    indexed.createProperty(database, "key", PropertyType.STRING);
    indexed.createIndex(database, "keyed", SchemaClass.INDEX_TYPE.NOTUNIQUE, "key");

    database.begin();
    database.<EntityImpl>newInstance("Indexed").field("key", "one").save();
    database.<EntityImpl>newInstance("Indexed").field("key", "two").save();
    database.commit();

    List<EntityImpl> result =
        database.query("select count(*) as total from Indexed where key > 'one'").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("total"));
      Assert.assertTrue(((Number) d.field("total")).longValue() > 0);
    }
  }

  @Test
  public void queryDistinct() {
    List<EntityImpl> result =
        database.query("select distinct(name) as name from City").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertTrue(result.size() > 1);

    Set<String> cities = new HashSet<String>();
    for (EntityImpl city : result) {
      String cityName = city.field("name");
      Assert.assertFalse(cities.contains(cityName));
      cities.add(cityName);
    }
  }

  @Test
  public void queryFunctionRenamed() {
    List<EntityImpl> result =
        database.query("select distinct(name) as dist from City").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertTrue(result.size() > 1);

    for (EntityImpl city : result) {
      Assert.assertTrue(city.containsField("dist"));
    }
  }

  @Test
  public void querySetNotDuplicates() {
    List<EntityImpl> result =
        database.query("select set(name) as name from City").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertEquals(result.size(), 1);

    Collection<Object> citiesFound = result.get(0).field("name");
    Assert.assertTrue(citiesFound.size() > 1);

    Set<String> cities = new HashSet<String>();
    for (Object city : citiesFound) {
      Assert.assertFalse(cities.contains(city.toString()));
      cities.add(city.toString());
    }
  }

  @Test
  public void queryList() {
    List<EntityImpl> result =
        database.query("select list(name) as names from City").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      List<Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  public void testSelectMap() {
    List<EntityImpl> result =
        database
            .query("select list( 1, 4, 5.00, 'john', map( 'kAA', 'vAA' ) ) as myresult")
            .stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertEquals(result.size(), 1);

    EntityImpl document = result.get(0);
    @SuppressWarnings("rawtypes")
    List myresult = document.field("myresult");
    Assert.assertNotNull(myresult);

    Assert.assertTrue(myresult.remove(Integer.valueOf(1)));
    Assert.assertTrue(myresult.remove(Integer.valueOf(4)));
    Assert.assertTrue(myresult.remove(Float.valueOf(5)));
    Assert.assertTrue(myresult.remove("john"));

    Assert.assertEquals(myresult.size(), 1);

    Assert.assertTrue(myresult.get(0) instanceof Map, "The object is: " + myresult.getClass());
    @SuppressWarnings("rawtypes")
    Map map = (Map) myresult.get(0);

    String value = (String) map.get("kAA");
    Assert.assertEquals(value, "vAA");

    Assert.assertEquals(map.size(), 1);
  }

  @Test
  public void querySet() {
    List<EntityImpl> result =
        database.query("select set(name) as names from City").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Set<Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void queryMap() {
    List<EntityImpl> result =
        database.query("select map(name, country.name) as names from City").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());

    for (EntityImpl d : result) {
      Map<Object, Object> citiesFound = d.field("names");
      Assert.assertTrue(citiesFound.size() > 1);
    }
  }

  @Test
  public void queryComposedAggregates() {
    List<EntityImpl> result =
        database
            .query(
                "select MIN(id) as min, max(id) as max, AVG(id) as average, sum(id) as total"
                    + " from Account")
            .stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("min"));
      Assert.assertNotNull(d.field("max"));
      Assert.assertNotNull(d.field("average"));
      Assert.assertNotNull(d.field("total"));

      Assert.assertTrue(
          ((Number) d.field("max")).longValue() > ((Number) d.field("average")).longValue());
      Assert.assertTrue(
          ((Number) d.field("average")).longValue() >= ((Number) d.field("min")).longValue());
      Assert.assertTrue(
          ((Number) d.field("total")).longValue() >= ((Number) d.field("max")).longValue(),
          "Total " + d.field("total") + " max " + d.field("max"));
    }
  }

  @Test
  public void queryFormat() {
    List<EntityImpl> result =
        database
            .query(
                "select format('%d - %s (%s)', nr, street, type, dummy ) as output from"
                    + " Account")
            .stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertTrue(result.size() > 1);
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("output"));
    }
  }

  @Test
  public void querySysdateNoFormat() {
    ResultSet result = database.command("select sysdate() as date from Account");

    Assert.assertTrue(result.hasNext());
    while (result.hasNext()) {
      Result d = result.next();
      Assert.assertNotNull(d.getProperty("date"));
    }
  }

  @Test
  public void querySysdateWithFormat() {
    List<EntityImpl> result =
        database.query("select sysdate('dd-MM-yyyy') as date from Account").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertTrue(result.size() > 1);
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("date"));
    }
  }

  @Test
  public void queryDate() {
    ResultSet result = database.command("select count(*) as tot from Account");

    int tot = ((Number) result.next().getProperty("tot")).intValue();
    assertFalse(result.hasNext());

    database.begin();
    long updated =
        database.command("update Account set created = date()").next().getProperty("count");
    database.commit();

    Assert.assertEquals(updated, tot);

    String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);

    result =
        database.query(
            "select from Account where created <= date('"
                + dateFormat.format(new Date())
                + "', \""
                + pattern
                + "\")");

    Assert.assertEquals(result.stream().count(), tot);
    result =
        database.query(
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
    //noinspection ResultOfMethodCallIgnored
    database.query("select blaaaa(salary) as max from Account").stream()
        .map(r -> (EntityImpl) r.toEntity())
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

    List<EntityImpl> result =
        database.query("select from Account where bigger(id,1000) = 1000").stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());
    for (EntityImpl d : result) {
      Assert.assertTrue((Integer) d.field("id") <= 1000);
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
    List<EntityImpl> result =
        database.query(sql).stream().map(r -> (EntityImpl) r.toEntity()).toList();

    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("value"));
      Assert.assertTrue(d.field("value") instanceof Long);
      Assert.assertEquals(moreThanInteger, d.<Object>field("value"));
    }
  }

  @Test
  public void testHashMethod() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    List<EntityImpl> result =
        database
            .query("select name, name.hash() as n256, name.hash('sha-512') as n512 from OUser")
            .stream()
            .map(r -> (EntityImpl) r.toEntity())
            .toList();

    Assert.assertFalse(result.isEmpty());
    for (EntityImpl d : result) {
      final String name = d.field("name");

      Assert.assertEquals(SecurityManager.createHash(name, "SHA-256"), d.field("n256"));
      Assert.assertEquals(SecurityManager.createHash(name, "SHA-512"), d.field("n512"));
    }
  }

  @Test
  public void testFirstFunction() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    List<Long> sequence = new ArrayList<Long>(100);
    for (long i = 0; i < 100; ++i) {
      sequence.add(i);
    }

    database.begin();
    new EntityImpl("V").field("sequence", sequence, PropertyType.EMBEDDEDLIST).save();
    var newSequence = new ArrayList<>(sequence);
    newSequence.remove(0);
    new EntityImpl("V").field("sequence", newSequence, PropertyType.EMBEDDEDLIST).save();
    database.commit();

    var result =
        database.query(
                "select first(sequence) as first from V where sequence is not null order by first")
            .toList();

    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0).<Object>getProperty("first"), 0L);
    Assert.assertEquals(result.get(1).<Object>getProperty("first"), 1L);
  }

  @Test
  public void testLastFunction() throws UnsupportedEncodingException, NoSuchAlgorithmException {
    List<Long> sequence = new ArrayList<Long>(100);
    for (long i = 0; i < 100; ++i) {
      sequence.add(i);
    }

    database.begin();
    new EntityImpl("V").field("sequence2", sequence).save();

    var newSequence = new ArrayList<>(sequence);
    newSequence.remove(sequence.size() - 1);

    new EntityImpl("V").field("sequence2", newSequence).save();
    database.commit();

    var result =
        database.query(
                "select last(sequence2) as last from V where sequence2 is not null order by last desc")
            .toList();

    Assert.assertEquals(result.size(), 2);

    Assert.assertEquals(result.get(0).<Object>getProperty("last"), 99L);
    Assert.assertEquals(result.get(1).<Object>getProperty("last"), 98L);
  }

  @Test
  public void querySplit() {
    String sql = "select v.split('-') as value from ( select '1-2-3' as v ) limit 1";

    List<EntityImpl> result =
        database.query(sql).stream().map(r -> (EntityImpl) r.toEntity()).toList();

    Assert.assertEquals(result.size(), 1);
    for (EntityImpl d : result) {
      Assert.assertNotNull(d.field("value"));
      Assert.assertTrue(d.field("value").getClass().isArray());

      Object[] array = d.field("value");

      Assert.assertEquals(array.length, 3);
      Assert.assertEquals(array[0], "1");
      Assert.assertEquals(array[1], "2");
      Assert.assertEquals(array[2], "3");
    }
  }
}
