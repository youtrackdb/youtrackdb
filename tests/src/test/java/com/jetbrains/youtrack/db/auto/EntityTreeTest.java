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

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class EntityTreeTest extends BaseDBTest {

  @Parameters(value = "remote")
  public EntityTreeTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void init() {
    createComplexTestClass();
    createSimpleTestClass();
    createCascadeDeleteClass();
    createPlanetClasses();
    createRefClasses();
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }


  @Test
  public void testPersonSaving() {
    addGaribaldiAndBonaparte();

    db.begin();
    Assert.assertTrue(
        db.query("select from Profile where nick = 'NBonaparte'").stream()
            .findAny()
            .isPresent());
    db.commit();
  }

  @Test(dependsOnMethods = "testPersonSaving")
  public void testCityEquality() {
    db.begin();
    var resultSet =
        executeQuery("select from profile where location.city.name = 'Rome'");
    Assert.assertEquals(resultSet.size(), 2);

    var p1 = resultSet.get(0);
    var p2 = resultSet.get(1);

    Assert.assertNotSame(p1, p2);
    Assert.assertSame(
        p1.getEntityProperty("location").getEntityProperty("city"),
        p2.getEntityProperty("location").getEntityProperty("city"));
    db.commit();
  }

  @Test(dependsOnMethods = "testCityEquality")
  public void testSaveCircularLink() {
    db.begin();
    var winston = db.newInstance("Profile");

    winston.setProperty("nick", "WChurcill");
    winston.setProperty("name", "Winston");
    winston.setProperty("surname", "Churcill");

    var country = db.newInstance("Country");
    country.setProperty("name", "England");

    var city = db.newInstance("City");
    city.setProperty("name", "London");
    city.setProperty("country", country);

    var address = db.newInstance("Address");
    address.setProperty("type", "Residence");
    address.setProperty("city", city);
    address.setProperty("street", "unknown");

    winston.setProperty("location", address);

    var nicholas = db.newInstance("Profile");
    nicholas.setProperty("nick", "NChurcill");
    nicholas.setProperty("name", "Nicholas");
    nicholas.setProperty("surname", "Churcill");

    nicholas.setProperty("location", winston.getEntityProperty("location"));

    nicholas.setProperty("invitedBy", winston);
    winston.setProperty("invitedBy", nicholas);

    db.save(nicholas);
    db.commit();
  }

  @Test(dependsOnMethods = "testSaveCircularLink")
  public void testSaveMultiCircular() {
    addBarackObamaAndFollowers();
  }

  @SuppressWarnings("unchecked")
  @Test(dependsOnMethods = "testSaveMultiCircular")
  public void testQueryMultiCircular() {
    db.begin();
    var resultSet =
        executeQuery("select * from Profile where name = 'Barack' and surname = 'Obama'");

    Assert.assertEquals(resultSet.size(), 1);
    for (var result : resultSet) {
      var profile = result.asEntity();
      final Collection<Identifiable> followers = profile.getProperty("followers");
      if (followers != null) {
        for (Identifiable follower : followers) {
          Assert.assertTrue(
              ((Collection<Identifiable>)
                  Objects.requireNonNull(follower.getEntity(db).getProperty("followings")))
                  .contains(profile));
        }
      }
    }
    db.commit();
  }

  @Test
  public void testSetFieldSize() {
    db.begin();
    var test = db.newInstance("JavaComplexTestClass");
    test.setProperty("set", new HashSet<>());

    for (int i = 0; i < 100; i++) {
      var child = db.newInstance("Child");
      child.setProperty("name", String.valueOf(i));
      test.<Set<Identifiable>>getProperty("set").add(child);
    }
    Assert.assertNotNull(test.<Set<Identifiable>>getProperty("set"));
    Assert.assertEquals(test.<Set<Identifiable>>getProperty("set").size(), 100);

    db.save(test);
    db.commit();

    // Assert.assertEquals(test.<Set<Identifiable>>getProperty("set").size(), 100);
    RID rid = test.getIdentity();
    db.close();
    db = createSessionInstance();

    db.begin();
    test = db.load(rid);
    Assert.assertNotNull(test.<Set<Identifiable>>getProperty("set"));
    for (Identifiable identifiable : test.<Set<Identifiable>>getProperty("set")) {
      var child = identifiable.getEntity(db);
      Assert.assertNotNull(child.<String>getProperty("name"));
      Assert.assertTrue(Integer.parseInt(child.getProperty("name")) < 100);
      Assert.assertTrue(Integer.parseInt(child.getProperty("name")) >= 0);
    }
    Assert.assertEquals(test.<Set<Identifiable>>getProperty("set").size(), 100);
    db.delete(db.bindToSession(test));
    db.commit();
  }

  @Test(dependsOnMethods = "testQueryMultiCircular")
  public void testCollectionsRemove() {
    var a = db.newInstance("JavaComplexTestClass");

    // LIST TEST
    var first = db.newInstance("Child");
    first.setProperty("name", "1");
    var second = db.newInstance("Child");
    second.setProperty("name", "2");
    var third = db.newInstance("Child");
    third.setProperty("name", "3");
    var fourth = db.newInstance("Child");
    fourth.setProperty("name", "4");
    var fifth = db.newInstance("Child");
    fifth.setProperty("name", "5");

    var set = new HashSet<Identifiable>();
    set.add(first);
    set.add(second);
    set.add(third);
    set.add(fourth);
    set.add(fifth);

    a.setProperty("set", set);

    var list = new ArrayList<Identifiable>();
    list.add(first);
    list.add(second);
    list.add(third);
    list.add(fourth);
    list.add(fifth);

    a.setProperty("list", list);

    a.<Set<Identifiable>>getProperty("set").remove(third);
    a.<List<Identifiable>>getProperty("list").remove(fourth);

    Assert.assertEquals(a.<Set<Identifiable>>getProperty("set").size(), 4);
    Assert.assertEquals(a.<List<Identifiable>>getProperty("list").size(), 4);

    db.begin();
    a = db.save(a);
    db.commit();

    db.begin();
    a = db.bindToSession(a);
    RID rid = a.getIdentity();
    Assert.assertEquals(a.<Set<Identifiable>>getProperty("set").size(), 4);
    Assert.assertEquals(a.<List<Identifiable>>getProperty("list").size(), 4);
    db.commit();

    db.close();

    db = createSessionInstance();

    db.begin();
    var loadedObj = db.loadEntity(rid);

    Assert.assertEquals(loadedObj.<Set<Object>>getProperty("set").size(), 4);
    Assert.assertEquals(loadedObj.<Set<Identifiable>>getProperty("set").size(), 4);

    db.delete(rid);
    db.commit();
  }

  @Test
  public void childNLevelUpdateTest() {
    db.begin();
    var p = db.newInstance("Planet");
    var near = db.newInstance("Planet");
    var sat = db.newInstance("Satellite");
    var satNear = db.newInstance("Satellite");
    sat.setProperty("diameter", 50);
    sat.setProperty("near", near);
    satNear.setProperty("diameter", 10);

    near.setProperty("satellites", Collections.singletonList(satNear));
    p.setProperty("satellites", Collections.singletonList(sat));

    db.save(p);
    db.commit();

    db.begin();
    RID rid = p.getIdentity();
    p = db.load(rid);
    sat = p.<List<Identifiable>>getProperty("satellites").getFirst().getEntity(db);
    near = sat.getEntityProperty("near");
    satNear = near.<List<Identifiable>>getProperty("satellites").getFirst().getEntity(db);
    Assert.assertEquals(satNear.<Long>getProperty("diameter"), 10);

    satNear.setProperty("diameter", 100);
    satNear.save();

    db.save(p);
    db.commit();

    db.begin();
    p = db.load(rid);
    sat = p.<List<Identifiable>>getProperty("satellites").getFirst().getEntity(db);
    near = sat.getEntityProperty("near");
    satNear = near.<List<Identifiable>>getProperty("satellites").getFirst().getEntity(db);
    Assert.assertEquals(satNear.<Long>getProperty("diameter"), 100);
    db.commit();
  }

  @Test(dependsOnMethods = "childNLevelUpdateTest")
  public void childMapUpdateTest() {
    db.begin();
    var p = db.newInstance("Planet");
    p.setProperty("name", "Earth");
    p.setProperty("distanceSun", 1000);

    var sat = db.newInstance("Satellite");
    sat.setProperty("diameter", 50);
    sat.setProperty("name", "Moon");

    p.setProperty("satellitesMap", Collections.singletonMap(sat.<String>getProperty("name"), sat));
    db.save(p);
    db.commit();

    db.begin();
    p = db.bindToSession(p);
    Assert.assertEquals(p.<Integer>getProperty("distanceSun"), 1000);
    Assert.assertEquals(p.getProperty("name"), "Earth");
    RID rid = p.getIdentity();

    p = db.load(rid);
    sat = p.<Map<String, Identifiable>>getProperty("satellitesMap").get("Moon").getEntity(db);
    Assert.assertEquals(p.<Integer>getProperty("distanceSun"), 1000);
    Assert.assertEquals(p.getProperty("name"), "Earth");
    Assert.assertEquals(sat.<Long>getProperty("diameter"), 50);
    sat.setProperty("diameter", 500);

    db.save(p);
    db.commit();

    db.begin();
    p = db.load(rid);
    sat = p.<Map<String, Identifiable>>getProperty("satellitesMap").get("Moon").getEntity(db);
    Assert.assertEquals(sat.<Long>getProperty("diameter"), 500);
    Assert.assertEquals(p.<Integer>getProperty("distanceSun"), 1000);
    Assert.assertEquals(p.getProperty("name"), "Earth");
    db.commit();
  }

  @Test(dependsOnMethods = "childMapUpdateTest")
  public void childMapNLevelUpdateTest() {
    var jupiter = db.newInstance("Planet");
    jupiter.setProperty("name", "Jupiter");
    jupiter.setProperty("distanceSun", 3000);
    var mercury = db.newInstance("Planet");
    mercury.setProperty("name", "Mercury");
    mercury.setProperty("distanceSun", 5000);
    var jupiterMoon = db.newInstance("Satellite");
    var mercuryMoon = db.newInstance("Satellite");
    jupiterMoon.setProperty("diameter", 50);
    jupiterMoon.setProperty("near", mercury);
    jupiterMoon.setProperty("name", "JupiterMoon");
    mercuryMoon.setProperty("diameter", 10);
    mercuryMoon.setProperty("name", "MercuryMoon");

    mercury.setProperty(
        "satellitesMap",
        Collections.singletonMap(mercuryMoon.<String>getProperty("name"), mercuryMoon));
    jupiter.setProperty(
        "satellitesMap",
        Collections.singletonMap(jupiterMoon.<String>getProperty("name"), jupiterMoon));

    db.begin();
    db.save(jupiter);
    db.commit();

    db.begin();
    RID rid = jupiter.getIdentity();
    jupiter = db.load(rid);
    jupiterMoon =
        jupiter
            .<Map<String, Identifiable>>getProperty("satellitesMap")
            .get("JupiterMoon")
            .getEntity(db);
    mercury = jupiterMoon.getEntityProperty("near");
    mercuryMoon =
        mercury
            .<Map<String, Identifiable>>getProperty("satellitesMap")
            .get("MercuryMoon")
            .getEntity(db);
    Assert.assertEquals(mercuryMoon.<Long>getProperty("diameter"), 10);
    Assert.assertEquals(mercuryMoon.getProperty("name"), "MercuryMoon");
    Assert.assertEquals(jupiterMoon.<Long>getProperty("diameter"), 50);
    Assert.assertEquals(jupiterMoon.getProperty("name"), "JupiterMoon");
    Assert.assertEquals(jupiter.getProperty("name"), "Jupiter");
    Assert.assertEquals(jupiter.<Integer>getProperty("distanceSun"), 3000);
    Assert.assertEquals(mercury.getProperty("name"), "Mercury");
    Assert.assertEquals(mercury.<Integer>getProperty("distanceSun"), 5000);
    mercuryMoon.setProperty("diameter", 100);
    db.save(jupiter);
    db.commit();

    db.close();
    db = createSessionInstance();

    db.begin();
    jupiter = db.load(rid);
    jupiterMoon =
        jupiter
            .<Map<String, Identifiable>>getProperty("satellitesMap")
            .get("JupiterMoon")
            .getEntity(db);
    mercury = jupiterMoon.getEntityProperty("near");
    mercuryMoon =
        mercury
            .<Map<String, Identifiable>>getProperty("satellitesMap")
            .get("MercuryMoon")
            .getEntity(db);
    Assert.assertEquals(mercuryMoon.<Long>getProperty("diameter"), 100);
    Assert.assertEquals(mercuryMoon.getProperty("name"), "MercuryMoon");
    Assert.assertEquals(jupiterMoon.<Long>getProperty("diameter"), 50);
    Assert.assertEquals(jupiterMoon.getProperty("name"), "JupiterMoon");
    Assert.assertEquals(jupiter.getProperty("name"), "Jupiter");
    Assert.assertEquals(jupiter.<Integer>getProperty("distanceSun"), 3000);
    Assert.assertEquals(mercury.getProperty("name"), "Mercury");
    Assert.assertEquals(mercury.<Integer>getProperty("distanceSun"), 5000);
    db.commit();
    db.close();
  }

  @Test
  public void iteratorShouldTerminate() {
    db.begin();

    var person = db.newEntity("Profile");
    person.setProperty("nick", "Guy1");
    person.setProperty("name", "Guy");
    person.setProperty("surname", "Ritchie");

    person = db.save(person);
    db.commit();

    db.begin();
    db.delete(db.bindToSession(person));
    db.commit();

    db.begin();
    var person2 = db.newEntity("Profile");
    person2.setProperty("nick", "Guy2");
    person2.setProperty("name", "Guy");
    person2.setProperty("surname", "Brush");
    db.save(person2);

    var it = db.browseClass("Profile");
    while (it.hasNext()) {
      it.next();
    }

    db.commit();
  }

  @Test
  public void testSave() {
    db.begin();
    var parent1 = db.newEntity("RefParent");
    parent1 = db.save(parent1);
    var parent2 = db.newEntity("RefParent");
    parent2 = db.save(parent2);

    var child1 = db.newEntity("RefChild");
    parent1.setProperty("children", Collections.singleton(child1));
    parent1 = db.save(parent1);

    var child2 = db.newEntity("RefChild");
    parent2.setProperty("children", Collections.singleton(child2));
    db.save(parent2);
    db.commit();

    db.begin();
    parent1 = db.load(parent1.getIdentity());
    parent2 = db.load(parent2.getIdentity());

    var child3 = db.newEntity("RefChild");

    var otherThing = db.newEntity("OtherThing");
    child3.setProperty("otherThing", otherThing);

    otherThing.setProperty("relationToParent1", parent1);
    otherThing.setProperty("relationToParent2", parent2);

    parent1.<Set<Identifiable>>getProperty("children").add(child3);
    parent2.<Set<Identifiable>>getProperty("children").add(child3);

    db.save(parent1);
    db.save(parent2);

    db.commit();
  }

  private void createCascadeDeleteClass() {
    var schema = db.getSchema();
    if (schema.existsClass("JavaCascadeDeleteTestClass")) {
      schema.dropClass("JavaCascadeDeleteTestClass");
    }

    var child = schema.getClass("Child");
    SchemaClass clazz = schema.createClass("JavaCascadeDeleteTestClass");
    clazz.createProperty(db, "simpleClass", PropertyType.LINK,
        schema.getClass("JavaSimpleTestClass"));
    clazz.createProperty(db, "binary", PropertyType.LINK);
    clazz.createProperty(db, "name", PropertyType.STRING);
    clazz.createProperty(db, "set", PropertyType.LINKSET, child);
    clazz.createProperty(db, "children", PropertyType.LINKMAP, child);
    clazz.createProperty(db, "list", PropertyType.LINKLIST, child);
  }

  private void createPlanetClasses() {
    var schema = db.getSchema();
    var satellite = schema.createClass("Satellite");
    var planet = schema.createClass("Planet");

    planet.createProperty(db, "name", PropertyType.STRING);
    planet.createProperty(db, "distanceSun", PropertyType.INTEGER);
    planet.createProperty(db, "satellites", PropertyType.LINKLIST, satellite);
    planet.createProperty(db, "satellitesMap", PropertyType.LINKMAP, satellite);

    satellite.createProperty(db, "name", PropertyType.STRING);
    satellite.createProperty(db, "diameter", PropertyType.LONG);
    satellite.createProperty(db, "near", PropertyType.LINK, planet);
  }

  private void createRefClasses() {
    var schema = db.getSchema();
    var refParent = schema.createClass("RefParent");
    var refChild = schema.createClass("RefChild");
    var otherThing = schema.createClass("OtherThing");

    refParent.createProperty(db, "children", PropertyType.LINKSET, refChild);
    refChild.createProperty(db, "otherThing", PropertyType.LINK, otherThing);

    otherThing.createProperty(db, "relationToParent1", PropertyType.LINK, refParent);
    otherThing.createProperty(db, "relationToParent2", PropertyType.LINK, refParent);
  }
}
