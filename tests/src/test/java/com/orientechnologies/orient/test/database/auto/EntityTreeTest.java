/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
public class EntityTreeTest extends DocumentDBBaseTest {

  public EntityTreeTest() {}

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

  @Test
  public void testPersonSaving() {
    addGaribaldiAndBonaparte();
    Assert.assertTrue(
        database.query("select from Profile where nick = 'NBonaparte'").stream()
            .findAny()
            .isPresent());
  }

  @Test(dependsOnMethods = "testPersonSaving")
  public void testCityEquality() {
    List<ODocument> resultset =
        executeQuery("select from profile where location.city.name = 'Rome'");
    Assert.assertEquals(resultset.size(), 2);

    var p1 = resultset.get(0);
    var p2 = resultset.get(1);

    Assert.assertNotSame(p1, p2);
    Assert.assertSame(
        p1.getElementProperty("location").getElementProperty("city"),
        p2.getElementProperty("location").getElementProperty("city"));
  }

  @Test(dependsOnMethods = "testCityEquality")
  public void testSaveCircularLink() {
    database.begin();
    var winston = database.newInstance("Profile");

    winston.setProperty("nick", "WChurcill");
    winston.setProperty("name", "Winston");
    winston.setProperty("surname", "Churcill");

    var country = database.newInstance("Country");
    country.setProperty("name", "England");

    var city = database.newInstance("City");
    city.setProperty("name", "London");
    city.setProperty("country", country);

    var address = database.newInstance("Address");
    address.setProperty("type", "Residence");
    address.setProperty("city", city);
    address.setProperty("street", "unknown");

    winston.setProperty("location", address);

    var nicholas = database.newInstance("Profile");
    nicholas.setProperty("nick", "NChurcill");
    nicholas.setProperty("name", "Nicholas");
    nicholas.setProperty("surname", "Churcill");

    nicholas.setProperty("location", winston.getElementProperty("location"));

    nicholas.setProperty("invitedBy", winston);
    winston.setProperty("invitedBy", nicholas);

    database.save(nicholas);
    database.commit();
  }

  @Test(dependsOnMethods = "testSaveCircularLink")
  public void testSaveMultiCircular() {
    addBarackObamaAndFollowers();
  }

  @SuppressWarnings("unchecked")
  @Test(dependsOnMethods = "testSaveMultiCircular")
  public void testQueryMultiCircular() {
    List<ODocument> result =
        executeQuery("select * from Profile where name = 'Barack' and surname = 'Obama'");

    Assert.assertEquals(result.size(), 1);
    for (ODocument profile : result) {
      final Collection<OIdentifiable> followers = profile.field("followers");
      if (followers != null) {
        for (OIdentifiable follower : followers) {
          Assert.assertTrue(
              ((Collection<OIdentifiable>)
                      Objects.requireNonNull(follower.getElement().getProperty("followings")))
                  .contains(profile));
        }
      }
    }
  }

  @Test
  public void testSetFieldSize() {
    database.begin();
    var test = database.newInstance("JavaComplexTestClass");
    test.setProperty("set", new HashSet<>());

    for (int i = 0; i < 100; i++) {
      var child = database.newInstance("Child");
      child.setProperty("name", String.valueOf(i));
      test.<Set<OIdentifiable>>getProperty("set").add(child);
    }
    Assert.assertNotNull(test.<Set<OIdentifiable>>getProperty("set"));
    Assert.assertEquals(test.<Set<OIdentifiable>>getProperty("set").size(), 100);

    database.save(test);
    database.commit();

    // Assert.assertEquals(test.<Set<OIdentifiable>>getProperty("set").size(), 100);
    ORID rid = test.getIdentity();
    database.close();
    database = createSessionInstance();

    test = database.load(rid);
    Assert.assertNotNull(test.<Set<OIdentifiable>>getProperty("set"));
    for (OIdentifiable identifiable : test.<Set<OIdentifiable>>getProperty("set")) {
      var child = identifiable.getElement();
      Assert.assertNotNull(child.<String>getProperty("name"));
      Assert.assertTrue(Integer.parseInt(child.getProperty("name")) < 100);
      Assert.assertTrue(Integer.parseInt(child.getProperty("name")) >= 0);
    }
    Assert.assertEquals(test.<Set<OIdentifiable>>getProperty("set").size(), 100);
    database.begin();
    database.delete(database.bindToSession(test));
    database.commit();
  }

  @Test(dependsOnMethods = "testQueryMultiCircular")
  public void testCollectionsRemove() {
    var a = database.newInstance("JavaComplexTestClass");

    // LIST TEST
    var first = database.newInstance("Child");
    first.setProperty("name", "1");
    var second = database.newInstance("Child");
    second.setProperty("name", "2");
    var third = database.newInstance("Child");
    third.setProperty("name", "3");
    var fourth = database.newInstance("Child");
    fourth.setProperty("name", "4");
    var fifth = database.newInstance("Child");
    fifth.setProperty("name", "5");

    var set = new HashSet<OIdentifiable>();
    set.add(first);
    set.add(second);
    set.add(third);
    set.add(fourth);
    set.add(fifth);

    a.setProperty("set", set);

    var list = new ArrayList<OIdentifiable>();
    list.add(first);
    list.add(second);
    list.add(third);
    list.add(fourth);
    list.add(fifth);

    a.setProperty("list", list);

    a.<Set<OIdentifiable>>getProperty("set").remove(third);
    a.<List<OIdentifiable>>getProperty("list").remove(fourth);

    Assert.assertEquals(a.<Set<OIdentifiable>>getProperty("set").size(), 4);
    Assert.assertEquals(a.<List<OIdentifiable>>getProperty("list").size(), 4);

    database.begin();
    a = database.save(a);
    database.commit();

    a = database.bindToSession(a);
    ORID rid = a.getIdentity();
    Assert.assertEquals(a.<Set<OIdentifiable>>getProperty("set").size(), 4);
    Assert.assertEquals(a.<List<OIdentifiable>>getProperty("list").size(), 4);

    database.close();

    database = createSessionInstance();

    var loadedObj = database.loadElement(rid);

    Assert.assertEquals(loadedObj.<Set<Object>>getProperty("set").size(), 4);
    Assert.assertEquals(loadedObj.<Set<OIdentifiable>>getProperty("set").size(), 4);

    database.begin();
    database.delete(rid);
    database.commit();
  }

  @Test
  public void childNLevelUpdateTest() {
    database.begin();
    var p = database.newInstance("Planet");
    var near = database.newInstance("Planet");
    var sat = database.newInstance("Satellite");
    var satNear = database.newInstance("Satellite");
    sat.setProperty("diameter", 50);
    sat.setProperty("near", near);
    satNear.setProperty("diameter", 10);

    near.setProperty("satellites", Collections.singletonList(satNear));
    p.setProperty("satellites", Collections.singletonList(sat));

    database.save(p);
    database.commit();

    database.begin();
    ORID rid = p.getIdentity();
    p = database.load(rid);
    sat = p.<List<OIdentifiable>>getProperty("satellites").get(0).getElement();
    near = sat.getElementProperty("near");
    satNear = near.<List<OIdentifiable>>getProperty("satellites").get(0).getElement();
    Assert.assertEquals(satNear.<Long>getProperty("diameter"), 10);

    satNear.setProperty("diameter", 100);
    satNear.save();

    database.save(p);
    database.commit();

    p = database.load(rid);
    sat = p.<List<OIdentifiable>>getProperty("satellites").get(0).getElement();
    near = sat.getElementProperty("near");
    satNear = near.<List<OIdentifiable>>getProperty("satellites").get(0).getElement();
    Assert.assertEquals(satNear.<Long>getProperty("diameter"), 100);
  }

  @Test(dependsOnMethods = "childNLevelUpdateTest")
  public void childMapUpdateTest() {
    database.begin();
    var p = database.newInstance("Planet");
    p.setProperty("name", "Earth");
    p.setProperty("distanceSun", 1000);

    var sat = database.newInstance("Satellite");
    sat.setProperty("diameter", 50);
    sat.setProperty("name", "Moon");

    p.setProperty("satellitesMap", Collections.singletonMap(sat.<String>getProperty("name"), sat));
    database.save(p);
    database.commit();

    p = database.bindToSession(p);
    Assert.assertEquals(p.<Integer>getProperty("distanceSun"), 1000);
    Assert.assertEquals(p.getProperty("name"), "Earth");
    ORID rid = p.getIdentity();

    database.begin();
    p = database.load(rid);
    sat = p.<Map<String, OIdentifiable>>getProperty("satellitesMap").get("Moon").getElement();
    Assert.assertEquals(p.<Integer>getProperty("distanceSun"), 1000);
    Assert.assertEquals(p.getProperty("name"), "Earth");
    Assert.assertEquals(sat.<Long>getProperty("diameter"), 50);
    sat.setProperty("diameter", 500);

    database.save(p);
    database.commit();

    p = database.load(rid);
    sat = p.<Map<String, OIdentifiable>>getProperty("satellitesMap").get("Moon").getElement();
    Assert.assertEquals(sat.<Long>getProperty("diameter"), 500);
    Assert.assertEquals(p.<Integer>getProperty("distanceSun"), 1000);
    Assert.assertEquals(p.getProperty("name"), "Earth");
  }

  @Test(dependsOnMethods = "childMapUpdateTest")
  public void childMapNLevelUpdateTest() {
    var jupiter = database.newInstance("Planet");
    jupiter.setProperty("name", "Jupiter");
    jupiter.setProperty("distanceSun", 3000);
    var mercury = database.newInstance("Planet");
    mercury.setProperty("name", "Mercury");
    mercury.setProperty("distanceSun", 5000);
    var jupiterMoon = database.newInstance("Satellite");
    var mercuryMoon = database.newInstance("Satellite");
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

    database.begin();
    database.save(jupiter);
    database.commit();

    database.begin();
    ORID rid = jupiter.getIdentity();
    jupiter = database.load(rid);
    jupiterMoon =
        jupiter
            .<Map<String, OIdentifiable>>getProperty("satellitesMap")
            .get("JupiterMoon")
            .getElement();
    mercury = jupiterMoon.getElementProperty("near");
    mercuryMoon =
        mercury
            .<Map<String, OIdentifiable>>getProperty("satellitesMap")
            .get("MercuryMoon")
            .getElement();
    Assert.assertEquals(mercuryMoon.<Long>getProperty("diameter"), 10);
    Assert.assertEquals(mercuryMoon.getProperty("name"), "MercuryMoon");
    Assert.assertEquals(jupiterMoon.<Long>getProperty("diameter"), 50);
    Assert.assertEquals(jupiterMoon.getProperty("name"), "JupiterMoon");
    Assert.assertEquals(jupiter.getProperty("name"), "Jupiter");
    Assert.assertEquals(jupiter.<Integer>getProperty("distanceSun"), 3000);
    Assert.assertEquals(mercury.getProperty("name"), "Mercury");
    Assert.assertEquals(mercury.<Integer>getProperty("distanceSun"), 5000);
    mercuryMoon.setProperty("diameter", 100);
    database.save(jupiter);
    database.commit();

    database.close();
    database = createSessionInstance();
    jupiter = database.load(rid);
    jupiterMoon =
        jupiter
            .<Map<String, OIdentifiable>>getProperty("satellitesMap")
            .get("JupiterMoon")
            .getElement();
    mercury = jupiterMoon.getElementProperty("near");
    mercuryMoon =
        mercury
            .<Map<String, OIdentifiable>>getProperty("satellitesMap")
            .get("MercuryMoon")
            .getElement();
    Assert.assertEquals(mercuryMoon.<Long>getProperty("diameter"), 100);
    Assert.assertEquals(mercuryMoon.getProperty("name"), "MercuryMoon");
    Assert.assertEquals(jupiterMoon.<Long>getProperty("diameter"), 50);
    Assert.assertEquals(jupiterMoon.getProperty("name"), "JupiterMoon");
    Assert.assertEquals(jupiter.getProperty("name"), "Jupiter");
    Assert.assertEquals(jupiter.<Integer>getProperty("distanceSun"), 3000);
    Assert.assertEquals(mercury.getProperty("name"), "Mercury");
    Assert.assertEquals(mercury.<Integer>getProperty("distanceSun"), 5000);
    database.close();
  }

  @Test
  public void iteratorShouldTerminate() {
    database.begin();

    var person = database.newElement("Profile");
    person.setProperty("nick", "Guy1");
    person.setProperty("name", "Guy");
    person.setProperty("surname", "Ritchie");

    person = database.save(person);
    database.commit();

    database.begin();
    database.delete(database.bindToSession(person));
    database.commit();

    database.begin();
    var person2 = database.newElement("Profile");
    person2.setProperty("nick", "Guy2");
    person2.setProperty("name", "Guy");
    person2.setProperty("surname", "Brush");
    database.save(person2);

    var it = database.browseClass("Profile");
    while (it.hasNext()) {
      it.next();
    }

    database.commit();
  }

  @Test
  public void testSave() {
    database.begin();
    var parent1 = database.newElement("RefParent");
    parent1 = database.save(parent1);
    var parent2 = database.newElement("RefParent");
    parent2 = database.save(parent2);

    var child1 = database.newElement("RefChild");
    parent1.setProperty("children", Collections.singleton(child1));
    parent1 = database.save(parent1);

    var child2 = database.newElement("RefChild");
    parent2.setProperty("children", Collections.singleton(child2));
    database.save(parent2);
    database.commit();

    database.begin();
    parent1 = database.load(parent1.getIdentity());
    parent2 = database.load(parent2.getIdentity());

    var child3 = database.newElement("RefChild");

    var otherThing = database.newElement("OtherThing");
    child3.setProperty("otherThing", otherThing);

    otherThing.setProperty("relationToParent1", parent1);
    otherThing.setProperty("relationToParent2", parent2);

    parent1.<Set<OIdentifiable>>getProperty("children").add(child3);
    parent2.<Set<OIdentifiable>>getProperty("children").add(child3);

    database.save(parent1);
    database.save(parent2);

    database.commit();
  }

  private void createCascadeDeleteClass() {
    var schema = database.getSchema();
    if (schema.existsClass("JavaCascadeDeleteTestClass")) {
      schema.dropClass("JavaCascadeDeleteTestClass");
    }

    var child = schema.getClass("Child");
    OClass clazz = schema.createClass("JavaCascadeDeleteTestClass");
    clazz.createProperty("simpleClass", OType.LINK, schema.getClass("JavaSimpleTestClass"));
    clazz.createProperty("binary", OType.LINK);
    clazz.createProperty("name", OType.STRING);
    clazz.createProperty("set", OType.LINKSET, child);
    clazz.createProperty("children", OType.LINKMAP, child);
    clazz.createProperty("list", OType.LINKLIST, child);
  }

  private void createPlanetClasses() {
    var schema = database.getSchema();
    var satellite = schema.createClass("Satellite");
    var planet = schema.createClass("Planet");

    planet.createProperty("name", OType.STRING);
    planet.createProperty("distanceSun", OType.INTEGER);
    planet.createProperty("satellites", OType.LINKLIST, satellite);
    planet.createProperty("satellitesMap", OType.LINKMAP, satellite);

    satellite.createProperty("name", OType.STRING);
    satellite.createProperty("diameter", OType.LONG);
    satellite.createProperty("near", OType.LINK, planet);
  }

  private void createRefClasses() {
    var schema = database.getSchema();
    var refParent = schema.createClass("RefParent");
    var refChild = schema.createClass("RefChild");
    var otherThing = schema.createClass("OtherThing");

    refParent.createProperty("children", OType.LINKSET, refChild);
    refChild.createProperty("otherThing", OType.LINK, otherThing);

    otherThing.createProperty("relationToParent1", OType.LINK, refParent);
    otherThing.createProperty("relationToParent2", OType.LINK, refParent);
  }
}
