package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 7/3/14
 */
@Test
public abstract class DocumentDBBaseTest extends BaseTest<ODatabaseSessionInternal> {

  protected static final int TOT_COMPANY_RECORDS = 10;
  protected static final int TOT_RECORDS_ACCOUNT = 100;

  protected DocumentDBBaseTest() {
  }

  @Parameters(value = "remote")
  protected DocumentDBBaseTest(boolean remote) {
    super(remote);
  }

  public DocumentDBBaseTest(boolean remote, String prefix) {
    super(remote, prefix);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    createBasicTestSchema();
  }

  @Override
  protected ODatabaseSessionInternal createSessionInstance(
      OxygenDB oxygenDB, String dbName, String user, String password) {
    var session = oxygenDB.open(dbName, user, password);
    return (ODatabaseSessionInternal) session;
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseSessionInternal db, Object... args) {
    return db.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseSessionInternal db, Map args) {
    return db.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseSessionInternal db) {
    return db.query(sql).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, Object... args) {
    return database.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, Map<?, ?> args) {
    return database.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql) {
    return database.query(sql).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected void addBarackObamaAndFollowers() {
    createProfileClass();

    database.begin();
    if (database.query("select from Profile where name = 'Barack' and surname = 'Obama'").stream()
        .findAny()
        .isEmpty()) {

      var bObama = database.newElement("Profile");
      bObama.setProperty("nick", "ThePresident");
      bObama.setProperty("name", "Barack");
      bObama.setProperty("surname", "Obama");
      bObama.setProperty("followings", Collections.emptySet());

      var follower1 = database.newElement("Profile");
      follower1.setProperty("nick", "PresidentSon1");
      follower1.setProperty("name", "Malia Ann");
      follower1.setProperty("surname", "Obama");
      follower1.setProperty("followings", Collections.singleton(bObama));
      follower1.setProperty("followers", Collections.emptySet());

      var follower2 = database.newElement("Profile");
      follower2.setProperty("nick", "PresidentSon2");
      follower2.setProperty("name", "Natasha");
      follower2.setProperty("surname", "Obama");
      follower2.setProperty("followings", Collections.singleton(bObama));
      follower2.setProperty("followers", Collections.emptySet());

      var followers = new HashSet<OElement>();
      followers.add(follower1);
      followers.add(follower2);

      bObama.setProperty("followers", followers);
      bObama.save();
    }

    database.commit();
  }

  protected void fillInAccountData() {
    Set<Integer> ids = new HashSet<>();
    for (int i = 0; i < TOT_RECORDS_ACCOUNT; i++) {
      ids.add(i);
    }

    byte[] binary;
    createAccountClass();

    final Set<Integer> accountClusterIds =
        Arrays.stream(database.getMetadata().getSchema().getClass("Account").getClusterIds())
            .asLongStream()
            .mapToObj(i -> (int) i)
            .collect(HashSet::new, HashSet::add, HashSet::addAll);

    if (database.query("select count(*) as count from Account").stream()
        .findFirst()
        .orElseThrow()
        .<Long>getProperty("count")
        == 0) {
      for (var id : ids) {
        database.begin();
        var element = database.newElement("Account");
        element.setProperty("id", id);
        element.setProperty("name", "Gipsy");
        element.setProperty("location", "Italy");
        element.setProperty("testLong", 10000000000L);
        element.setProperty("salary", id + 300);
        element.setProperty("extra", "This is an extra field not included in the schema");
        element.setProperty("value", (byte) 10);

        binary = new byte[100];
        for (int b = 0; b < binary.length; ++b) {
          binary[b] = (byte) b;
        }
        element.setProperty("binary", binary);
        element.save();
        if (!remoteDB) {
          Assert.assertTrue(accountClusterIds.contains(element.getIdentity().getClusterId()));
        }
        database.commit();
      }
    }
  }

  protected void generateProfiles() {
    createProfileClass();
    createCountryClass();
    createCityClass();

    database.executeInTx(
        () -> {
          addGaribaldiAndBonaparte();
          addBarackObamaAndFollowers();

          var count =
              database.query("select count(*) as count from Profile").stream()
                  .findFirst()
                  .orElseThrow()
                  .<Long>getProperty("count");

          if (count < 1_000) {
            for (int i = 0; i < 1_000 - count; i++) {
              var profile = database.newElement("Profile");
              profile.setProperty("nick", "generatedNick" + i);
              profile.setProperty("name", "generatedName" + i);
              profile.setProperty("surname", "generatedSurname" + i);
              profile.save();
            }
          }
        });
  }

  protected void addGaribaldiAndBonaparte() {
    database.executeInTx(
        () -> {
          if (database.query("select from Profile where nick = 'NBonaparte'").stream()
              .findAny()
              .isPresent()) {
            return;
          }

          var rome = addRome();
          var garibaldi = database.newInstance("Profile");
          garibaldi.setProperty("nick", "GGaribaldi");
          garibaldi.setProperty("name", "Giuseppe");
          garibaldi.setProperty("surname", "Garibaldi");

          var gAddress = database.newInstance("Address");
          gAddress.setProperty("type", "Residence");
          gAddress.setProperty("street", "Piazza Navona, 1");
          gAddress.setProperty("city", rome);
          garibaldi.setProperty("location", gAddress);

          var bonaparte = database.newInstance("Profile");
          bonaparte.setProperty("nick", "NBonaparte");
          bonaparte.setProperty("name", "Napoleone");
          bonaparte.setProperty("surname", "Bonaparte");
          bonaparte.setProperty("invitedBy", garibaldi);

          var bnAddress = database.newInstance("Address");
          bnAddress.setProperty("type", "Residence");
          bnAddress.setProperty("street", "Piazza di Spagna, 111");
          bnAddress.setProperty("city", rome);
          bonaparte.setProperty("location", bnAddress);

          bonaparte.save();
        });
  }

  private OElement addRome() {
    return database.computeInTx(
        () -> {
          var italy = addItaly();
          var city = database.newInstance("City");
          city.setProperty("name", "Rome");
          city.setProperty("country", italy);
          city.save();

          return city;
        });
  }

  private OElement addItaly() {
    return database.computeInTx(
        () -> {
          var italy = database.newElement("Country");
          italy.setProperty("name", "Italy");
          italy.save();

          return italy;
        });
  }

  protected void generateCompanyData() {
    fillInAccountData();
    createCompanyClass();

    if (database.query("select count(*) as count from Company").stream()
        .findFirst()
        .orElseThrow()
        .<Long>getProperty("count")
        > 0) {
      return;
    }

    var address = createRedmondAddress();

    for (int i = 0; i < TOT_COMPANY_RECORDS; ++i) {
      database.begin();
      var company = database.newInstance("Company");
      company.setProperty("id", i);
      company.setProperty("name", "Microsoft" + i);
      company.setProperty("employees", 100000 + i);
      company.setProperty("salary", 1000000000.0f + i);

      var addresses = new ArrayList<OElement>();
      addresses.add(database.bindToSession(address));
      company.setProperty("addresses", addresses);
      database.save(company);
      database.commit();
    }
  }

  protected OElement createRedmondAddress() {
    database.begin();
    var washington = database.newInstance("Country");
    washington.setProperty("name", "Washington");
    washington.save();

    var redmond = database.newInstance("City");
    redmond.setProperty("name", "Redmond");
    redmond.setProperty("country", washington);
    redmond.save();

    var address = database.newInstance("Address");
    address.setProperty("type", "Headquarter");
    address.setProperty("city", redmond);
    address.setProperty("street", "WA 98073-9717");
    address.save();

    database.commit();
    return address;
  }

  protected OClass createCountryClass() {
    if (database.getClass("Country") != null) {
      return database.getClass("Country");
    }

    var cls = database.createClass("Country");
    cls.createProperty(database, "name", OType.STRING);
    return cls;
  }

  protected OClass createCityClass() {
    var countryCls = createCountryClass();

    if (database.getClass("City") != null) {
      return database.getClass("City");
    }

    var cls = database.createClass("City");
    cls.createProperty(database, "name", OType.STRING);
    cls.createProperty(database, "country", OType.LINK, countryCls);

    return cls;
  }

  protected OClass createAddressClass() {
    if (database.getClass("Address") != null) {
      return database.getClass("Address");
    }

    var cityCls = createCityClass();
    var cls = database.createClass("Address");
    cls.createProperty(database, "type", OType.STRING);
    cls.createProperty(database, "street", OType.STRING);
    cls.createProperty(database, "city", OType.LINK, cityCls);

    return cls;
  }

  protected OClass createAccountClass() {
    if (database.getClass("Account") != null) {
      return database.getClass("Account");
    }

    var addressCls = createAddressClass();
    var cls = database.createClass("Account");
    cls.createProperty(database, "id", OType.INTEGER);
    cls.createProperty(database, "name", OType.STRING);
    cls.createProperty(database, "surname", OType.STRING);
    cls.createProperty(database, "birthDate", OType.DATE);
    cls.createProperty(database, "salary", OType.FLOAT);
    cls.createProperty(database, "addresses", OType.LINKLIST, addressCls);
    cls.createProperty(database, "thumbnail", OType.BINARY);
    cls.createProperty(database, "photo", OType.BINARY);

    return cls;
  }

  protected void createCompanyClass() {
    if (database.getClass("Company") != null) {
      return;
    }

    createAccountClass();
    var cls = database.createClassIfNotExist("Company", "Account");
    cls.createProperty(database, "employees", OType.INTEGER);
  }

  protected void createProfileClass() {
    if (database.getClass("Profile") != null) {
      return;
    }

    var addressCls = createAddressClass();
    var cls = database.createClass("Profile");
    cls.createProperty(database, "nick", OType.STRING)
        .setMin(database, "3")
        .setMax(database, "30")
        .createIndex(database, OClass.INDEX_TYPE.UNIQUE,
            new ODocument().field("ignoreNullValues", true));
    cls.createProperty(database, "followings", OType.LINKSET, cls);
    cls.createProperty(database, "followers", OType.LINKSET, cls);
    cls.createProperty(database, "name", OType.STRING)
        .setMin(database, "3")
        .setMax(database, "30")
        .createIndex(database, OClass.INDEX_TYPE.NOTUNIQUE);

    cls.createProperty(database, "surname", OType.STRING).setMin(database, "3")
        .setMax(database, "30");
    cls.createProperty(database, "location", OType.LINK, addressCls);
    cls.createProperty(database, "hash", OType.LONG);
    cls.createProperty(database, "invitedBy", OType.LINK, cls);
    cls.createProperty(database, "value", OType.INTEGER);

    cls.createProperty(database, "registeredOn", OType.DATETIME)
        .setMin(database, "2010-01-01 00:00:00");
    cls.createProperty(database, "lastAccessOn", OType.DATETIME)
        .setMin(database, "2010-01-01 00:00:00");
    cls.createProperty(database, "photo", OType.TRANSIENT);
  }

  protected OClass createInheritanceTestAbstractClass() {
    if (database.getClass("InheritanceTestAbstractClass") != null) {
      return database.getClass("InheritanceTestAbstractClass");
    }

    var cls = database.createClass("InheritanceTestAbstractClass");
    cls.createProperty(database, "cField", OType.INTEGER);
    return cls;
  }

  protected OClass createInheritanceTestBaseClass() {
    if (database.getClass("InheritanceTestBaseClass") != null) {
      return database.getClass("InheritanceTestBaseClass");
    }

    var abstractCls = createInheritanceTestAbstractClass();
    var cls = database.createClass("InheritanceTestBaseClass", abstractCls.getName());
    cls.createProperty(database, "aField", OType.STRING);

    return cls;
  }

  protected void createInheritanceTestClass() {
    if (database.getClass("InheritanceTestClass") != null) {
      return;
    }

    var baseCls = createInheritanceTestBaseClass();
    var cls = database.createClass("InheritanceTestClass", baseCls.getName());
    cls.createProperty(database, "bField", OType.STRING);
  }

  protected void createBasicTestSchema() {
    createCountryClass();
    createAddressClass();
    createCityClass();
    createAccountClass();
    createCompanyClass();
    createProfileClass();
    createStrictTestClass();
    createAnimalRaceClass();
    createWhizClass();

    if (database.getClusterIdByName("csv") == -1) {
      database.addCluster("csv");
    }

    if (database.getClusterIdByName("flat") == -1) {
      database.addCluster("flat");
    }

    if (database.getClusterIdByName("binary") == -1) {
      database.addCluster("binary");
    }
  }

  private void createWhizClass() {
    OClass account = createAccountClass();
    if (database.getMetadata().getSchema().existsClass("Whiz")) {
      return;
    }

    OClass whiz = database.getMetadata().getSchema().createClass("Whiz", 1, (OClass[]) null);
    whiz.createProperty(database, "id", OType.INTEGER);
    whiz.createProperty(database, "account", OType.LINK, account);
    whiz.createProperty(database, "date", OType.DATE).setMin(database, "2010-01-01");
    whiz.createProperty(database, "text", OType.STRING).setMandatory(database, true)
        .setMin(database, "1")
        .setMax(database, "140");
    whiz.createProperty(database, "replyTo", OType.LINK, account);
  }

  private void createAnimalRaceClass() {
    if (database.getMetadata().getSchema().existsClass("AnimalRace")) {
      return;
    }

    OClass animalRace =
        database.getMetadata().getSchema().createClass("AnimalRace", 1, (OClass[]) null);
    animalRace.createProperty(database, "name", OType.STRING);
    OClass animal = database.getMetadata().getSchema().createClass("Animal", 1, (OClass[]) null);
    animal.createProperty(database, "races", OType.LINKSET, animalRace);
    animal.createProperty(database, "name", OType.STRING);
  }

  private void createStrictTestClass() {
    if (database.getMetadata().getSchema().existsClass("StrictTest")) {
      return;
    }

    OClass strictTest =
        database.getMetadata().getSchema().createClass("StrictTest", 1, (OClass[]) null);
    strictTest.setStrictMode(database, true);
    strictTest.createProperty(database, "id", OType.INTEGER).isMandatory();
    strictTest.createProperty(database, "name", OType.STRING);
  }

  protected void createComplexTestClass() {
    if (database.getSchema().existsClass("JavaComplexTestClass")) {
      database.getSchema().dropClass("JavaComplexTestClass");
    }
    if (database.getSchema().existsClass("Child")) {
      database.getSchema().dropClass("Child");
    }

    var childCls = database.createClass("Child");
    childCls.createProperty(database, "name", OType.STRING);

    var cls = database.createClass("JavaComplexTestClass");

    cls.createProperty(database, "embeddedDocument", OType.EMBEDDED);
    cls.createProperty(database, "document", OType.LINK);
    cls.createProperty(database, "byteArray", OType.LINK);
    cls.createProperty(database, "name", OType.STRING);
    cls.createProperty(database, "child", OType.LINK, childCls);
    cls.createProperty(database, "stringMap", OType.EMBEDDEDMAP);
    cls.createProperty(database, "stringListMap", OType.EMBEDDEDMAP);
    cls.createProperty(database, "list", OType.LINKLIST, childCls);
    cls.createProperty(database, "set", OType.LINKSET, childCls);
    cls.createProperty(database, "duplicationTestSet", OType.LINKSET, childCls);
    cls.createProperty(database, "children", OType.LINKMAP, childCls);
    cls.createProperty(database, "stringSet", OType.EMBEDDEDSET);
    cls.createProperty(database, "embeddedList", OType.EMBEDDEDLIST);
    cls.createProperty(database, "embeddedSet", OType.EMBEDDEDSET);
    cls.createProperty(database, "embeddedChildren", OType.EMBEDDEDMAP);
    cls.createProperty(database, "mapObject", OType.EMBEDDEDMAP);
  }

  protected void createSimpleTestClass() {
    if (database.getSchema().existsClass("JavaSimpleTestClass")) {
      database.getSchema().dropClass("JavaSimpleTestClass");
    }

    var cls = database.createClass("JavaSimpleTestClass");
    cls.createProperty(database, "text", OType.STRING).setDefaultValue(database, "initTest");
    cls.createProperty(database, "numberSimple", OType.INTEGER).setDefaultValue(database, "0");
    cls.createProperty(database, "longSimple", OType.LONG).setDefaultValue(database, "0");
    cls.createProperty(database, "doubleSimple", OType.DOUBLE).setDefaultValue(database, "0");
    cls.createProperty(database, "floatSimple", OType.FLOAT).setDefaultValue(database, "0");
    cls.createProperty(database, "byteSimple", OType.BYTE).setDefaultValue(database, "0");
    cls.createProperty(database, "shortSimple", OType.SHORT).setDefaultValue(database, "0");
    cls.createProperty(database, "dateField", OType.DATETIME);
  }

  protected void generateGraphData() {
    if (database.getSchema().existsClass("GraphVehicle")) {
      return;
    }

    OClass vehicleClass = database.createVertexClass("GraphVehicle");
    database.createClass("GraphCar", vehicleClass.getName());
    database.createClass("GraphMotocycle", "GraphVehicle");

    database.begin();
    var carNode = database.newVertex("GraphCar");
    carNode.setProperty("brand", "Hyundai");
    carNode.setProperty("model", "Coupe");
    carNode.setProperty("year", 2003);
    carNode.save();

    var motoNode = database.newVertex("GraphMotocycle");
    motoNode.setProperty("brand", "Yamaha");
    motoNode.setProperty("model", "X-City 250");
    motoNode.setProperty("year", 2009);
    motoNode.save();

    database.commit();

    database.begin();
    carNode = database.bindToSession(carNode);
    motoNode = database.bindToSession(motoNode);
    database.newEdge(carNode, motoNode).save();

    List<OResult> result =
        database.query("select from GraphVehicle").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);
    for (OResult v : result) {
      Assert.assertTrue(v.getElement().get().getSchemaType().get().isSubClassOf(vehicleClass));
    }

    database.commit();
    result = database.query("select from GraphVehicle").stream().toList();
    Assert.assertEquals(result.size(), 2);

    OEdge edge1 = null;
    OEdge edge2 = null;

    for (OResult v : result) {
      Assert.assertTrue(v.getElement().get().getSchemaType().get().isSubClassOf("GraphVehicle"));

      if (v.getElement().get().getSchemaType().isPresent()
          && v.getElement().get().getSchemaType().get().getName().equals("GraphCar")) {
        Assert.assertEquals(
            CollectionUtils.size(
                database.<OVertex>load(v.getIdentity().get()).getEdges(ODirection.OUT)),
            1);
        edge1 =
            database
                .<OVertex>load(v.getIdentity().get())
                .getEdges(ODirection.OUT)
                .iterator()
                .next();
      } else {
        Assert.assertEquals(
            CollectionUtils.size(
                database.<OVertex>load(v.getIdentity().get()).getEdges(ODirection.IN)),
            1);
        edge2 =
            database.<OVertex>load(v.getIdentity().get()).getEdges(ODirection.IN).iterator().next();
      }
    }

    Assert.assertEquals(edge1, edge2);
  }
}
