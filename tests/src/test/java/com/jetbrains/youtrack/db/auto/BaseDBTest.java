package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.query.ExecutionPlan;
import com.jetbrains.youtrack.db.api.query.ExecutionStep;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionStepInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.FetchFromIndexStep;
import java.util.ArrayList;
import java.util.Arrays;
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
public abstract class BaseDBTest extends BaseTest<DatabaseSessionInternal> {

  protected static final int TOT_COMPANY_RECORDS = 10;
  protected static final int TOT_RECORDS_ACCOUNT = 100;

  protected BaseDBTest() {
  }

  @Parameters(value = "remote")
  protected BaseDBTest(boolean remote) {
    super(remote);
  }

  public BaseDBTest(boolean remote, String prefix) {
    super(remote, prefix);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();

    createBasicTestSchema();
  }

  @Override
  protected DatabaseSessionInternal createSessionInstance(
      YouTrackDB youTrackDB, String dbName, String user, String password) {
    var session = youTrackDB.open(dbName, user, password);
    return (DatabaseSessionInternal) session;
  }

  protected List<Result> executeQuery(String sql, DatabaseSessionInternal db,
      Object... args) {
    return db.query(sql, args).stream()
        .toList();
  }

  protected List<Result> executeQuery(String sql, DatabaseSessionInternal db, Map args) {
    return db.query(sql, args).stream()
        .toList();
  }

  protected List<Result> executeQuery(String sql, DatabaseSessionInternal db) {
    return db.query(sql).stream()
        .toList();
  }

  protected List<Result> executeQuery(String sql, Object... args) {
    return db.query(sql, args).stream()
        .toList();
  }

  protected List<Result> executeQuery(String sql, Map<?, ?> args) {
    return db.query(sql, args).stream()
        .toList();
  }

  protected List<Result> executeQuery(String sql) {
    return db.query(sql).stream()
        .toList();
  }

  protected void addBarackObamaAndFollowers() {
    createProfileClass();

    db.begin();
    if (db.query("select from Profile where name = 'Barack' and surname = 'Obama'").stream()
        .findAny()
        .isEmpty()) {

      var bObama = db.newEntity("Profile");
      bObama.setProperty("nick", "ThePresident");
      bObama.setProperty("name", "Barack");
      bObama.setProperty("surname", "Obama");
      bObama.getOrCreateLinkSet("followings");

      var follower1 = db.newEntity("Profile");
      follower1.setProperty("nick", "PresidentSon1");
      follower1.setProperty("name", "Malia Ann");
      follower1.setProperty("surname", "Obama");

      follower1.getOrCreateLinkSet("followings").add(bObama);
      follower1.getOrCreateLinkSet("followers");

      var follower2 = db.newEntity("Profile");
      follower2.setProperty("nick", "PresidentSon2");
      follower2.setProperty("name", "Natasha");
      follower2.setProperty("surname", "Obama");
      follower2.getOrCreateLinkSet("followings").add(bObama);
      follower2.getOrCreateLinkSet("followers");

      var followers = new HashSet<Entity>();
      followers.add(follower1);
      followers.add(follower2);

      bObama.getOrCreateLinkSet("followers").addAll(followers);
      bObama.save();
    }

    db.commit();
  }

  protected void fillInAccountData() {
    Set<Integer> ids = new HashSet<>();
    for (var i = 0; i < TOT_RECORDS_ACCOUNT; i++) {
      ids.add(i);
    }

    byte[] binary;
    createAccountClass();

    final Set<Integer> accountClusterIds =
        Arrays.stream(db.getMetadata().getSchema().getClass("Account").getClusterIds())
            .asLongStream()
            .mapToObj(i -> (int) i)
            .collect(HashSet::new, HashSet::add, HashSet::addAll);

    if (db.query("select count(*) as count from Account").stream()
        .findFirst()
        .orElseThrow()
        .<Long>getProperty("count")
        == 0) {
      for (var id : ids) {
        db.begin();
        var element = db.newEntity("Account");
        element.setProperty("id", id);
        element.setProperty("name", "Gipsy");
        element.setProperty("location", "Italy");
        element.setProperty("testLong", 10000000000L);
        element.setProperty("salary", id + 300);
        element.setProperty("extra", "This is an extra field not included in the schema");
        element.setProperty("value", (byte) 10);

        binary = new byte[100];
        for (var b = 0; b < binary.length; ++b) {
          binary[b] = (byte) b;
        }
        element.setProperty("binary", binary);
        element.save();
        if (!remoteDB) {
          Assert.assertTrue(accountClusterIds.contains(element.getIdentity().getClusterId()));
        }
        db.commit();
      }
    }
  }

  protected void generateProfiles() {
    createProfileClass();
    createCountryClass();
    createCityClass();

    db.executeInTx(
        () -> {
          addGaribaldiAndBonaparte();
          addBarackObamaAndFollowers();

          var count =
              db.query("select count(*) as count from Profile").stream()
                  .findFirst()
                  .orElseThrow()
                  .<Long>getProperty("count");

          if (count < 1_000) {
            for (var i = 0; i < 1_000 - count; i++) {
              var profile = db.newEntity("Profile");
              profile.setProperty("nick", "generatedNick" + i);
              profile.setProperty("name", "generatedName" + i);
              profile.setProperty("surname", "generatedSurname" + i);
              profile.save();
            }
          }
        });
  }

  protected void addGaribaldiAndBonaparte() {
    db.executeInTx(
        () -> {
          if (db.query("select from Profile where nick = 'NBonaparte'").stream()
              .findAny()
              .isPresent()) {
            return;
          }

          var rome = addRome();
          var garibaldi = db.newInstance("Profile");
          garibaldi.setProperty("nick", "GGaribaldi");
          garibaldi.setProperty("name", "Giuseppe");
          garibaldi.setProperty("surname", "Garibaldi");

          var gAddress = db.newInstance("Address");
          gAddress.setProperty("type", "Residence");
          gAddress.setProperty("street", "Piazza Navona, 1");
          gAddress.setProperty("city", rome);
          garibaldi.setProperty("location", gAddress);

          var bonaparte = db.newInstance("Profile");
          bonaparte.setProperty("nick", "NBonaparte");
          bonaparte.setProperty("name", "Napoleone");
          bonaparte.setProperty("surname", "Bonaparte");
          bonaparte.setProperty("invitedBy", garibaldi);

          var bnAddress = db.newInstance("Address");
          bnAddress.setProperty("type", "Residence");
          bnAddress.setProperty("street", "Piazza di Spagna, 111");
          bnAddress.setProperty("city", rome);
          bonaparte.setProperty("location", bnAddress);

          bonaparte.save();
        });
  }

  private Entity addRome() {
    return db.computeInTx(
        () -> {
          var italy = addItaly();
          var city = db.newInstance("City");
          city.setProperty("name", "Rome");
          city.setProperty("country", italy);
          city.save();

          return city;
        });
  }

  private Entity addItaly() {
    return db.computeInTx(
        () -> {
          var italy = db.newEntity("Country");
          italy.setProperty("name", "Italy");
          italy.save();

          return italy;
        });
  }

  protected void generateCompanyData() {
    fillInAccountData();
    createCompanyClass();

    if (db.query("select count(*) as count from Company").stream()
        .findFirst()
        .orElseThrow()
        .<Long>getProperty("count")
        > 0) {
      return;
    }

    var address = createRedmondAddress();

    for (var i = 0; i < TOT_COMPANY_RECORDS; ++i) {
      db.begin();
      var company = db.newInstance("Company");
      company.setProperty("id", i);
      company.setProperty("name", "Microsoft" + i);
      company.setProperty("employees", 100000 + i);
      company.setProperty("salary", 1000000000.0f + i);

      var addresses = new ArrayList<Entity>();
      addresses.add(db.bindToSession(address));
      company.setProperty("addresses", addresses);
      db.commit();
    }
  }

  protected Entity createRedmondAddress() {
    db.begin();
    var washington = db.newInstance("Country");
    washington.setProperty("name", "Washington");
    washington.save();

    var redmond = db.newInstance("City");
    redmond.setProperty("name", "Redmond");
    redmond.setProperty("country", washington);
    redmond.save();

    var address = db.newInstance("Address");
    address.setProperty("type", "Headquarter");
    address.setProperty("city", redmond);
    address.setProperty("street", "WA 98073-9717");
    address.save();

    db.commit();
    return address;
  }

  protected SchemaClass createCountryClass() {
    if (db.getClass("Country") != null) {
      return db.getClass("Country");
    }

    var cls = db.createClass("Country");
    cls.createProperty(db, "name", PropertyType.STRING);
    return cls;
  }

  protected SchemaClass createCityClass() {
    var countryCls = createCountryClass();

    if (db.getClass("City") != null) {
      return db.getClass("City");
    }

    var cls = db.createClass("City");
    cls.createProperty(db, "name", PropertyType.STRING);
    cls.createProperty(db, "country", PropertyType.LINK, countryCls);

    return cls;
  }

  protected SchemaClass createAddressClass() {
    if (db.getClass("Address") != null) {
      return db.getClass("Address");
    }

    var cityCls = createCityClass();
    var cls = db.createClass("Address");
    cls.createProperty(db, "type", PropertyType.STRING);
    cls.createProperty(db, "street", PropertyType.STRING);
    cls.createProperty(db, "city", PropertyType.LINK, cityCls);

    return cls;
  }

  protected SchemaClass createAccountClass() {
    if (db.getClass("Account") != null) {
      return db.getClass("Account");
    }

    var addressCls = createAddressClass();
    var cls = db.createClass("Account");
    cls.createProperty(db, "id", PropertyType.INTEGER);
    cls.createProperty(db, "name", PropertyType.STRING);
    cls.createProperty(db, "surname", PropertyType.STRING);
    cls.createProperty(db, "birthDate", PropertyType.DATE);
    cls.createProperty(db, "salary", PropertyType.FLOAT);
    cls.createProperty(db, "addresses", PropertyType.LINKLIST, addressCls);
    cls.createProperty(db, "thumbnail", PropertyType.BINARY);
    cls.createProperty(db, "photo", PropertyType.BINARY);

    return cls;
  }

  protected void createCompanyClass() {
    if (db.getClass("Company") != null) {
      return;
    }

    createAccountClass();
    var cls = db.createClassIfNotExist("Company", "Account");
    cls.createProperty(db, "employees", PropertyType.INTEGER);
  }

  protected void createProfileClass() {
    if (db.getClass("Profile") != null) {
      return;
    }

    var addressCls = createAddressClass();
    var cls = db.createClass("Profile");
    cls.createProperty(db, "nick", PropertyType.STRING)
        .setMin(db, "3")
        .setMax(db, "30")
        .createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE,
            Map.of("ignoreNullValues", true));
    cls.createProperty(db, "followings", PropertyType.LINKSET, cls);
    cls.createProperty(db, "followers", PropertyType.LINKSET, cls);
    cls.createProperty(db, "name", PropertyType.STRING)
        .setMin(db, "3")
        .setMax(db, "30")
        .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    cls.createProperty(db, "surname", PropertyType.STRING).setMin(db, "3")
        .setMax(db, "30");
    cls.createProperty(db, "location", PropertyType.LINK, addressCls);
    cls.createProperty(db, "hash", PropertyType.LONG);
    cls.createProperty(db, "invitedBy", PropertyType.LINK, cls);
    cls.createProperty(db, "value", PropertyType.INTEGER);

    cls.createProperty(db, "registeredOn", PropertyType.DATETIME)
        .setMin(db, "2010-01-01 00:00:00");
    cls.createProperty(db, "lastAccessOn", PropertyType.DATETIME)
        .setMin(db, "2010-01-01 00:00:00");
    cls.createProperty(db, "photo", PropertyType.TRANSIENT);
  }

  protected SchemaClass createInheritanceTestAbstractClass() {
    if (db.getClass("InheritanceTestAbstractClass") != null) {
      return db.getClass("InheritanceTestAbstractClass");
    }

    var cls = db.createClass("InheritanceTestAbstractClass");
    cls.createProperty(db, "cField", PropertyType.INTEGER);
    return cls;
  }

  protected SchemaClass createInheritanceTestBaseClass() {
    if (db.getClass("InheritanceTestBaseClass") != null) {
      return db.getClass("InheritanceTestBaseClass");
    }

    var abstractCls = createInheritanceTestAbstractClass();
    var cls = db.createClass("InheritanceTestBaseClass", abstractCls.getName());
    cls.createProperty(db, "aField", PropertyType.STRING);

    return cls;
  }

  protected void createInheritanceTestClass() {
    if (db.getClass("InheritanceTestClass") != null) {
      return;
    }

    var baseCls = createInheritanceTestBaseClass();
    var cls = db.createClass("InheritanceTestClass", baseCls.getName());
    cls.createProperty(db, "bField", PropertyType.STRING);
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

    if (db.getClusterIdByName("csv") == -1) {
      db.addCluster("csv");
    }

    if (db.getClusterIdByName("flat") == -1) {
      db.addCluster("flat");
    }

    if (db.getClusterIdByName("binary") == -1) {
      db.addCluster("binary");
    }
  }

  private void createWhizClass() {
    var account = createAccountClass();
    if (db.getMetadata().getSchema().existsClass("Whiz")) {
      return;
    }

    var whiz = db.getMetadata().getSchema()
        .createClass("Whiz", 1, (SchemaClass[]) null);
    whiz.createProperty(db, "id", PropertyType.INTEGER);
    whiz.createProperty(db, "account", PropertyType.LINK, account);
    whiz.createProperty(db, "date", PropertyType.DATE).setMin(db, "2010-01-01");
    whiz.createProperty(db, "text", PropertyType.STRING).setMandatory(db, true)
        .setMin(db, "1")
        .setMax(db, "140");
    whiz.createProperty(db, "replyTo", PropertyType.LINK, account);
  }

  private void createAnimalRaceClass() {
    if (db.getMetadata().getSchema().existsClass("AnimalRace")) {
      return;
    }

    var animalRace =
        db.getMetadata().getSchema().createClass("AnimalRace", 1, (SchemaClass[]) null);
    animalRace.createProperty(db, "name", PropertyType.STRING);
    var animal = db.getMetadata().getSchema()
        .createClass("Animal", 1, (SchemaClass[]) null);
    animal.createProperty(db, "races", PropertyType.LINKSET, animalRace);
    animal.createProperty(db, "name", PropertyType.STRING);
  }

  private void createStrictTestClass() {
    if (db.getMetadata().getSchema().existsClass("StrictTest")) {
      return;
    }

    var strictTest =
        db.getMetadata().getSchema().createClass("StrictTest", 1, (SchemaClass[]) null);
    strictTest.setStrictMode(db, true);
    strictTest.createProperty(db, "id", PropertyType.INTEGER).isMandatory();
    strictTest.createProperty(db, "name", PropertyType.STRING);
  }

  protected void createComplexTestClass() {
    if (db.getSchema().existsClass("JavaComplexTestClass")) {
      db.getSchema().dropClass("JavaComplexTestClass");
    }
    if (db.getSchema().existsClass("Child")) {
      db.getSchema().dropClass("Child");
    }

    var childCls = db.createClass("Child");
    childCls.createProperty(db, "name", PropertyType.STRING);

    var cls = db.createClass("JavaComplexTestClass");

    cls.createProperty(db, "embeddedDocument", PropertyType.EMBEDDED);
    cls.createProperty(db, "document", PropertyType.LINK);
    cls.createProperty(db, "byteArray", PropertyType.LINK);
    cls.createProperty(db, "name", PropertyType.STRING);
    cls.createProperty(db, "child", PropertyType.LINK, childCls);
    cls.createProperty(db, "stringMap", PropertyType.EMBEDDEDMAP);
    cls.createProperty(db, "stringListMap", PropertyType.EMBEDDEDMAP);
    cls.createProperty(db, "list", PropertyType.LINKLIST, childCls);
    cls.createProperty(db, "set", PropertyType.LINKSET, childCls);
    cls.createProperty(db, "duplicationTestSet", PropertyType.LINKSET, childCls);
    cls.createProperty(db, "children", PropertyType.LINKMAP, childCls);
    cls.createProperty(db, "stringSet", PropertyType.EMBEDDEDSET);
    cls.createProperty(db, "embeddedList", PropertyType.EMBEDDEDLIST);
    cls.createProperty(db, "embeddedSet", PropertyType.EMBEDDEDSET);
    cls.createProperty(db, "embeddedChildren", PropertyType.EMBEDDEDMAP);
    cls.createProperty(db, "mapObject", PropertyType.EMBEDDEDMAP);
  }

  protected void createSimpleTestClass() {
    if (db.getSchema().existsClass("JavaSimpleTestClass")) {
      db.getSchema().dropClass("JavaSimpleTestClass");
    }

    var cls = db.createClass("JavaSimpleTestClass");
    cls.createProperty(db, "text", PropertyType.STRING).setDefaultValue(db, "initTest");
    cls.createProperty(db, "numberSimple", PropertyType.INTEGER)
        .setDefaultValue(db, "0");
    cls.createProperty(db, "longSimple", PropertyType.LONG).setDefaultValue(db, "0");
    cls.createProperty(db, "doubleSimple", PropertyType.DOUBLE)
        .setDefaultValue(db, "0");
    cls.createProperty(db, "floatSimple", PropertyType.FLOAT).setDefaultValue(db, "0");
    cls.createProperty(db, "byteSimple", PropertyType.BYTE).setDefaultValue(db, "0");
    cls.createProperty(db, "shortSimple", PropertyType.SHORT).setDefaultValue(db, "0");
    cls.createProperty(db, "dateField", PropertyType.DATETIME);
  }

  protected void generateGraphData() {
    if (db.getSchema().existsClass("GraphVehicle")) {
      return;
    }

    var vehicleClass = db.createVertexClass("GraphVehicle");
    db.createClass("GraphCar", vehicleClass.getName());
    db.createClass("GraphMotocycle", "GraphVehicle");

    db.begin();
    var carNode = db.newVertex("GraphCar");
    carNode.setProperty("brand", "Hyundai");
    carNode.setProperty("model", "Coupe");
    carNode.setProperty("year", 2003);
    carNode.save();

    var motoNode = db.newVertex("GraphMotocycle");
    motoNode.setProperty("brand", "Yamaha");
    motoNode.setProperty("model", "X-City 250");
    motoNode.setProperty("year", 2009);
    motoNode.save();

    db.commit();

    db.begin();
    carNode = db.bindToSession(carNode);
    motoNode = db.bindToSession(motoNode);
    db.newRegularEdge(carNode, motoNode).save();

    var result =
        db.query("select from GraphVehicle").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);
    for (var v : result) {
      Assert.assertTrue(v.getEntity().get().getSchemaType().get().isSubClassOf(vehicleClass));
    }

    db.commit();
    result = db.query("select from GraphVehicle").stream().toList();
    Assert.assertEquals(result.size(), 2);

    Edge edge1 = null;
    Edge edge2 = null;

    for (var v : result) {
      Assert.assertTrue(v.getEntity().get().getSchemaType().get().isSubClassOf("GraphVehicle"));

      if (v.getEntity().get().getSchemaType().isPresent()
          && v.getEntity().get().getSchemaType().get().getName().equals("GraphCar")) {
        Assert.assertEquals(
            CollectionUtils.size(
                db.<Vertex>load(v.getIdentity().get()).getEdges(Direction.OUT)),
            1);
        edge1 =
            db
                .<Vertex>load(v.getIdentity().get())
                .getEdges(Direction.OUT)
                .iterator()
                .next();
      } else {
        Assert.assertEquals(
            CollectionUtils.size(
                db.<Vertex>load(v.getIdentity().get()).getEdges(Direction.IN)),
            1);
        edge2 =
            db.<Vertex>load(v.getIdentity().get()).getEdges(Direction.IN).iterator()
                .next();
      }
    }

    Assert.assertEquals(edge1, edge2);
  }

  public static int indexesUsed(ExecutionPlan executionPlan) {
    var indexes = new HashSet<String>();
    indexesUsed(indexes, executionPlan);

    return indexes.size();
  }

  private static void indexesUsed(Set<String> indexes, ExecutionPlan executionPlan) {
    var steps = executionPlan.getSteps();
    for (var step : steps) {
      indexesUsed(indexes, step);
    }
  }

  private static void indexesUsed(Set<String> indexes, ExecutionStep step) {
    if (step instanceof FetchFromIndexStep fetchFromIndexStep) {
      indexes.add(fetchFromIndexStep.getIndexName());
    }

    var subSteps = step.getSubSteps();
    for (var subStep : subSteps) {
      indexesUsed(indexes, subStep);
    }

    if (step instanceof ExecutionStepInternal internalStep) {
      var subPlans = internalStep.getSubExecutionPlans();
      for (var subPlan : subPlans) {
        indexesUsed(indexes, subPlan);
      }
    }
  }
}
