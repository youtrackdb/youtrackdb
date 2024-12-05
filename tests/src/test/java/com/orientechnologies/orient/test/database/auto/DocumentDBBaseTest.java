package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionStepInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.FetchFromIndexStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
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
public abstract class DocumentDBBaseTest extends BaseTest<YTDatabaseSessionInternal> {

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
  protected YTDatabaseSessionInternal createSessionInstance(
      YouTrackDB youTrackDB, String dbName, String user, String password) {
    var session = youTrackDB.open(dbName, user, password);
    return (YTDatabaseSessionInternal) session;
  }

  protected List<EntityImpl> executeQuery(String sql, YTDatabaseSessionInternal db,
      Object... args) {
    return db.query(sql, args).stream()
        .map(YTResult::toEntity)
        .map(element -> (EntityImpl) element)
        .toList();
  }

  protected List<EntityImpl> executeQuery(String sql, YTDatabaseSessionInternal db, Map args) {
    return db.query(sql, args).stream()
        .map(YTResult::toEntity)
        .map(element -> (EntityImpl) element)
        .toList();
  }

  protected List<EntityImpl> executeQuery(String sql, YTDatabaseSessionInternal db) {
    return db.query(sql).stream()
        .map(YTResult::toEntity)
        .map(element -> (EntityImpl) element)
        .toList();
  }

  protected List<EntityImpl> executeQuery(String sql, Object... args) {
    return database.query(sql, args).stream()
        .map(YTResult::toEntity)
        .map(element -> (EntityImpl) element)
        .toList();
  }

  protected List<EntityImpl> executeQuery(String sql, Map<?, ?> args) {
    return database.query(sql, args).stream()
        .map(YTResult::toEntity)
        .map(element -> (EntityImpl) element)
        .toList();
  }

  protected List<EntityImpl> executeQuery(String sql) {
    return database.query(sql).stream()
        .map(YTResult::toEntity)
        .map(element -> (EntityImpl) element)
        .toList();
  }

  protected void addBarackObamaAndFollowers() {
    createProfileClass();

    database.begin();
    if (database.query("select from Profile where name = 'Barack' and surname = 'Obama'").stream()
        .findAny()
        .isEmpty()) {

      var bObama = database.newEntity("Profile");
      bObama.setProperty("nick", "ThePresident");
      bObama.setProperty("name", "Barack");
      bObama.setProperty("surname", "Obama");
      bObama.setProperty("followings", Collections.emptySet());

      var follower1 = database.newEntity("Profile");
      follower1.setProperty("nick", "PresidentSon1");
      follower1.setProperty("name", "Malia Ann");
      follower1.setProperty("surname", "Obama");
      follower1.setProperty("followings", Collections.singleton(bObama));
      follower1.setProperty("followers", Collections.emptySet());

      var follower2 = database.newEntity("Profile");
      follower2.setProperty("nick", "PresidentSon2");
      follower2.setProperty("name", "Natasha");
      follower2.setProperty("surname", "Obama");
      follower2.setProperty("followings", Collections.singleton(bObama));
      follower2.setProperty("followers", Collections.emptySet());

      var followers = new HashSet<Entity>();
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
        var element = database.newEntity("Account");
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
              var profile = database.newEntity("Profile");
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

  private Entity addRome() {
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

  private Entity addItaly() {
    return database.computeInTx(
        () -> {
          var italy = database.newEntity("Country");
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

      var addresses = new ArrayList<Entity>();
      addresses.add(database.bindToSession(address));
      company.setProperty("addresses", addresses);
      database.save(company);
      database.commit();
    }
  }

  protected Entity createRedmondAddress() {
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

  protected YTClass createCountryClass() {
    if (database.getClass("Country") != null) {
      return database.getClass("Country");
    }

    var cls = database.createClass("Country");
    cls.createProperty(database, "name", YTType.STRING);
    return cls;
  }

  protected YTClass createCityClass() {
    var countryCls = createCountryClass();

    if (database.getClass("City") != null) {
      return database.getClass("City");
    }

    var cls = database.createClass("City");
    cls.createProperty(database, "name", YTType.STRING);
    cls.createProperty(database, "country", YTType.LINK, countryCls);

    return cls;
  }

  protected YTClass createAddressClass() {
    if (database.getClass("Address") != null) {
      return database.getClass("Address");
    }

    var cityCls = createCityClass();
    var cls = database.createClass("Address");
    cls.createProperty(database, "type", YTType.STRING);
    cls.createProperty(database, "street", YTType.STRING);
    cls.createProperty(database, "city", YTType.LINK, cityCls);

    return cls;
  }

  protected YTClass createAccountClass() {
    if (database.getClass("Account") != null) {
      return database.getClass("Account");
    }

    var addressCls = createAddressClass();
    var cls = database.createClass("Account");
    cls.createProperty(database, "id", YTType.INTEGER);
    cls.createProperty(database, "name", YTType.STRING);
    cls.createProperty(database, "surname", YTType.STRING);
    cls.createProperty(database, "birthDate", YTType.DATE);
    cls.createProperty(database, "salary", YTType.FLOAT);
    cls.createProperty(database, "addresses", YTType.LINKLIST, addressCls);
    cls.createProperty(database, "thumbnail", YTType.BINARY);
    cls.createProperty(database, "photo", YTType.BINARY);

    return cls;
  }

  protected void createCompanyClass() {
    if (database.getClass("Company") != null) {
      return;
    }

    createAccountClass();
    var cls = database.createClassIfNotExist("Company", "Account");
    cls.createProperty(database, "employees", YTType.INTEGER);
  }

  protected void createProfileClass() {
    if (database.getClass("Profile") != null) {
      return;
    }

    var addressCls = createAddressClass();
    var cls = database.createClass("Profile");
    cls.createProperty(database, "nick", YTType.STRING)
        .setMin(database, "3")
        .setMax(database, "30")
        .createIndex(database, YTClass.INDEX_TYPE.UNIQUE,
            new EntityImpl().field("ignoreNullValues", true));
    cls.createProperty(database, "followings", YTType.LINKSET, cls);
    cls.createProperty(database, "followers", YTType.LINKSET, cls);
    cls.createProperty(database, "name", YTType.STRING)
        .setMin(database, "3")
        .setMax(database, "30")
        .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);

    cls.createProperty(database, "surname", YTType.STRING).setMin(database, "3")
        .setMax(database, "30");
    cls.createProperty(database, "location", YTType.LINK, addressCls);
    cls.createProperty(database, "hash", YTType.LONG);
    cls.createProperty(database, "invitedBy", YTType.LINK, cls);
    cls.createProperty(database, "value", YTType.INTEGER);

    cls.createProperty(database, "registeredOn", YTType.DATETIME)
        .setMin(database, "2010-01-01 00:00:00");
    cls.createProperty(database, "lastAccessOn", YTType.DATETIME)
        .setMin(database, "2010-01-01 00:00:00");
    cls.createProperty(database, "photo", YTType.TRANSIENT);
  }

  protected YTClass createInheritanceTestAbstractClass() {
    if (database.getClass("InheritanceTestAbstractClass") != null) {
      return database.getClass("InheritanceTestAbstractClass");
    }

    var cls = database.createClass("InheritanceTestAbstractClass");
    cls.createProperty(database, "cField", YTType.INTEGER);
    return cls;
  }

  protected YTClass createInheritanceTestBaseClass() {
    if (database.getClass("InheritanceTestBaseClass") != null) {
      return database.getClass("InheritanceTestBaseClass");
    }

    var abstractCls = createInheritanceTestAbstractClass();
    var cls = database.createClass("InheritanceTestBaseClass", abstractCls.getName());
    cls.createProperty(database, "aField", YTType.STRING);

    return cls;
  }

  protected void createInheritanceTestClass() {
    if (database.getClass("InheritanceTestClass") != null) {
      return;
    }

    var baseCls = createInheritanceTestBaseClass();
    var cls = database.createClass("InheritanceTestClass", baseCls.getName());
    cls.createProperty(database, "bField", YTType.STRING);
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
    YTClass account = createAccountClass();
    if (database.getMetadata().getSchema().existsClass("Whiz")) {
      return;
    }

    YTClass whiz = database.getMetadata().getSchema().createClass("Whiz", 1, (YTClass[]) null);
    whiz.createProperty(database, "id", YTType.INTEGER);
    whiz.createProperty(database, "account", YTType.LINK, account);
    whiz.createProperty(database, "date", YTType.DATE).setMin(database, "2010-01-01");
    whiz.createProperty(database, "text", YTType.STRING).setMandatory(database, true)
        .setMin(database, "1")
        .setMax(database, "140");
    whiz.createProperty(database, "replyTo", YTType.LINK, account);
  }

  private void createAnimalRaceClass() {
    if (database.getMetadata().getSchema().existsClass("AnimalRace")) {
      return;
    }

    YTClass animalRace =
        database.getMetadata().getSchema().createClass("AnimalRace", 1, (YTClass[]) null);
    animalRace.createProperty(database, "name", YTType.STRING);
    YTClass animal = database.getMetadata().getSchema().createClass("Animal", 1, (YTClass[]) null);
    animal.createProperty(database, "races", YTType.LINKSET, animalRace);
    animal.createProperty(database, "name", YTType.STRING);
  }

  private void createStrictTestClass() {
    if (database.getMetadata().getSchema().existsClass("StrictTest")) {
      return;
    }

    YTClass strictTest =
        database.getMetadata().getSchema().createClass("StrictTest", 1, (YTClass[]) null);
    strictTest.setStrictMode(database, true);
    strictTest.createProperty(database, "id", YTType.INTEGER).isMandatory();
    strictTest.createProperty(database, "name", YTType.STRING);
  }

  protected void createComplexTestClass() {
    if (database.getSchema().existsClass("JavaComplexTestClass")) {
      database.getSchema().dropClass("JavaComplexTestClass");
    }
    if (database.getSchema().existsClass("Child")) {
      database.getSchema().dropClass("Child");
    }

    var childCls = database.createClass("Child");
    childCls.createProperty(database, "name", YTType.STRING);

    var cls = database.createClass("JavaComplexTestClass");

    cls.createProperty(database, "embeddedDocument", YTType.EMBEDDED);
    cls.createProperty(database, "document", YTType.LINK);
    cls.createProperty(database, "byteArray", YTType.LINK);
    cls.createProperty(database, "name", YTType.STRING);
    cls.createProperty(database, "child", YTType.LINK, childCls);
    cls.createProperty(database, "stringMap", YTType.EMBEDDEDMAP);
    cls.createProperty(database, "stringListMap", YTType.EMBEDDEDMAP);
    cls.createProperty(database, "list", YTType.LINKLIST, childCls);
    cls.createProperty(database, "set", YTType.LINKSET, childCls);
    cls.createProperty(database, "duplicationTestSet", YTType.LINKSET, childCls);
    cls.createProperty(database, "children", YTType.LINKMAP, childCls);
    cls.createProperty(database, "stringSet", YTType.EMBEDDEDSET);
    cls.createProperty(database, "embeddedList", YTType.EMBEDDEDLIST);
    cls.createProperty(database, "embeddedSet", YTType.EMBEDDEDSET);
    cls.createProperty(database, "embeddedChildren", YTType.EMBEDDEDMAP);
    cls.createProperty(database, "mapObject", YTType.EMBEDDEDMAP);
  }

  protected void createSimpleTestClass() {
    if (database.getSchema().existsClass("JavaSimpleTestClass")) {
      database.getSchema().dropClass("JavaSimpleTestClass");
    }

    var cls = database.createClass("JavaSimpleTestClass");
    cls.createProperty(database, "text", YTType.STRING).setDefaultValue(database, "initTest");
    cls.createProperty(database, "numberSimple", YTType.INTEGER).setDefaultValue(database, "0");
    cls.createProperty(database, "longSimple", YTType.LONG).setDefaultValue(database, "0");
    cls.createProperty(database, "doubleSimple", YTType.DOUBLE).setDefaultValue(database, "0");
    cls.createProperty(database, "floatSimple", YTType.FLOAT).setDefaultValue(database, "0");
    cls.createProperty(database, "byteSimple", YTType.BYTE).setDefaultValue(database, "0");
    cls.createProperty(database, "shortSimple", YTType.SHORT).setDefaultValue(database, "0");
    cls.createProperty(database, "dateField", YTType.DATETIME);
  }

  protected void generateGraphData() {
    if (database.getSchema().existsClass("GraphVehicle")) {
      return;
    }

    YTClass vehicleClass = database.createVertexClass("GraphVehicle");
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

    List<YTResult> result =
        database.query("select from GraphVehicle").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);
    for (YTResult v : result) {
      Assert.assertTrue(v.getEntity().get().getSchemaType().get().isSubClassOf(vehicleClass));
    }

    database.commit();
    result = database.query("select from GraphVehicle").stream().toList();
    Assert.assertEquals(result.size(), 2);

    Edge edge1 = null;
    Edge edge2 = null;

    for (YTResult v : result) {
      Assert.assertTrue(v.getEntity().get().getSchemaType().get().isSubClassOf("GraphVehicle"));

      if (v.getEntity().get().getSchemaType().isPresent()
          && v.getEntity().get().getSchemaType().get().getName().equals("GraphCar")) {
        Assert.assertEquals(
            CollectionUtils.size(
                database.<Vertex>load(v.getIdentity().get()).getEdges(ODirection.OUT)),
            1);
        edge1 =
            database
                .<Vertex>load(v.getIdentity().get())
                .getEdges(ODirection.OUT)
                .iterator()
                .next();
      } else {
        Assert.assertEquals(
            CollectionUtils.size(
                database.<Vertex>load(v.getIdentity().get()).getEdges(ODirection.IN)),
            1);
        edge2 =
            database.<Vertex>load(v.getIdentity().get()).getEdges(ODirection.IN).iterator()
                .next();
      }
    }

    Assert.assertEquals(edge1, edge2);
  }

  public static int indexesUsed(OExecutionPlan executionPlan) {
    var indexes = new HashSet<String>();
    indexesUsed(indexes, executionPlan);

    return indexes.size();
  }

  private static void indexesUsed(Set<String> indexes, OExecutionPlan executionPlan) {
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
