package com.jetbrains.youtrack.db.internal.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import org.junit.Test;

/**
 *
 */
public class DBSequenceRemoteTest extends AbstractRemoteTest {

  DatabaseSessionInternal db;

  @Override
  public void setup() throws Exception {
    super.setup();
    YouTrackDB factory =
        new YouTrackDBImpl("remote:localhost", "root", "root",
            YouTrackDBConfig.defaultConfig());
    db = (DatabaseSessionInternal) factory.open(name.getMethodName(), "admin", "admin");
  }

  @Override
  public void teardown() {
    db.close();
    super.teardown();
  }

  @Test
  public void shouldSequenceWithDefaultValueNoTx() {

    db.command("CREATE CLASS Person EXTENDS V");
    db.command("CREATE SEQUENCE personIdSequence TYPE ORDERED;");
    db.command(
        "CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default"
            + " \"sequence('personIdSequence').next()\");");
    db.command("CREATE INDEX Person.id ON Person (id) UNIQUE");

    db.begin();
    for (var i = 0; i < 10; i++) {
      var person = db.newVertex("Person");
      person.setProperty("name", "Foo" + i);
      person.setProperty("id", 1000 + i);
    }
    db.commit();

    assertThat(db.countClass("Person")).isEqualTo(10);
  }

  @Test
  public void shouldSequenceWithDefaultValueTx() {

    db.command("CREATE CLASS Person EXTENDS V");
    db.command("CREATE SEQUENCE personIdSequence TYPE ORDERED;");
    db.command(
        "CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default"
            + " \"sequence('personIdSequence').next()\");");
    db.command("CREATE INDEX Person.id ON Person (id) UNIQUE");

    db.begin();

    for (var i = 0; i < 10; i++) {
      var person = db.newVertex("Person");
      person.setProperty("name", "Foo" + i);
    }

    db.commit();

    assertThat(db.countClass("Person")).isEqualTo(10);
  }

  @Test
  public void testCreateCachedSequenceInTx() {
    db.begin();
    db.command("CREATE SEQUENCE CircuitSequence TYPE CACHED START 1 INCREMENT 1 CACHE 10;");
    db.commit();

    db.command("select sequence('CircuitSequence').next() as seq");
  }

  @Test
  public void testCreateOrderedSequenceInTx() {
    db.begin();
    db.command("CREATE SEQUENCE CircuitSequence TYPE ORDERED;");
    db.commit();

    db.command("select sequence('CircuitSequence').next() as seq");
  }
}
