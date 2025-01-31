package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLNonBlockingQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class NonBlockingQueryTest extends BaseDBTest {

  @Parameters(value = "remote")
  public NonBlockingQueryTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    db.command("create class Foo").close();
  }

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {
    super.beforeMethod();
    db.command("delete from Foo").close();
  }

  @Test
  public void testClone() {

    var db = this.db;

    db.begin();
    db.command("insert into Foo (a) values ('bar')").close();
    db.commit();
    var newDb = db.copy();

    List<EntityImpl> result = newDb.query(new SQLSynchQuery<EntityImpl>("Select from Foo"));
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).field("a"), "bar");
    newDb.close();
  }

  @Test
  public void testNonBlockingQuery() {
    var db = this.db;
    final var counter = new AtomicInteger(0); // db.begin();
    for (var i = 0; i < 1000; i++) {
      db.command("insert into Foo (a) values ('bar')").close();
    }
    Future future =
        db.query(
            new SQLNonBlockingQuery<Object>(
                "select from Foo",
                new CommandResultListener() {
                  @Override
                  public boolean result(DatabaseSessionInternal db, Object iRecord) {
                    counter.incrementAndGet();
                    return true;
                  }

                  @Override
                  public void end() {
                  }

                  @Override
                  public Object getResult() {
                    return null;
                  }
                }));
    Assert.assertNotEquals(counter.get(), 1000);
    try {
      future.get();
      Assert.assertEquals(counter.get(), 1000);
    } catch (InterruptedException e) {
      Assert.fail();
      e.printStackTrace();
    } catch (ExecutionException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }

  @Test
  public void testNonBlockingQueryWithCompositeIndex() {
    db.command("create property Foo.x integer").close();
    db.command("create property Foo.y integer").close();
    db.command("create index Foo_xy_index on Foo (x, y) notunique").close();

    var db = this.db;
    final var counter = new AtomicInteger(0); // db.begin();
    for (var i = 0; i < 1000; i++) {
      db.command("insert into Foo (a, x, y) values ('bar', ?, ?)", i, 1000 - i).close();
    }
    Future future =
        db.query(
            new SQLNonBlockingQuery<Object>(
                "select from Foo where x=500 and y=500",
                new CommandResultListener() {
                  @Override
                  public boolean result(DatabaseSessionInternal db, Object iRecord) {
                    counter.incrementAndGet();
                    return true;
                  }

                  @Override
                  public void end() {
                  }

                  @Override
                  public Object getResult() {
                    return null;
                  }
                }));
    Assert.assertNotEquals(counter.get(), 1);
    try {
      future.get();
      Assert.assertEquals(counter.get(), 1);
    } catch (InterruptedException e) {
      Assert.fail();
      e.printStackTrace();
    } catch (ExecutionException e) {
      Assert.fail();
      e.printStackTrace();
    }
  }
}
