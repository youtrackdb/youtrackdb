package com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BTreeRidBagConcurrencySingleBasedRidBag {

  public static final String URL = "plocal:target/testdb/BTreeRidBagConcurrencySingleBasedRidBag";
  private final AtomicInteger positionCounter = new AtomicInteger();
  private final ConcurrentSkipListSet<RID> ridTree = new ConcurrentSkipListSet<RID>();
  private final CountDownLatch latch = new CountDownLatch(1);
  private RID docContainerRid;
  private final ExecutorService threadExecutor = Executors.newCachedThreadPool();
  private volatile boolean cont = true;

  private int topThreshold;
  private int bottomThreshold;

  @Before
  public void beforeMethod() {
    topThreshold =
        GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(30);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(20);
  }

  @After
  public void afterMethod() {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testConcurrency() throws Exception {
    DatabaseSessionInternal db = new DatabaseDocumentTx(URL);
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    EntityImpl document = ((EntityImpl) db.newEntity());
    RidBag ridBag = new RidBag(db);

    document.field("ridBag", ridBag);
    for (int i = 0; i < 100; i++) {
      final RID ridToAdd = new RecordId(0, positionCounter.incrementAndGet());
      ridBag.add(ridToAdd);
      ridTree.add(ridToAdd);
    }
    document.save();

    docContainerRid = document.getIdentity();

    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    for (int i = 0; i < 5; i++) {
      futures.add(threadExecutor.submit(new RidAdder(i)));
    }

    for (int i = 0; i < 5; i++) {
      futures.add(threadExecutor.submit(new RidDeleter(i)));
    }

    latch.countDown();

    Thread.sleep(30 * 60000);
    cont = false;

    for (Future<Void> future : futures) {
      future.get();
    }

    document = db.load(document.getIdentity());
    document.setLazyLoad(false);

    ridBag = document.field("ridBag");

    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(ridTree.remove(identifiable.getIdentity()));
    }

    Assert.assertTrue(ridTree.isEmpty());

    System.out.println("Result size is " + ridBag.size());
    db.close();
  }

  public class RidAdder implements Callable<Void> {

    private final int id;

    public RidAdder(int id) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      int addedRecords = 0;

      DatabaseSessionInternal db = new DatabaseDocumentTx(URL);
      db.open("admin", "admin");

      try {
        while (cont) {
          List<RID> ridsToAdd = new ArrayList<RID>();
          for (int i = 0; i < 10; i++) {
            ridsToAdd.add(new RecordId(0, positionCounter.incrementAndGet()));
          }

          while (true) {
            EntityImpl document = db.load(docContainerRid);
            document.setLazyLoad(false);

            RidBag ridBag = document.field("ridBag");
            for (RID rid : ridsToAdd) {
              ridBag.add(rid);
            }

            try {
              document.save();
            } catch (ConcurrentModificationException e) {
              continue;
            }

            break;
          }

          ridTree.addAll(ridsToAdd);
          addedRecords += ridsToAdd.size();
        }
      } finally {
        db.close();
      }

      System.out.println(
          RidAdder.class.getSimpleName() + ":" + id + "-" + addedRecords + " were added.");
      return null;
    }
  }

  public class RidDeleter implements Callable<Void> {

    private final int id;

    public RidDeleter(int id) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      latch.await();

      int deletedRecords = 0;

      Random rnd = new Random();
      DatabaseSessionInternal db = new DatabaseDocumentTx(URL);
      db.open("admin", "admin");

      try {
        while (cont) {
          while (true) {
            EntityImpl document = db.load(docContainerRid);
            document.setLazyLoad(false);
            RidBag ridBag = document.field("ridBag");
            Iterator<Identifiable> iterator = ridBag.iterator();

            List<RID> ridsToDelete = new ArrayList<RID>();
            int counter = 0;
            while (iterator.hasNext()) {
              Identifiable identifiable = iterator.next();
              if (rnd.nextBoolean()) {
                iterator.remove();
                counter++;
                ridsToDelete.add(identifiable.getIdentity());
              }

              if (counter >= 5) {
                break;
              }
            }

            try {
              document.save();
            } catch (ConcurrentModificationException e) {
              continue;
            }

            ridTree.removeAll(ridsToDelete);

            deletedRecords += ridsToDelete.size();
            break;
          }
        }
      } finally {
        db.close();
      }

      System.out.println(
          RidDeleter.class.getSimpleName() + ":" + id + "-" + deletedRecords + " were deleted.");
      return null;
    }
  }
}
