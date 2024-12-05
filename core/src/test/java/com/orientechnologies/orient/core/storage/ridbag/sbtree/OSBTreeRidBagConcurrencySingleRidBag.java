package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.RidBag;
import com.orientechnologies.orient.core.exception.YTConcurrentModificationException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
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

public class OSBTreeRidBagConcurrencySingleRidBag {

  public static final String URL = "plocal:target/testdb/OSBTreeRidBagConcurrencySingleRidBag";
  private final AtomicInteger positionCounter = new AtomicInteger();
  private final ConcurrentSkipListSet<YTRID> ridTree = new ConcurrentSkipListSet<YTRID>();
  private final CountDownLatch latch = new CountDownLatch(1);
  private YTRID docContainerRid;
  private final ExecutorService threadExecutor = Executors.newCachedThreadPool();
  private volatile boolean cont = true;

  private int topThreshold;
  private int bottomThreshold;

  @Before
  public void beforeMethod() {
    topThreshold =
        YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(30);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(20);
  }

  @After
  public void afterMethod() {
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testConcurrency() throws Exception {
    YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(URL);
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    YTEntityImpl document = new YTEntityImpl();
    RidBag ridBag = new RidBag(db);

    document.field("ridBag", ridBag);
    for (int i = 0; i < 100; i++) {
      final YTRID ridToAdd = new YTRecordId(0, positionCounter.incrementAndGet());
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

    for (YTIdentifiable identifiable : ridBag) {
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

      YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(URL);
      db.open("admin", "admin");

      try {
        while (cont) {
          List<YTRID> ridsToAdd = new ArrayList<YTRID>();
          for (int i = 0; i < 10; i++) {
            ridsToAdd.add(new YTRecordId(0, positionCounter.incrementAndGet()));
          }

          while (true) {
            YTEntityImpl document = db.load(docContainerRid);
            document.setLazyLoad(false);

            RidBag ridBag = document.field("ridBag");
            for (YTRID rid : ridsToAdd) {
              ridBag.add(rid);
            }

            try {
              document.save();
            } catch (YTConcurrentModificationException e) {
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
      YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(URL);
      db.open("admin", "admin");

      try {
        while (cont) {
          while (true) {
            YTEntityImpl document = db.load(docContainerRid);
            document.setLazyLoad(false);
            RidBag ridBag = document.field("ridBag");
            Iterator<YTIdentifiable> iterator = ridBag.iterator();

            List<YTRID> ridsToDelete = new ArrayList<YTRID>();
            int counter = 0;
            while (iterator.hasNext()) {
              YTIdentifiable identifiable = iterator.next();
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
            } catch (YTConcurrentModificationException e) {
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
