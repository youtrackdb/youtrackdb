package com.orientechnologies.core.storage.ridbag.sbtree;

import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.exception.YTConcurrentModificationException;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OSBTreeRidBagConcurrencyMultiRidBag {

  public static final String URL = "plocal:target/testdb/OSBTreeRidBagConcurrencyMultiRidBag";
  private final AtomicInteger positionCounter = new AtomicInteger();
  private final ConcurrentHashMap<YTRID, ConcurrentSkipListSet<YTRID>> ridTreePerDocument =
      new ConcurrentHashMap<YTRID, ConcurrentSkipListSet<YTRID>>();
  private final AtomicReference<Long> lastClusterPosition = new AtomicReference<Long>();

  private final CountDownLatch latch = new CountDownLatch(1);

  private final ExecutorService threadExecutor = Executors.newCachedThreadPool();
  private final ScheduledExecutorService addDocExecutor = Executors.newScheduledThreadPool(5);

  private volatile boolean cont = true;

  private int linkbagCacheSize;
  private int evictionSize;

  private int topThreshold;
  private int bottomThreshold;

  @Before
  public void beforeMethod() {
    linkbagCacheSize = YTGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.getValueAsInteger();
    evictionSize =
        YTGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.getValueAsInteger();
    topThreshold =
        YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(30);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(20);

    YTGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.setValue(1000);
    YTGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.setValue(100);
  }

  @After
  public void afterMethod() {
    YTGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.setValue(linkbagCacheSize);
    YTGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.setValue(evictionSize);
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
    for (int i = 0; i < 100; i++) {
      YTEntityImpl document = new YTEntityImpl();
      RidBag ridBag = new RidBag(db);
      document.field("ridBag", ridBag);

      document.save();

      ridTreePerDocument.put(document.getIdentity(), new ConcurrentSkipListSet<YTRID>());
      lastClusterPosition.set(document.getIdentity().getClusterPosition());
    }

    final List<Future<?>> futures = new ArrayList<Future<?>>();

    Random random = new Random();
    for (int i = 0; i < 5; i++) {
      addDocExecutor.scheduleAtFixedRate(
          new DocumentAdder(), random.nextInt(250), 250, TimeUnit.MILLISECONDS);
    }

    for (int i = 0; i < 5; i++) {
      futures.add(threadExecutor.submit(new RidAdder(i)));
    }

    for (int i = 0; i < 5; i++) {
      futures.add(threadExecutor.submit(new RidDeleter(i)));
    }

    latch.countDown();

    Thread.sleep(30 * 60000);

    addDocExecutor.shutdown();
    addDocExecutor.awaitTermination(30, TimeUnit.SECONDS);

    Thread.sleep(30 * 60000);

    cont = false;

    for (Future<?> future : futures) {
      future.get();
    }

    long amountOfRids = 0;
    for (YTRID rid : ridTreePerDocument.keySet()) {
      YTEntityImpl document = db.load(rid);
      document.setLazyLoad(false);

      final ConcurrentSkipListSet<YTRID> ridTree = ridTreePerDocument.get(rid);

      final RidBag ridBag = document.field("ridBag");

      for (YTIdentifiable identifiable : ridBag) {
        Assert.assertTrue(ridTree.remove(identifiable.getIdentity()));
      }

      Assert.assertTrue(ridTree.isEmpty());
      amountOfRids += ridBag.size();
    }

    System.out.println(
        "Total  records added :  " + db.countClusterElements(db.getDefaultClusterId()));
    System.out.println("Total rids added : " + amountOfRids);

    db.drop();
  }

  public final class DocumentAdder implements Runnable {

    @Override
    public void run() {
      YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(URL);
      db.open("admin", "admin");

      try {
        YTEntityImpl document = new YTEntityImpl();
        RidBag ridBag = new RidBag(db);
        document.field("ridBag", ridBag);

        document.save();
        ridTreePerDocument.put(document.getIdentity(), new ConcurrentSkipListSet<YTRID>());

        while (true) {
          final long position = lastClusterPosition.get();
          if (position < document.getIdentity().getClusterPosition()) {
            if (lastClusterPosition.compareAndSet(
                position, document.getIdentity().getClusterPosition())) {
              break;
            }
          } else {
            break;
          }
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      } finally {
        db.close();
      }
    }
  }

  public class RidAdder implements Callable<Void> {

    private final int id;

    public RidAdder(int id) {
      this.id = id;
    }

    @Override
    public Void call() throws Exception {
      final Random random = new Random();
      long addedRecords = 0;
      int retries = 0;

      YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(URL);
      db.open("admin", "admin");

      final int defaultClusterId = db.getDefaultClusterId();
      latch.await();
      try {
        while (cont) {
          List<YTRID> ridsToAdd = new ArrayList<YTRID>();
          for (int i = 0; i < 10; i++) {
            ridsToAdd.add(new YTRecordId(0, positionCounter.incrementAndGet()));
          }

          final long position = random.nextInt(lastClusterPosition.get().intValue());
          final YTRID orid = new YTRecordId(defaultClusterId, position);

          while (true) {
            YTEntityImpl document = db.load(orid);
            document.setLazyLoad(false);

            RidBag ridBag = document.field("ridBag");
            for (YTRID rid : ridsToAdd) {
              ridBag.add(rid);
            }

            try {
              document.save();
            } catch (YTConcurrentModificationException e) {
              retries++;
              continue;
            }

            break;
          }

          final ConcurrentSkipListSet<YTRID> ridTree = ridTreePerDocument.get(orid);
          ridTree.addAll(ridsToAdd);
          addedRecords += ridsToAdd.size();
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      } finally {
        db.close();
      }

      System.out.println(
          RidAdder.class.getSimpleName()
              + ":"
              + id
              + "-"
              + addedRecords
              + " were added. retries : "
              + retries);
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
      final Random random = new Random();
      long deletedRecords = 0;
      int retries = 0;

      YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(URL);
      db.open("admin", "admin");

      final int defaultClusterId = db.getDefaultClusterId();
      latch.await();
      try {
        while (cont) {
          final long position = random.nextInt(lastClusterPosition.get().intValue());
          final YTRID orid = new YTRecordId(defaultClusterId, position);

          while (true) {
            YTEntityImpl document = db.load(orid);
            document.setLazyLoad(false);
            RidBag ridBag = document.field("ridBag");
            Iterator<YTIdentifiable> iterator = ridBag.iterator();

            List<YTRID> ridsToDelete = new ArrayList<YTRID>();
            int counter = 0;
            while (iterator.hasNext()) {
              YTIdentifiable identifiable = iterator.next();
              if (random.nextBoolean()) {
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
              retries++;
              continue;
            }

            final ConcurrentSkipListSet<YTRID> ridTree = ridTreePerDocument.get(orid);
            ridTree.removeAll(ridsToDelete);

            deletedRecords += ridsToDelete.size();
            break;
          }
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      } finally {
        db.close();
      }

      System.out.println(
          RidDeleter.class.getSimpleName()
              + ":"
              + id
              + "-"
              + deletedRecords
              + " were deleted. retries : "
              + retries);
      return null;
    }
  }
}
