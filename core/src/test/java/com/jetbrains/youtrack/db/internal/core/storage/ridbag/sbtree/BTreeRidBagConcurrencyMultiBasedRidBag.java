package com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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

public class BTreeRidBagConcurrencyMultiBasedRidBag {

  public static final String URL = "plocal:target/testdb/BTreeRidBagConcurrencyMultiBasedRidBag";
  private final AtomicInteger positionCounter = new AtomicInteger();
  private final ConcurrentHashMap<RID, ConcurrentSkipListSet<RID>> ridTreePerDocument =
      new ConcurrentHashMap<RID, ConcurrentSkipListSet<RID>>();
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
    linkbagCacheSize = GlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.getValueAsInteger();
    evictionSize =
        GlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.getValueAsInteger();
    topThreshold =
        GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(30);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(20);

    GlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.setValue(1000);
    GlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.setValue(100);
  }

  @After
  public void afterMethod() {
    GlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.setValue(linkbagCacheSize);
    GlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.setValue(evictionSize);
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
    for (int i = 0; i < 100; i++) {
      EntityImpl document = ((EntityImpl) db.newEntity());
      RidBag ridBag = new RidBag(db);
      document.field("ridBag", ridBag);

      document.save();

      ridTreePerDocument.put(document.getIdentity(), new ConcurrentSkipListSet<RID>());
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
    for (RID rid : ridTreePerDocument.keySet()) {
      EntityImpl document = db.load(rid);
      document.setLazyLoad(false);

      final ConcurrentSkipListSet<RID> ridTree = ridTreePerDocument.get(rid);

      final RidBag ridBag = document.field("ridBag");

      for (Identifiable identifiable : ridBag) {
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
      DatabaseSessionInternal db = new DatabaseDocumentTx(URL);
      db.open("admin", "admin");

      try {
        EntityImpl document = ((EntityImpl) db.newEntity());
        RidBag ridBag = new RidBag(db);
        document.field("ridBag", ridBag);

        document.save();
        ridTreePerDocument.put(document.getIdentity(), new ConcurrentSkipListSet<RID>());

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

      DatabaseSessionInternal db = new DatabaseDocumentTx(URL);
      db.open("admin", "admin");

      final int defaultClusterId = db.getDefaultClusterId();
      latch.await();
      try {
        while (cont) {
          List<RID> ridsToAdd = new ArrayList<RID>();
          for (int i = 0; i < 10; i++) {
            ridsToAdd.add(new RecordId(0, positionCounter.incrementAndGet()));
          }

          final long position = random.nextInt(lastClusterPosition.get().intValue());
          final RID orid = new RecordId(defaultClusterId, position);

          while (true) {
            EntityImpl document = db.load(orid);
            document.setLazyLoad(false);

            RidBag ridBag = document.field("ridBag");
            for (RID rid : ridsToAdd) {
              ridBag.add(rid);
            }

            try {
              document.save();
            } catch (ConcurrentModificationException e) {
              retries++;
              continue;
            }

            break;
          }

          final ConcurrentSkipListSet<RID> ridTree = ridTreePerDocument.get(orid);
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

      DatabaseSessionInternal db = new DatabaseDocumentTx(URL);
      db.open("admin", "admin");

      final int defaultClusterId = db.getDefaultClusterId();
      latch.await();
      try {
        while (cont) {
          final long position = random.nextInt(lastClusterPosition.get().intValue());
          final RID orid = new RecordId(defaultClusterId, position);

          while (true) {
            EntityImpl document = db.load(orid);
            document.setLazyLoad(false);
            RidBag ridBag = document.field("ridBag");
            Iterator<RID> iterator = ridBag.iterator();

            List<RID> ridsToDelete = new ArrayList<RID>();
            int counter = 0;
            while (iterator.hasNext()) {
              Identifiable identifiable = iterator.next();
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
            } catch (ConcurrentModificationException e) {
              retries++;
              continue;
            }

            final ConcurrentSkipListSet<RID> ridTree = ridTreePerDocument.get(orid);
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
