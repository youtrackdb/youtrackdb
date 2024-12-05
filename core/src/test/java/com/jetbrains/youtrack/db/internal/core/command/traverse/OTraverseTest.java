package com.jetbrains.youtrack.db.internal.core.command.traverse;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OTraverseTest extends DBTestBase {

  private EntityImpl rootDocument;
  private OTraverse traverse;

  public void beforeTest() throws Exception {
    super.beforeTest();

    rootDocument = new EntityImpl();
    traverse = new OTraverse(db);
    traverse.target(rootDocument).fields("*");
  }

  @Test
  public void testDepthTraverse() {

    final EntityImpl aa = new EntityImpl();
    final EntityImpl ab = new EntityImpl();
    final EntityImpl ba = new EntityImpl();
    final EntityImpl bb = new EntityImpl();
    final EntityImpl a = new EntityImpl();
    a.setProperty("aa", aa, YTType.LINK);
    a.setProperty("ab", ab, YTType.LINK);
    final EntityImpl b = new EntityImpl();
    b.setProperty("ba", ba, YTType.LINK);
    b.setProperty("bb", bb, YTType.LINK);

    rootDocument.setProperty("a", a, YTType.LINK);
    rootDocument.setProperty("b", b, YTType.LINK);

    final EntityImpl c1 = new EntityImpl();
    final EntityImpl c1a = new EntityImpl();
    c1.setProperty("c1a", c1a, YTType.LINK);
    final EntityImpl c1b = new EntityImpl();
    c1.setProperty("c1b", c1b, YTType.LINK);
    final EntityImpl c2 = new EntityImpl();
    final EntityImpl c2a = new EntityImpl();
    c2.setProperty("c2a", c2a, YTType.LINK);
    final EntityImpl c2b = new EntityImpl();
    c2.setProperty("c2b", c2b, YTType.LINK);
    final EntityImpl c3 = new EntityImpl();
    final EntityImpl c3a = new EntityImpl();
    c3.setProperty("c3a", c3a, YTType.LINK);
    final EntityImpl c3b = new EntityImpl();
    c3.setProperty("c3b", c3b, YTType.LINK);
    rootDocument.setProperty("c", new ArrayList<>(Arrays.asList(c1, c2, c3)), YTType.LINKLIST);

    db.executeInTx(() -> rootDocument.save(db.getClusterNameById(db.getDefaultClusterId())));

    rootDocument = db.bindToSession(rootDocument);
    final List<EntityImpl> expectedResult =
        Arrays.asList(
            rootDocument,
            db.bindToSession(a),
            db.bindToSession(aa),
            db.bindToSession(ab),
            db.bindToSession(b),
            db.bindToSession(ba),
            db.bindToSession(bb),
            db.bindToSession(c1),
            db.bindToSession(c1a),
            db.bindToSession(c1b),
            db.bindToSession(c2),
            db.bindToSession(c2a),
            db.bindToSession(c2b),
            db.bindToSession(c3),
            db.bindToSession(c3a),
            db.bindToSession(c3b));

    final List<YTIdentifiable> results = traverse.execute(db);

    compareTraverseResults(expectedResult, results);
  }

  @Test
  public void testBreadthTraverse() throws Exception {
    traverse.setStrategy(OTraverse.STRATEGY.BREADTH_FIRST);

    final EntityImpl aa = new EntityImpl();
    final EntityImpl ab = new EntityImpl();
    final EntityImpl ba = new EntityImpl();
    final EntityImpl bb = new EntityImpl();
    final EntityImpl a = new EntityImpl();
    a.setProperty("aa", aa, YTType.LINK);
    a.setProperty("ab", ab, YTType.LINK);
    final EntityImpl b = new EntityImpl();
    b.setProperty("ba", ba, YTType.LINK);
    b.setProperty("bb", bb, YTType.LINK);

    rootDocument.setProperty("a", a, YTType.LINK);
    rootDocument.setProperty("b", b, YTType.LINK);

    final EntityImpl c1 = new EntityImpl();
    final EntityImpl c1a = new EntityImpl();
    c1.setProperty("c1a", c1a, YTType.LINK);
    final EntityImpl c1b = new EntityImpl();
    c1.setProperty("c1b", c1b, YTType.LINK);
    final EntityImpl c2 = new EntityImpl();
    final EntityImpl c2a = new EntityImpl();
    c2.setProperty("c2a", c2a, YTType.LINK);
    final EntityImpl c2b = new EntityImpl();
    c2.setProperty("c2b", c2b, YTType.LINK);
    final EntityImpl c3 = new EntityImpl();
    final EntityImpl c3a = new EntityImpl();
    c3.setProperty("c3a", c3a, YTType.LINK);
    final EntityImpl c3b = new EntityImpl();
    c3.setProperty("c3b", c3b, YTType.LINK);
    rootDocument.setProperty("c", new ArrayList<>(Arrays.asList(c1, c2, c3)), YTType.LINKLIST);

    db.executeInTx(() -> rootDocument.save(db.getClusterNameById(db.getDefaultClusterId())));

    rootDocument = db.bindToSession(rootDocument);
    final List<EntityImpl> expectedResult =
        Arrays.asList(
            rootDocument,
            db.bindToSession(a),
            db.bindToSession(b),
            db.bindToSession(aa),
            db.bindToSession(ab),
            db.bindToSession(ba),
            db.bindToSession(bb),
            db.bindToSession(c1),
            db.bindToSession(c2),
            db.bindToSession(c3),
            db.bindToSession(c1a),
            db.bindToSession(c1b),
            db.bindToSession(c2a),
            db.bindToSession(c2b),
            db.bindToSession(c3a),
            db.bindToSession(c3b));

    final List<YTIdentifiable> results = traverse.execute(db);

    compareTraverseResults(expectedResult, results);
  }

  private void compareTraverseResults(List<EntityImpl> expectedResult,
      List<YTIdentifiable> results) {
    boolean equality = results.size() == expectedResult.size();
    for (int i = 0; i < expectedResult.size() && equality; i++) {
      equality &= results.get(i).equals(expectedResult.get(i));
    }
    System.out.println("Expected: " + expectedResult);
    System.out.println();
    System.out.println("Actual:   " + results);
    Assert.assertTrue(equality);
  }
}
