package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OTraverseTest extends DBTestBase {

  private YTEntityImpl rootDocument;
  private OTraverse traverse;

  public void beforeTest() throws Exception {
    super.beforeTest();

    rootDocument = new YTEntityImpl();
    traverse = new OTraverse(db);
    traverse.target(rootDocument).fields("*");
  }

  @Test
  public void testDepthTraverse() {

    final YTEntityImpl aa = new YTEntityImpl();
    final YTEntityImpl ab = new YTEntityImpl();
    final YTEntityImpl ba = new YTEntityImpl();
    final YTEntityImpl bb = new YTEntityImpl();
    final YTEntityImpl a = new YTEntityImpl();
    a.setProperty("aa", aa, YTType.LINK);
    a.setProperty("ab", ab, YTType.LINK);
    final YTEntityImpl b = new YTEntityImpl();
    b.setProperty("ba", ba, YTType.LINK);
    b.setProperty("bb", bb, YTType.LINK);

    rootDocument.setProperty("a", a, YTType.LINK);
    rootDocument.setProperty("b", b, YTType.LINK);

    final YTEntityImpl c1 = new YTEntityImpl();
    final YTEntityImpl c1a = new YTEntityImpl();
    c1.setProperty("c1a", c1a, YTType.LINK);
    final YTEntityImpl c1b = new YTEntityImpl();
    c1.setProperty("c1b", c1b, YTType.LINK);
    final YTEntityImpl c2 = new YTEntityImpl();
    final YTEntityImpl c2a = new YTEntityImpl();
    c2.setProperty("c2a", c2a, YTType.LINK);
    final YTEntityImpl c2b = new YTEntityImpl();
    c2.setProperty("c2b", c2b, YTType.LINK);
    final YTEntityImpl c3 = new YTEntityImpl();
    final YTEntityImpl c3a = new YTEntityImpl();
    c3.setProperty("c3a", c3a, YTType.LINK);
    final YTEntityImpl c3b = new YTEntityImpl();
    c3.setProperty("c3b", c3b, YTType.LINK);
    rootDocument.setProperty("c", new ArrayList<>(Arrays.asList(c1, c2, c3)), YTType.LINKLIST);

    db.executeInTx(() -> rootDocument.save(db.getClusterNameById(db.getDefaultClusterId())));

    rootDocument = db.bindToSession(rootDocument);
    final List<YTEntityImpl> expectedResult =
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

    final YTEntityImpl aa = new YTEntityImpl();
    final YTEntityImpl ab = new YTEntityImpl();
    final YTEntityImpl ba = new YTEntityImpl();
    final YTEntityImpl bb = new YTEntityImpl();
    final YTEntityImpl a = new YTEntityImpl();
    a.setProperty("aa", aa, YTType.LINK);
    a.setProperty("ab", ab, YTType.LINK);
    final YTEntityImpl b = new YTEntityImpl();
    b.setProperty("ba", ba, YTType.LINK);
    b.setProperty("bb", bb, YTType.LINK);

    rootDocument.setProperty("a", a, YTType.LINK);
    rootDocument.setProperty("b", b, YTType.LINK);

    final YTEntityImpl c1 = new YTEntityImpl();
    final YTEntityImpl c1a = new YTEntityImpl();
    c1.setProperty("c1a", c1a, YTType.LINK);
    final YTEntityImpl c1b = new YTEntityImpl();
    c1.setProperty("c1b", c1b, YTType.LINK);
    final YTEntityImpl c2 = new YTEntityImpl();
    final YTEntityImpl c2a = new YTEntityImpl();
    c2.setProperty("c2a", c2a, YTType.LINK);
    final YTEntityImpl c2b = new YTEntityImpl();
    c2.setProperty("c2b", c2b, YTType.LINK);
    final YTEntityImpl c3 = new YTEntityImpl();
    final YTEntityImpl c3a = new YTEntityImpl();
    c3.setProperty("c3a", c3a, YTType.LINK);
    final YTEntityImpl c3b = new YTEntityImpl();
    c3.setProperty("c3b", c3b, YTType.LINK);
    rootDocument.setProperty("c", new ArrayList<>(Arrays.asList(c1, c2, c3)), YTType.LINKLIST);

    db.executeInTx(() -> rootDocument.save(db.getClusterNameById(db.getDefaultClusterId())));

    rootDocument = db.bindToSession(rootDocument);
    final List<YTEntityImpl> expectedResult =
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

  private void compareTraverseResults(List<YTEntityImpl> expectedResult,
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
