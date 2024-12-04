package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OTraverseTest extends DBTestBase {

  private YTDocument rootDocument;
  private OTraverse traverse;

  public void beforeTest() throws Exception {
    super.beforeTest();

    rootDocument = new YTDocument();
    traverse = new OTraverse(db);
    traverse.target(rootDocument).fields("*");
  }

  @Test
  public void testDepthTraverse() {

    final YTDocument aa = new YTDocument();
    final YTDocument ab = new YTDocument();
    final YTDocument ba = new YTDocument();
    final YTDocument bb = new YTDocument();
    final YTDocument a = new YTDocument();
    a.setProperty("aa", aa, YTType.LINK);
    a.setProperty("ab", ab, YTType.LINK);
    final YTDocument b = new YTDocument();
    b.setProperty("ba", ba, YTType.LINK);
    b.setProperty("bb", bb, YTType.LINK);

    rootDocument.setProperty("a", a, YTType.LINK);
    rootDocument.setProperty("b", b, YTType.LINK);

    final YTDocument c1 = new YTDocument();
    final YTDocument c1a = new YTDocument();
    c1.setProperty("c1a", c1a, YTType.LINK);
    final YTDocument c1b = new YTDocument();
    c1.setProperty("c1b", c1b, YTType.LINK);
    final YTDocument c2 = new YTDocument();
    final YTDocument c2a = new YTDocument();
    c2.setProperty("c2a", c2a, YTType.LINK);
    final YTDocument c2b = new YTDocument();
    c2.setProperty("c2b", c2b, YTType.LINK);
    final YTDocument c3 = new YTDocument();
    final YTDocument c3a = new YTDocument();
    c3.setProperty("c3a", c3a, YTType.LINK);
    final YTDocument c3b = new YTDocument();
    c3.setProperty("c3b", c3b, YTType.LINK);
    rootDocument.setProperty("c", new ArrayList<>(Arrays.asList(c1, c2, c3)), YTType.LINKLIST);

    db.executeInTx(() -> rootDocument.save(db.getClusterNameById(db.getDefaultClusterId())));

    rootDocument = db.bindToSession(rootDocument);
    final List<YTDocument> expectedResult =
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

    final YTDocument aa = new YTDocument();
    final YTDocument ab = new YTDocument();
    final YTDocument ba = new YTDocument();
    final YTDocument bb = new YTDocument();
    final YTDocument a = new YTDocument();
    a.setProperty("aa", aa, YTType.LINK);
    a.setProperty("ab", ab, YTType.LINK);
    final YTDocument b = new YTDocument();
    b.setProperty("ba", ba, YTType.LINK);
    b.setProperty("bb", bb, YTType.LINK);

    rootDocument.setProperty("a", a, YTType.LINK);
    rootDocument.setProperty("b", b, YTType.LINK);

    final YTDocument c1 = new YTDocument();
    final YTDocument c1a = new YTDocument();
    c1.setProperty("c1a", c1a, YTType.LINK);
    final YTDocument c1b = new YTDocument();
    c1.setProperty("c1b", c1b, YTType.LINK);
    final YTDocument c2 = new YTDocument();
    final YTDocument c2a = new YTDocument();
    c2.setProperty("c2a", c2a, YTType.LINK);
    final YTDocument c2b = new YTDocument();
    c2.setProperty("c2b", c2b, YTType.LINK);
    final YTDocument c3 = new YTDocument();
    final YTDocument c3a = new YTDocument();
    c3.setProperty("c3a", c3a, YTType.LINK);
    final YTDocument c3b = new YTDocument();
    c3.setProperty("c3b", c3b, YTType.LINK);
    rootDocument.setProperty("c", new ArrayList<>(Arrays.asList(c1, c2, c3)), YTType.LINKLIST);

    db.executeInTx(() -> rootDocument.save(db.getClusterNameById(db.getDefaultClusterId())));

    rootDocument = db.bindToSession(rootDocument);
    final List<YTDocument> expectedResult =
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

  private void compareTraverseResults(List<YTDocument> expectedResult,
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
