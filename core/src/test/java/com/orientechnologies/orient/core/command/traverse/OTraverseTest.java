package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OTraverseTest extends BaseMemoryDatabase {

  private ODocument rootDocument;
  private OTraverse traverse;

  public void beforeTest() {
    super.beforeTest();

    rootDocument = new ODocument();
    traverse = new OTraverse();
    traverse.target(rootDocument).fields("*");
  }

  @Test
  public void testDepthTraverse() {

    final ODocument aa = new ODocument();
    final ODocument ab = new ODocument();
    final ODocument ba = new ODocument();
    final ODocument bb = new ODocument();
    final ODocument a = new ODocument();
    a.setProperty("aa", aa, OType.LINK);
    a.setProperty("ab", ab, OType.LINK);
    final ODocument b = new ODocument();
    b.setProperty("ba", ba, OType.LINK);
    b.setProperty("bb", bb, OType.LINK);

    rootDocument.setProperty("a", a, OType.LINK);
    rootDocument.setProperty("b", b, OType.LINK);

    final ODocument c1 = new ODocument();
    final ODocument c1a = new ODocument();
    c1.setProperty("c1a", c1a, OType.LINK);
    final ODocument c1b = new ODocument();
    c1.setProperty("c1b", c1b, OType.LINK);
    final ODocument c2 = new ODocument();
    final ODocument c2a = new ODocument();
    c2.setProperty("c2a", c2a, OType.LINK);
    final ODocument c2b = new ODocument();
    c2.setProperty("c2b", c2b, OType.LINK);
    final ODocument c3 = new ODocument();
    final ODocument c3a = new ODocument();
    c3.setProperty("c3a", c3a, OType.LINK);
    final ODocument c3b = new ODocument();
    c3.setProperty("c3b", c3b, OType.LINK);
    rootDocument.setProperty("c", new ArrayList<>(Arrays.asList(c1, c2, c3)), OType.LINKLIST);

    rootDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    final List<ODocument> expectedResult =
        Arrays.asList(rootDocument, a, aa, ab, b, ba, bb, c1, c1a, c1b, c2, c2a, c2b, c3, c3a, c3b);

    final List<OIdentifiable> results = traverse.execute();

    compareTraverseResults(expectedResult, results);
  }

  @Test
  public void testBreadthTraverse() throws Exception {
    traverse.setStrategy(OTraverse.STRATEGY.BREADTH_FIRST);

    final ODocument aa = new ODocument();
    final ODocument ab = new ODocument();
    final ODocument ba = new ODocument();
    final ODocument bb = new ODocument();
    final ODocument a = new ODocument();
    a.setProperty("aa", aa, OType.LINK);
    a.setProperty("ab", ab, OType.LINK);
    final ODocument b = new ODocument();
    b.setProperty("ba", ba, OType.LINK);
    b.setProperty("bb", bb, OType.LINK);

    rootDocument.setProperty("a", a, OType.LINK);
    rootDocument.setProperty("b", b, OType.LINK);

    final ODocument c1 = new ODocument();
    final ODocument c1a = new ODocument();
    c1.setProperty("c1a", c1a, OType.LINK);
    final ODocument c1b = new ODocument();
    c1.setProperty("c1b", c1b, OType.LINK);
    final ODocument c2 = new ODocument();
    final ODocument c2a = new ODocument();
    c2.setProperty("c2a", c2a, OType.LINK);
    final ODocument c2b = new ODocument();
    c2.setProperty("c2b", c2b, OType.LINK);
    final ODocument c3 = new ODocument();
    final ODocument c3a = new ODocument();
    c3.setProperty("c3a", c3a, OType.LINK);
    final ODocument c3b = new ODocument();
    c3.setProperty("c3b", c3b, OType.LINK);
    rootDocument.setProperty("c", new ArrayList<>(Arrays.asList(c1, c2, c3)), OType.LINKLIST);

    rootDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    final List<ODocument> expectedResult =
        Arrays.asList(rootDocument, a, b, aa, ab, ba, bb, c1, c2, c3, c1a, c1b, c2a, c2b, c3a, c3b);

    final List<OIdentifiable> results = traverse.execute();

    compareTraverseResults(expectedResult, results);
  }

  private void compareTraverseResults(List<ODocument> expectedResult, List<OIdentifiable> results) {
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
