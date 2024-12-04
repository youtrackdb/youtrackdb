package com.orientechnologies.common.comparator;

import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import java.util.Comparator;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 11.07.12
 */
public class DefaultComparatorTest {

  private final ODefaultComparator comparator = ODefaultComparator.INSTANCE;

  @Test
  public void testCompareStrings() {
    final OCompositeKey keyOne = new OCompositeKey("name4", YTType.STRING);
    final OCompositeKey keyTwo = new OCompositeKey("name5", YTType.STRING);

    assertCompareTwoKeys(comparator, keyOne, keyTwo);
  }

  private void assertCompareTwoKeys(
      final Comparator<Object> comparator, final Object keyOne, final Object keyTwo) {
    Assert.assertTrue(comparator.compare(keyOne, keyTwo) < 0);
    Assert.assertTrue(comparator.compare(keyTwo, keyOne) > 0);
    Assert.assertEquals(0, comparator.compare(keyTwo, keyTwo));
  }
}
