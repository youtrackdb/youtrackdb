package com.jetbrains.youtrack.db.internal.common.collection.closabledictionary;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ClosableLRUListTest {

  @Test
  public void tesMoveToTail() {
    ClosableLRUList<Long, CIItem> lruList = new ClosableLRUList<Long, CIItem>();
    ClosableEntry<Long, CIItem> firstEntry = new ClosableEntry<Long, CIItem>(new CIItem());

    Assert.assertFalse(lruList.contains(firstEntry));
    Assert.assertEquals(lruList.size(), 0);

    lruList.moveToTheTail(firstEntry);
    Assert.assertEquals(lruList.size(), 1);
    Assert.assertTrue(lruList.contains(firstEntry));
    assertContent(lruList, new ClosableEntry[]{firstEntry});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    ClosableEntry<Long, CIItem> secondEntry = new ClosableEntry<Long, CIItem>(new CIItem());

    lruList.moveToTheTail(secondEntry);

    Assert.assertEquals(lruList.size(), 2);
    Assert.assertTrue(lruList.contains(firstEntry));
    Assert.assertTrue(lruList.contains(secondEntry));
    assertContent(lruList, new ClosableEntry[]{firstEntry, secondEntry});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    ClosableEntry<Long, CIItem> thirdEntry = new ClosableEntry<Long, CIItem>(new CIItem());
    lruList.moveToTheTail(thirdEntry);

    Assert.assertEquals(lruList.size(), 3);
    Assert.assertTrue(lruList.contains(firstEntry));
    Assert.assertTrue(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));
    assertContent(lruList, new ClosableEntry[]{firstEntry, secondEntry, thirdEntry});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    lruList.moveToTheTail(secondEntry);
    Assert.assertEquals(lruList.size(), 3);
    Assert.assertTrue(lruList.contains(firstEntry));
    Assert.assertTrue(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));
    assertContent(lruList, new ClosableEntry[]{firstEntry, thirdEntry, secondEntry});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    lruList.moveToTheTail(firstEntry);
    Assert.assertEquals(lruList.size(), 3);
    Assert.assertTrue(lruList.contains(firstEntry));
    Assert.assertTrue(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));
    assertContent(lruList, new ClosableEntry[]{thirdEntry, secondEntry, firstEntry});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    lruList.moveToTheTail(firstEntry);
    Assert.assertEquals(lruList.size(), 3);
    Assert.assertTrue(lruList.contains(firstEntry));
    Assert.assertTrue(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));
    assertContent(lruList, new ClosableEntry[]{thirdEntry, secondEntry, firstEntry});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());
  }

  @Test
  public void tesRemove() {
    ClosableLRUList<Long, CIItem> lruList = new ClosableLRUList<Long, CIItem>();

    ClosableEntry<Long, CIItem> firstEntry = new ClosableEntry<Long, CIItem>(new CIItem());
    ClosableEntry<Long, CIItem> secondEntry = new ClosableEntry<Long, CIItem>(new CIItem());
    ClosableEntry<Long, CIItem> thirdEntry = new ClosableEntry<Long, CIItem>(new CIItem());

    lruList.moveToTheTail(firstEntry);
    lruList.moveToTheTail(secondEntry);
    lruList.moveToTheTail(thirdEntry);

    lruList.remove(firstEntry);
    assertContent(lruList, new ClosableEntry[]{secondEntry, thirdEntry});

    Assert.assertEquals(lruList.size(), 2);
    Assert.assertFalse(lruList.contains(firstEntry));
    Assert.assertTrue(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    lruList.remove(thirdEntry);
    assertContent(lruList, new ClosableEntry[]{secondEntry});

    Assert.assertEquals(lruList.size(), 1);
    Assert.assertFalse(lruList.contains(firstEntry));
    Assert.assertTrue(lruList.contains(secondEntry));
    Assert.assertFalse(lruList.contains(thirdEntry));

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    lruList.remove(secondEntry);
    assertContent(lruList, new ClosableEntry[]{});

    Assert.assertEquals(lruList.size(), 0);
    Assert.assertFalse(lruList.contains(firstEntry));
    Assert.assertFalse(lruList.contains(secondEntry));
    Assert.assertFalse(lruList.contains(thirdEntry));

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    lruList.remove(secondEntry);
    assertContent(lruList, new ClosableEntry[]{});

    Assert.assertEquals(lruList.size(), 0);
    Assert.assertFalse(lruList.contains(firstEntry));
    Assert.assertFalse(lruList.contains(secondEntry));
    Assert.assertFalse(lruList.contains(thirdEntry));

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    lruList.moveToTheTail(firstEntry);
    lruList.moveToTheTail(secondEntry);
    lruList.moveToTheTail(thirdEntry);

    lruList.remove(secondEntry);
    assertContent(lruList, new ClosableEntry[]{firstEntry, thirdEntry});

    Assert.assertEquals(lruList.size(), 2);
    Assert.assertTrue(lruList.contains(firstEntry));
    Assert.assertFalse(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    lruList.moveToTheTail(secondEntry);
    assertContent(lruList, new ClosableEntry[]{firstEntry, thirdEntry, secondEntry});
    Assert.assertEquals(lruList.size(), 3);

    lruList.remove(secondEntry);
    assertContent(lruList, new ClosableEntry[]{firstEntry, thirdEntry});

    Assert.assertEquals(lruList.size(), 2);
    Assert.assertTrue(lruList.contains(firstEntry));
    Assert.assertFalse(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());
  }

  @Test
  public void testPool() {
    ClosableLRUList<Long, CIItem> lruList = new ClosableLRUList<Long, CIItem>();

    ClosableEntry<Long, CIItem> firstEntry = new ClosableEntry<Long, CIItem>(new CIItem());
    ClosableEntry<Long, CIItem> secondEntry = new ClosableEntry<Long, CIItem>(new CIItem());
    ClosableEntry<Long, CIItem> thirdEntry = new ClosableEntry<Long, CIItem>(new CIItem());

    lruList.moveToTheTail(firstEntry);
    lruList.moveToTheTail(secondEntry);
    lruList.moveToTheTail(thirdEntry);

    ClosableEntry<Long, CIItem> removed = lruList.poll();
    Assert.assertSame(removed, firstEntry);
    Assert.assertEquals(lruList.size(), 2);

    Assert.assertFalse(lruList.contains(firstEntry));
    Assert.assertTrue(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));

    assertContent(lruList, new ClosableEntry[]{secondEntry, thirdEntry});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    removed = lruList.poll();
    Assert.assertSame(removed, secondEntry);
    Assert.assertEquals(lruList.size(), 1);

    Assert.assertFalse(lruList.contains(firstEntry));
    Assert.assertFalse(lruList.contains(secondEntry));
    Assert.assertTrue(lruList.contains(thirdEntry));

    assertContent(lruList, new ClosableEntry[]{thirdEntry});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    removed = lruList.poll();
    Assert.assertSame(removed, thirdEntry);
    Assert.assertEquals(lruList.size(), 0);

    Assert.assertFalse(lruList.contains(firstEntry));
    Assert.assertFalse(lruList.contains(secondEntry));
    Assert.assertFalse(lruList.contains(thirdEntry));

    assertContent(lruList, new ClosableEntry[]{});

    Assert.assertTrue(lruList.assertBackwardStructure());
    Assert.assertTrue(lruList.assertForwardStructure());

    removed = lruList.poll();
    Assert.assertNull(removed);
  }

  public class CIItem implements ClosableItem {

    private volatile boolean open = true;

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      open = false;
    }

    @Override
    public void open() {
      open = true;
    }
  }

  private void assertContent(
      ClosableLRUList<Long, CIItem> lruList, ClosableEntry<Long, CIItem>[] entries) {
    final List<ClosableEntry<Long, CIItem>> entryList = Arrays.asList(entries);

    final Iterator<ClosableEntry<Long, CIItem>> iterator = entryList.iterator();
    for (ClosableEntry<Long, CIItem> entry : lruList) {
      Assert.assertTrue(iterator.hasNext());
      Assert.assertSame(entry, iterator.next());
    }

    Assert.assertFalse(iterator.hasNext());
  }
}
