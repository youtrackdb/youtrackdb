package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.CommandInterruptedException;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperationsManager;
import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HashTableDirectoryV2Test extends DbTestBase {

  private HashTableDirectory directory;

  @Before
  public void before() throws IOException {
    AbstractPaginatedStorage storage = (AbstractPaginatedStorage) db.getStorage();
    directory =
        new HashTableDirectory(".tsc", "hashTableDirectoryTest", "hashTableDirectoryTest", storage);

    final AtomicOperation atomicOperation = startTx();
    directory.create(atomicOperation);
    completeTx();
  }

  @After
  public void afterClass() throws Exception {
    final AtomicOperation atomicOperation = startTx();
    directory.delete(atomicOperation);
    completeTx();
  }

  private AtomicOperation startTx() throws IOException {
    AbstractPaginatedStorage storage = (AbstractPaginatedStorage) db.getStorage();
    AtomicOperationsManager manager = storage.getAtomicOperationsManager();
    Assert.assertNull(manager.getCurrentOperation());
    return manager.startAtomicOperation(null);
  }

  private void rollbackTx() throws IOException {
    AbstractPaginatedStorage storage = (AbstractPaginatedStorage) db.getStorage();
    AtomicOperationsManager manager = storage.getAtomicOperationsManager();
    manager.endAtomicOperation(new CommandInterruptedException(""));
    Assert.assertNull(manager.getCurrentOperation());
  }

  private void completeTx() throws IOException {
    AbstractPaginatedStorage storage = (AbstractPaginatedStorage) db.getStorage();
    AtomicOperationsManager manager = storage.getAtomicOperationsManager();
    manager.endAtomicOperation(null);
    Assert.assertNull(manager.getCurrentOperation());
  }

  @Test
  public void addFirstLevel() throws IOException {
    AtomicOperation atomicOperation = startTx();

    long[] level = new long[LocalHashTableV2.MAX_LEVEL_SIZE];
    for (int i = 0; i < level.length; i++) {
      level[i] = i;
    }

    int index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);

    Assert.assertEquals(0, index);
    Assert.assertEquals(2, directory.getMaxLeftChildDepth(0, atomicOperation));
    Assert.assertEquals(3, directory.getMaxRightChildDepth(0, atomicOperation));
    Assert.assertEquals(4, directory.getNodeLocalDepth(0, atomicOperation));

    Assertions.assertThat(directory.getNode(0, atomicOperation)).isEqualTo(level);

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(0, i, atomicOperation), i);
    }
    rollbackTx();
  }

  @Test
  public void changeFirstLevel() throws IOException {
    AtomicOperation atomicOperation = startTx();
    long[] level = new long[LocalHashTableV2.MAX_LEVEL_SIZE];
    for (int i = 0; i < level.length; i++) {
      level[i] = i;
    }

    directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);

    for (int i = 0; i < level.length; i++) {
      directory.setNodePointer(0, i, i + 100, atomicOperation);
    }

    directory.setMaxLeftChildDepth(0, (byte) 100, atomicOperation);
    directory.setMaxRightChildDepth(0, (byte) 101, atomicOperation);
    directory.setNodeLocalDepth(0, (byte) 102, atomicOperation);

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(0, i, atomicOperation), i + 100);
    }

    Assert.assertEquals(100, directory.getMaxLeftChildDepth(0, atomicOperation));
    Assert.assertEquals(101, directory.getMaxRightChildDepth(0, atomicOperation));
    Assert.assertEquals(102, directory.getNodeLocalDepth(0, atomicOperation));

    rollbackTx();
  }

  @Test
  public void addThreeRemoveSecondAddNewAndChange() throws IOException {
    AtomicOperation atomicOperation = startTx();

    long[] level = new long[LocalHashTableV2.MAX_LEVEL_SIZE];
    for (int i = 0; i < level.length; i++) {
      level[i] = i;
    }

    int index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(0, index);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 100;
    }

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(1, index);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 200;
    }

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(2, index);

    directory.deleteNode(1, atomicOperation);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 300;
    }

    index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    Assert.assertEquals(1, index);

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(1, i, atomicOperation), i + 300);
    }

    Assert.assertEquals(5, directory.getMaxLeftChildDepth(1, atomicOperation));
    Assert.assertEquals(6, directory.getMaxRightChildDepth(1, atomicOperation));
    Assert.assertEquals(7, directory.getNodeLocalDepth(1, atomicOperation));

    rollbackTx();
  }

  @Test
  public void addRemoveChangeMix() throws IOException {
    AtomicOperation atomicOperation = startTx();

    long[] level = new long[LocalHashTableV2.MAX_LEVEL_SIZE];
    for (int i = 0; i < level.length; i++) {
      level[i] = i;
    }

    int index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(0, index);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 100;
    }

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(1, index);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 200;
    }

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(2, index);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 300;
    }

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(3, index);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 400;
    }

    index = directory.addNewNode((byte) 2, (byte) 3, (byte) 4, level, atomicOperation);
    Assert.assertEquals(4, index);

    directory.deleteNode(1, atomicOperation);
    directory.deleteNode(3, atomicOperation);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 500;
    }

    index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    Assert.assertEquals(3, index);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 600;
    }

    index = directory.addNewNode((byte) 8, (byte) 9, (byte) 10, level, atomicOperation);
    Assert.assertEquals(1, index);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 700;
    }

    index = directory.addNewNode((byte) 11, (byte) 12, (byte) 13, level, atomicOperation);
    Assert.assertEquals(5, index);

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(3, i, atomicOperation), i + 500);
    }

    Assert.assertEquals(5, directory.getMaxLeftChildDepth(3, atomicOperation));
    Assert.assertEquals(6, directory.getMaxRightChildDepth(3, atomicOperation));
    Assert.assertEquals(7, directory.getNodeLocalDepth(3, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(1, i, atomicOperation), i + 600);
    }

    Assert.assertEquals(8, directory.getMaxLeftChildDepth(1, atomicOperation));
    Assert.assertEquals(9, directory.getMaxRightChildDepth(1, atomicOperation));
    Assert.assertEquals(10, directory.getNodeLocalDepth(1, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(5, i, atomicOperation), i + 700);
    }

    Assert.assertEquals(11, directory.getMaxLeftChildDepth(5, atomicOperation));
    Assert.assertEquals(12, directory.getMaxRightChildDepth(5, atomicOperation));
    Assert.assertEquals(13, directory.getNodeLocalDepth(5, atomicOperation));

    rollbackTx();
  }

  @Test
  public void addThreePages() throws IOException {
    AtomicOperation atomicOperation = startTx();

    int firsIndex = -1;
    int secondIndex = -1;
    int thirdIndex = -1;

    long[] level = new long[LocalHashTableV2.MAX_LEVEL_SIZE];

    for (int n = 0; n < DirectoryFirstPageV2.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++) {
        level[i] = i + n * 100L;
      }

      int index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
      if (firsIndex < 0) {
        firsIndex = index;
      }
    }

    for (int n = 0; n < DirectoryPageV2.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++) {
        level[i] = i + n * 100L;
      }

      int index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
      if (secondIndex < 0) {
        secondIndex = index;
      }
    }

    for (int n = 0; n < DirectoryPageV2.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++) {
        level[i] = i + n * 100L;
      }

      int index = directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
      if (thirdIndex < 0) {
        thirdIndex = index;
      }
    }

    Assert.assertEquals(0, firsIndex);
    Assert.assertEquals(DirectoryFirstPageV2.NODES_PER_PAGE, secondIndex);
    Assert.assertEquals(
        DirectoryFirstPageV2.NODES_PER_PAGE + DirectoryPageV2.NODES_PER_PAGE, thirdIndex);

    directory.deleteNode(secondIndex, atomicOperation);
    directory.deleteNode(firsIndex, atomicOperation);
    directory.deleteNode(thirdIndex, atomicOperation);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 1000;
    }

    int index = directory.addNewNode((byte) 8, (byte) 9, (byte) 10, level, atomicOperation);
    Assert.assertEquals(index, thirdIndex);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 2000;
    }

    index = directory.addNewNode((byte) 11, (byte) 12, (byte) 13, level, atomicOperation);
    Assert.assertEquals(index, firsIndex);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 3000;
    }

    index = directory.addNewNode((byte) 14, (byte) 15, (byte) 16, level, atomicOperation);
    Assert.assertEquals(index, secondIndex);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 4000;
    }

    index = directory.addNewNode((byte) 17, (byte) 18, (byte) 19, level, atomicOperation);
    Assert.assertEquals(
        DirectoryFirstPageV2.NODES_PER_PAGE + 2L * DirectoryPageV2.NODES_PER_PAGE, index);

    Assert.assertEquals(8, directory.getMaxLeftChildDepth(thirdIndex, atomicOperation));
    Assert.assertEquals(9, directory.getMaxRightChildDepth(thirdIndex, atomicOperation));
    Assert.assertEquals(10, directory.getNodeLocalDepth(thirdIndex, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(thirdIndex, i, atomicOperation), i + 1000);
    }

    Assert.assertEquals(11, directory.getMaxLeftChildDepth(firsIndex, atomicOperation));
    Assert.assertEquals(12, directory.getMaxRightChildDepth(firsIndex, atomicOperation));
    Assert.assertEquals(13, directory.getNodeLocalDepth(firsIndex, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(firsIndex, i, atomicOperation), i + 2000);
    }

    Assert.assertEquals(14, directory.getMaxLeftChildDepth(secondIndex, atomicOperation));
    Assert.assertEquals(15, directory.getMaxRightChildDepth(secondIndex, atomicOperation));
    Assert.assertEquals(16, directory.getNodeLocalDepth(secondIndex, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(secondIndex, i, atomicOperation), i + 3000);
    }

    final int lastIndex = DirectoryFirstPageV2.NODES_PER_PAGE + 2 * DirectoryPageV2.NODES_PER_PAGE;

    Assert.assertEquals(17, directory.getMaxLeftChildDepth(lastIndex, atomicOperation));
    Assert.assertEquals(18, directory.getMaxRightChildDepth(lastIndex, atomicOperation));
    Assert.assertEquals(19, directory.getNodeLocalDepth(lastIndex, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(lastIndex, i, atomicOperation), i + 4000);
    }
    rollbackTx();
  }

  @Test
  public void changeLastNodeSecondPage() throws IOException {
    AtomicOperation atomicOperation = startTx();

    long[] level = new long[LocalHashTableV2.MAX_LEVEL_SIZE];

    for (int n = 0; n < DirectoryFirstPageV2.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++) {
        level[i] = i + n * 100L;
      }

      directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    }

    for (int n = 0; n < DirectoryPageV2.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++) {
        level[i] = i + n * 100L;
      }

      directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    }

    for (int n = 0; n < DirectoryPageV2.NODES_PER_PAGE; n++) {
      for (int i = 0; i < level.length; i++) {
        level[i] = i + n * 100L;
      }

      directory.addNewNode((byte) 5, (byte) 6, (byte) 7, level, atomicOperation);
    }

    directory.deleteNode(
        DirectoryFirstPageV2.NODES_PER_PAGE + DirectoryPageV2.NODES_PER_PAGE - 1, atomicOperation);

    for (int i = 0; i < level.length; i++) {
      level[i] = i + 1000;
    }

    int index = directory.addNewNode((byte) 8, (byte) 9, (byte) 10, level, atomicOperation);
    Assert.assertEquals(
        DirectoryFirstPageV2.NODES_PER_PAGE + DirectoryPageV2.NODES_PER_PAGE - 1, index);

    directory.setMaxLeftChildDepth(index - 1, (byte) 10, atomicOperation);
    directory.setMaxRightChildDepth(index - 1, (byte) 11, atomicOperation);
    directory.setNodeLocalDepth(index - 1, (byte) 12, atomicOperation);

    for (int i = 0; i < level.length; i++) {
      directory.setNodePointer(index - 1, i, i + 2000, atomicOperation);
    }

    directory.setMaxLeftChildDepth(index + 1, (byte) 13, atomicOperation);
    directory.setMaxRightChildDepth(index + 1, (byte) 14, atomicOperation);
    directory.setNodeLocalDepth(index + 1, (byte) 15, atomicOperation);

    for (int i = 0; i < level.length; i++) {
      directory.setNodePointer(index + 1, i, i + 3000, atomicOperation);
    }

    Assert.assertEquals(10, directory.getMaxLeftChildDepth(index - 1, atomicOperation));
    Assert.assertEquals(11, directory.getMaxRightChildDepth(index - 1, atomicOperation));
    Assert.assertEquals(12, directory.getNodeLocalDepth(index - 1, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(index - 1, i, atomicOperation), i + 2000);
    }

    Assert.assertEquals(8, directory.getMaxLeftChildDepth(index, atomicOperation));
    Assert.assertEquals(9, directory.getMaxRightChildDepth(index, atomicOperation));
    Assert.assertEquals(10, directory.getNodeLocalDepth(index, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(index, i, atomicOperation), i + 1000);
    }

    Assert.assertEquals(13, directory.getMaxLeftChildDepth(index + 1, atomicOperation));
    Assert.assertEquals(14, directory.getMaxRightChildDepth(index + 1, atomicOperation));
    Assert.assertEquals(15, directory.getNodeLocalDepth(index + 1, atomicOperation));

    for (int i = 0; i < level.length; i++) {
      Assert.assertEquals(directory.getNodePointer(index + 1, i, atomicOperation), i + 3000);
    }
    rollbackTx();
  }
}
