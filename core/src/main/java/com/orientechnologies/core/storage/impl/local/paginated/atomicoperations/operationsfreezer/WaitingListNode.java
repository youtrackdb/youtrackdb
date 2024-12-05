package com.orientechnologies.core.storage.impl.local.paginated.atomicoperations.operationsfreezer;

import com.orientechnologies.common.concur.lock.YTInterruptedException;
import com.orientechnologies.common.exception.YTException;
import java.util.concurrent.CountDownLatch;

final class WaitingListNode {

  /**
   * Latch which indicates that all links are created between add and existing list elements.
   */
  final CountDownLatch linkLatch = new CountDownLatch(1);

  final Thread item;
  volatile WaitingListNode next;

  WaitingListNode(Thread item) {
    this.item = item;
  }

  void waitTillAllLinksWillBeCreated() {
    try {
      linkLatch.await();
    } catch (InterruptedException e) {
      throw YTException.wrapException(
          new YTInterruptedException(
              "Thread was interrupted while was waiting for completion of 'waiting linked list'"
                  + " operation"),
          e);
    }
  }
}
