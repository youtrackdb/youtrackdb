package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionData;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class BackgroundNewDelta implements Runnable, SyncSource {

  public static final int CHUNK_MAX_SIZE = 8388608; // 8MB
  private final List<FrontendTransactionData> transactions;
  private final PipedOutputStream outputStream;
  private final InputStream inputStream;
  private final CountDownLatch finished = new CountDownLatch(1);

  public BackgroundNewDelta(List<FrontendTransactionData> transactions) throws IOException {
    this.transactions = transactions;
    outputStream = new PipedOutputStream();
    inputStream = new PipedInputStream(outputStream, CHUNK_MAX_SIZE);
    Thread t = new Thread(this);
    t.setDaemon(true);
    t.start();
  }

  @Override
  public void run() {
    try {
      DataOutput output = new DataOutputStream(outputStream);
      for (FrontendTransactionData transaction : transactions) {
        output.writeBoolean(true);
        transaction.write(output);
      }
      output.writeBoolean(false);
      outputStream.close();
    } catch (IOException e) {
      LogManager.instance().debug(this, "Error on network delta serialization", e);
    } finally {
      finished.countDown();
    }
  }

  @Override
  public boolean getIncremental() {
    return false;
  }

  @Override
  public InputStream getInputStream() {
    return inputStream;
  }

  @Override
  public CountDownLatch getFinished() {
    return finished;
  }

  @Override
  public boolean isValid() {
    return false;
  }

  @Override
  public void invalidate() {
    // DO NOTHING IS INVALID BY DEFINITION
  }
}
