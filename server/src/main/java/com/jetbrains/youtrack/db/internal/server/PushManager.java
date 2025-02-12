package com.jetbrains.youtrack.db.internal.server;

import com.jetbrains.youtrack.db.internal.client.remote.message.PushSchemaRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.PushSequencesRequest;
import com.jetbrains.youtrack.db.internal.client.remote.message.PushStorageConfigurationRequest;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.thread.ThreadPoolExecutors;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.MetadataUpdateListener;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;
import com.jetbrains.youtrack.db.internal.server.network.protocol.binary.NetworkProtocolBinary;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class PushManager implements MetadataUpdateListener {

  protected final Set<WeakReference<NetworkProtocolBinary>> distributedConfigPush =
      new HashSet<>();
  protected final PushEventType storageConfigurations = new PushEventType();
  protected final PushEventType schema = new PushEventType();
  protected final PushEventType indexManager = new PushEventType();
  protected final PushEventType functions = new PushEventType();
  protected final PushEventType sequences = new PushEventType();
  private final Set<String> registerDatabase = new HashSet<>();
  private final ExecutorService executor;

  public PushManager() {
    executor =
        ThreadPoolExecutors.newCachedThreadPool(
            "Push Requests", Thread.currentThread().getThreadGroup(), 5, 0);
  }

  public synchronized void cleanPushSockets() {
    var iter = distributedConfigPush.iterator();
    while (iter.hasNext()) {
      if (iter.next().get() == null) {
        iter.remove();
      }
    }
    storageConfigurations.cleanListeners();
    schema.cleanListeners();
    indexManager.cleanListeners();
    functions.cleanListeners();
    sequences.cleanListeners();
  }

  public void shutdown() {
    executor.shutdownNow();
  }

  private void genericSubscribe(
      PushEventType context, DatabaseSessionInternal database, NetworkProtocolBinary protocol) {
    if (!registerDatabase.contains(database.getDatabaseName())) {
      database.getSharedContext().registerListener(this);
      registerDatabase.add(database.getDatabaseName());
    }
    context.subscribe(database.getDatabaseName(), protocol);
  }

  public synchronized void subscribeStorageConfiguration(
      DatabaseSessionInternal database, NetworkProtocolBinary protocol) {
    genericSubscribe(storageConfigurations, database, protocol);
  }

  public synchronized void subscribeSchema(
      DatabaseSessionInternal database, NetworkProtocolBinary protocol) {
    genericSubscribe(schema, database, protocol);
  }

  public synchronized void subscribeIndexManager(
      DatabaseSessionInternal database, NetworkProtocolBinary protocol) {
    genericSubscribe(indexManager, database, protocol);
  }

  public synchronized void subscribeFunctions(
      DatabaseSessionInternal database, NetworkProtocolBinary protocol) {
    genericSubscribe(functions, database, protocol);
  }

  public synchronized void subscribeSequences(
      DatabaseSessionInternal database, NetworkProtocolBinary protocol) {
    genericSubscribe(sequences, database, protocol);
  }

  @Override
  public void onSchemaUpdate(DatabaseSessionInternal db, String database,
      SchemaShared schema) {
    var request = new PushSchemaRequest();
    this.schema.send(db, database, request, this);
  }

  @Override
  public void onSequenceLibraryUpdate(DatabaseSessionInternal session, String database) {
    var request = new PushSequencesRequest();
    this.sequences.send(session, database, request, this);
  }

  @Override
  public void onStorageConfigurationUpdate(String database,
      StorageConfiguration update) {
    var request = new PushStorageConfigurationRequest(update);
    storageConfigurations.send(null, database, request, this);
  }

  public void genericNotify(
      DatabaseSessionInternal session,
      Map<String, Set<WeakReference<NetworkProtocolBinary>>> context,
      String database,
      PushEventType pack) {
    try {
      DatabaseSessionInternal sessionCopy;
      if (session != null) {
        sessionCopy = session.copy();
      } else {
        sessionCopy = null;
      }

      executor.submit(
          () -> {
            Set<WeakReference<NetworkProtocolBinary>> clients = null;
            synchronized (PushManager.this) {
              var cl = context.get(database);
              if (cl != null) {
                clients = new HashSet<>(cl);
              }
            }
            if (clients != null) {
              for (var ref : clients) {
                var protocolBinary = ref.get();
                if (protocolBinary != null) {
                  try {
                    var request = pack.getRequest(database);
                    if (request != null) {
                      if (sessionCopy != null) {
                        sessionCopy.activateOnCurrentThread();
                        protocolBinary.push(sessionCopy, request);
                      } else {
                        protocolBinary.push(null, request);
                      }
                    }
                  } catch (IOException e) {
                    synchronized (PushManager.this) {
                      context.get(database).remove(ref);
                    }
                  }
                } else {
                  synchronized (PushManager.this) {
                    context.get(database).remove(ref);
                  }
                }
              }
            }
          });
    } catch (RejectedExecutionException e) {
      LogManager.instance()
          .info(this, "Cannot send push request to client for database '%s'", database);
    }
  }
}
