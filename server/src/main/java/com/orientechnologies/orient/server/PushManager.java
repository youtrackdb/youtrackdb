package com.orientechnologies.orient.server;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.thread.ThreadPoolExecutors;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.MetadataUpdateListener;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerShared;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OPushFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OPushIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OPushSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OPushSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OPushStorageConfigurationRequest;
import com.orientechnologies.orient.server.network.protocol.binary.NetworkProtocolBinary;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class PushManager implements MetadataUpdateListener {

  protected final Set<WeakReference<NetworkProtocolBinary>> distributedConfigPush =
      new HashSet<>();
  protected final OPushEventType storageConfigurations = new OPushEventType();
  protected final OPushEventType schema = new OPushEventType();
  protected final OPushEventType indexManager = new OPushEventType();
  protected final OPushEventType functions = new OPushEventType();
  protected final OPushEventType sequences = new OPushEventType();
  private final Set<String> registerDatabase = new HashSet<>();
  private final ExecutorService executor;

  public PushManager() {
    executor =
        ThreadPoolExecutors.newCachedThreadPool(
            "Push Requests", Thread.currentThread().getThreadGroup(), 5, 0);
  }

  public synchronized void pushDistributedConfig(DatabaseSessionInternal database,
      List<String> hosts) {
    Iterator<WeakReference<NetworkProtocolBinary>> iter = distributedConfigPush.iterator();
    while (iter.hasNext()) {
      WeakReference<NetworkProtocolBinary> ref = iter.next();
      NetworkProtocolBinary protocolBinary = ref.get();
      if (protocolBinary != null) {
        // TODO Filter by database, push just list of active server for a specific database
        OPushDistributedConfigurationRequest request =
            new OPushDistributedConfigurationRequest(hosts);
        try {
          OBinaryPushResponse response = protocolBinary.push(database, request);
        } catch (IOException e) {
          iter.remove();
        }
      } else {
        iter.remove();
      }
    }
  }

  public synchronized void subscribeDistributeConfig(NetworkProtocolBinary channel) {
    distributedConfigPush.add(new WeakReference<NetworkProtocolBinary>(channel));
  }

  public synchronized void cleanPushSockets() {
    Iterator<WeakReference<NetworkProtocolBinary>> iter = distributedConfigPush.iterator();
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

  private void cleanListeners(Map<String, Set<WeakReference<NetworkProtocolBinary>>> toClean) {
    for (Set<WeakReference<NetworkProtocolBinary>> value : toClean.values()) {
      Iterator<WeakReference<NetworkProtocolBinary>> iter = value.iterator();
      while (iter.hasNext()) {
        if (iter.next().get() == null) {
          iter.remove();
        }
      }
    }
  }

  public void shutdown() {
    executor.shutdownNow();
  }

  private void genericSubscribe(
      OPushEventType context, DatabaseSessionInternal database, NetworkProtocolBinary protocol) {
    if (!registerDatabase.contains(database.getName())) {
      database.getSharedContext().registerListener(this);
      registerDatabase.add(database.getName());
    }
    context.subscribe(database.getName(), protocol);
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
  public void onSchemaUpdate(DatabaseSessionInternal session, String database,
      SchemaShared schema) {
    var document = schema.toNetworkStream();
    document.setup(null);
    OPushSchemaRequest request = new OPushSchemaRequest(document);
    this.schema.send(session, database, request, this);
  }

  @Override
  public void onIndexManagerUpdate(DatabaseSessionInternal session, String database,
      IndexManagerAbstract indexManager) {
    var document = ((IndexManagerShared) indexManager).toNetworkStream(session);
    document.setup(null);
    OPushIndexManagerRequest request = new OPushIndexManagerRequest(document);
    this.indexManager.send(session, database, request, this);
  }

  @Override
  public void onFunctionLibraryUpdate(DatabaseSessionInternal session, String database) {
    OPushFunctionsRequest request = new OPushFunctionsRequest();
    this.functions.send(session, database, request, this);
  }

  @Override
  public void onSequenceLibraryUpdate(DatabaseSessionInternal session, String database) {
    OPushSequencesRequest request = new OPushSequencesRequest();
    this.sequences.send(session, database, request, this);
  }

  @Override
  public void onStorageConfigurationUpdate(String database,
      StorageConfiguration update) {
    OPushStorageConfigurationRequest request = new OPushStorageConfigurationRequest(update);
    storageConfigurations.send(null, database, request, this);
  }

  public void genericNotify(
      DatabaseSessionInternal session,
      Map<String, Set<WeakReference<NetworkProtocolBinary>>> context,
      String database,
      OPushEventType pack) {
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
              Set<WeakReference<NetworkProtocolBinary>> cl = context.get(database);
              if (cl != null) {
                clients = new HashSet<>(cl);
              }
            }
            if (clients != null) {
              for (WeakReference<NetworkProtocolBinary> ref : clients) {
                NetworkProtocolBinary protocolBinary = ref.get();
                if (protocolBinary != null) {
                  try {
                    OBinaryPushRequest<?> request = pack.getRequest(database);
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
