package com.orientechnologies.orient.server;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.thread.OThreadPoolExecutors;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.client.remote.message.OBinaryPushResponse;
import com.orientechnologies.orient.client.remote.message.OPushDistributedConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OPushFunctionsRequest;
import com.orientechnologies.orient.client.remote.message.OPushIndexManagerRequest;
import com.orientechnologies.orient.client.remote.message.OPushSchemaRequest;
import com.orientechnologies.orient.client.remote.message.OPushSequencesRequest;
import com.orientechnologies.orient.client.remote.message.OPushStorageConfigurationRequest;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OIndexManagerShared;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

public class OPushManager implements OMetadataUpdateListener {

  protected final Set<WeakReference<ONetworkProtocolBinary>> distributedConfigPush =
      new HashSet<>();
  protected final OPushEventType storageConfigurations = new OPushEventType();
  protected final OPushEventType schema = new OPushEventType();
  protected final OPushEventType indexManager = new OPushEventType();
  protected final OPushEventType functions = new OPushEventType();
  protected final OPushEventType sequences = new OPushEventType();
  private final Set<String> registerDatabase = new HashSet<>();
  private final ExecutorService executor;

  public OPushManager() {
    executor =
        OThreadPoolExecutors.newCachedThreadPool(
            "Push Requests", Thread.currentThread().getThreadGroup(), 5, 0);
  }

  public synchronized void pushDistributedConfig(ODatabaseSessionInternal database,
      List<String> hosts) {
    Iterator<WeakReference<ONetworkProtocolBinary>> iter = distributedConfigPush.iterator();
    while (iter.hasNext()) {
      WeakReference<ONetworkProtocolBinary> ref = iter.next();
      ONetworkProtocolBinary protocolBinary = ref.get();
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

  public synchronized void subscribeDistributeConfig(ONetworkProtocolBinary channel) {
    distributedConfigPush.add(new WeakReference<ONetworkProtocolBinary>(channel));
  }

  public synchronized void cleanPushSockets() {
    Iterator<WeakReference<ONetworkProtocolBinary>> iter = distributedConfigPush.iterator();
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

  private void cleanListeners(Map<String, Set<WeakReference<ONetworkProtocolBinary>>> toClean) {
    for (Set<WeakReference<ONetworkProtocolBinary>> value : toClean.values()) {
      Iterator<WeakReference<ONetworkProtocolBinary>> iter = value.iterator();
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
      OPushEventType context, ODatabaseSessionInternal database, ONetworkProtocolBinary protocol) {
    if (!registerDatabase.contains(database.getName())) {
      database.getSharedContext().registerListener(this);
      registerDatabase.add(database.getName());
    }
    context.subscribe(database.getName(), protocol);
  }

  public synchronized void subscribeStorageConfiguration(
      ODatabaseSessionInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(storageConfigurations, database, protocol);
  }

  public synchronized void subscribeSchema(
      ODatabaseSessionInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(schema, database, protocol);
  }

  public synchronized void subscribeIndexManager(
      ODatabaseSessionInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(indexManager, database, protocol);
  }

  public synchronized void subscribeFunctions(
      ODatabaseSessionInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(functions, database, protocol);
  }

  public synchronized void subscribeSequences(
      ODatabaseSessionInternal database, ONetworkProtocolBinary protocol) {
    genericSubscribe(sequences, database, protocol);
  }

  @Override
  public void onSchemaUpdate(ODatabaseSessionInternal session, String database,
      OSchemaShared schema) {
    var document = schema.toNetworkStream();
    document.setup(null);
    OPushSchemaRequest request = new OPushSchemaRequest(document);
    this.schema.send(session, database, request, this);
  }

  @Override
  public void onIndexManagerUpdate(ODatabaseSessionInternal session, String database,
      OIndexManagerAbstract indexManager) {
    var document = ((OIndexManagerShared) indexManager).toNetworkStream();
    document.setup(null);
    OPushIndexManagerRequest request = new OPushIndexManagerRequest(document);
    this.indexManager.send(session, database, request, this);
  }

  @Override
  public void onFunctionLibraryUpdate(ODatabaseSessionInternal session, String database) {
    OPushFunctionsRequest request = new OPushFunctionsRequest();
    this.functions.send(session, database, request, this);
  }

  @Override
  public void onSequenceLibraryUpdate(ODatabaseSessionInternal session, String database) {
    OPushSequencesRequest request = new OPushSequencesRequest();
    this.sequences.send(session, database, request, this);
  }

  @Override
  public void onStorageConfigurationUpdate(String database,
      OStorageConfiguration update) {
    OPushStorageConfigurationRequest request = new OPushStorageConfigurationRequest(update);
    storageConfigurations.send(null, database, request, this);
  }

  public void genericNotify(
      ODatabaseSessionInternal session,
      Map<String, Set<WeakReference<ONetworkProtocolBinary>>> context,
      String database,
      OPushEventType pack) {
    try {
      ODatabaseSessionInternal sessionCopy;
      if (session != null) {
        sessionCopy = session.copy();
      } else {
        sessionCopy = null;
      }

      executor.submit(
          () -> {
            Set<WeakReference<ONetworkProtocolBinary>> clients = null;
            synchronized (OPushManager.this) {
              Set<WeakReference<ONetworkProtocolBinary>> cl = context.get(database);
              if (cl != null) {
                clients = new HashSet<>(cl);
              }
            }
            if (clients != null) {
              for (WeakReference<ONetworkProtocolBinary> ref : clients) {
                ONetworkProtocolBinary protocolBinary = ref.get();
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
                    synchronized (OPushManager.this) {
                      context.get(database).remove(ref);
                    }
                  }
                } else {
                  synchronized (OPushManager.this) {
                    context.get(database).remove(ref);
                  }
                }
              }
            }
          });
    } catch (RejectedExecutionException e) {
      OLogManager.instance()
          .info(this, "Cannot send push request to client for database '%s'", database);
    }
  }
}
