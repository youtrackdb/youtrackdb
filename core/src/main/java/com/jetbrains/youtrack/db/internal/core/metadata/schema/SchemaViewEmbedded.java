package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.common.util.ArrayUtils;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Role;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SchemaViewEmbedded extends SchemaViewImpl {

  protected SchemaViewEmbedded(SchemaShared iOwner, String iName, ViewConfig cfg,
      int[] iClusterIds) {
    super(iOwner, iName, cfg, iClusterIds);
  }

  protected SchemaViewEmbedded(SchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  public Property addProperty(
      DatabaseSessionInternal session, final String propertyName,
      final PropertyType type,
      final PropertyType linkedType,
      final SchemaClass linkedClass,
      final boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  public SchemaClassImpl setEncryption(DatabaseSessionInternal session, final String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass setClusterSelection(DatabaseSession session, final String value) {
    throw new UnsupportedOperationException();
  }

  public SchemaClassImpl setCustom(DatabaseSession session, final String name, final String value) {
    throw new UnsupportedOperationException();
  }

  public void clearCustom(DatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass setSuperClasses(DatabaseSession session,
      final List<? extends SchemaClass> classes) {
    throw new UnsupportedOperationException();
  }

  public SchemaClass removeBaseClassInternal(DatabaseSessionInternal session,
      final SchemaClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (subclasses == null) {
        return this;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (SchemaClassImpl) baseClass);
      }

      return this;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public SchemaClass addSuperClass(DatabaseSession session, final SchemaClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass removeSuperClass(DatabaseSession session, SchemaClass superClass) {
    throw new UnsupportedOperationException();
  }

  protected void setSuperClassesInternal(DatabaseSessionInternal session,
      final List<? extends SchemaClass> classes) {
    List<SchemaClassImpl> newSuperClasses = new ArrayList<>();
    SchemaClassImpl cls;
    for (SchemaClass superClass : classes) {
      if (superClass instanceof SchemaClassAbstractDelegate) {
        cls = (SchemaClassImpl) ((SchemaClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (SchemaClassImpl) superClass;
      }

      if (newSuperClasses.contains(cls)) {
        throw new SchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<SchemaClassImpl> toAddList = new ArrayList<>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<SchemaClassImpl> toRemoveList = new ArrayList<>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (SchemaClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (SchemaClassImpl addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }

  public SchemaClass setName(DatabaseSession session, final String name) {
    throw new UnsupportedOperationException();
  }

  public void setDefaultClusterId(DatabaseSession session, final int defaultClusterId) {
    var sessionInternal = (DatabaseSessionInternal) session;
    acquireSchemaWriteLock(sessionInternal);
    try {
      checkEmbedded();
      this.defaultClusterId = defaultClusterId;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  protected void addClusterIdInternal(DatabaseSessionInternal database, final int clusterId) {
    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      owner.checkClusterCanBeAdded(clusterId, this);

      for (int currId : clusterIds) {
        if (currId == clusterId)
        // ALREADY ADDED
        {
          return;
        }
      }

      clusterIds = ArrayUtils.copyOf(clusterIds, clusterIds.length + 1);
      clusterIds[clusterIds.length - 1] = clusterId;
      Arrays.sort(clusterIds);

      onlyAddPolymorphicClusterId(clusterId);

      if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID) {
        defaultClusterId = clusterId;
      }

      ((SchemaEmbedded) owner).addClusterForView(database, clusterId, this);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public SchemaClass setShortName(DatabaseSession session, String shortName) {
    throw new UnsupportedOperationException();
  }

  protected PropertyImpl createPropertyInstance() {
    return new PropertyEmbedded(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public SchemaClass truncateCluster(DatabaseSession session, String clusterName) {
    throw new UnsupportedOperationException();
  }

  public SchemaClass setStrictMode(DatabaseSession session, final boolean isStrict) {
    throw new UnsupportedOperationException();
  }

  public SchemaClass setDescription(DatabaseSession session, String iDescription) {
    throw new UnsupportedOperationException();
  }

  public SchemaClass addClusterId(DatabaseSession session, final int clusterId) {
    var internalSession = (DatabaseSessionInternal) session;
    internalSession.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new SchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(internalSession);
    try {
      addClusterIdInternal(internalSession, clusterId);

    } finally {
      releaseSchemaWriteLock(internalSession);
    }
    return this;
  }

  public SchemaClass removeClusterId(DatabaseSession session, final int clusterId) {
    var sessionInternal = (DatabaseSessionInternal) session;
    sessionInternal.checkSecurity(Rule.ResourceGeneric.SCHEMA, Role.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new DatabaseException(
          " Impossible to remove the last cluster of class '"
              + getName()
              + "' drop the class instead");
    }

    acquireSchemaWriteLock(sessionInternal);
    try {
      removeClusterIdInternal(sessionInternal, clusterId);
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }

    return this;
  }

  private void removeClusterIdInternal(
      DatabaseSessionInternal database, final int clusterToRemove) {

    acquireSchemaWriteLock(database);
    try {
      checkEmbedded();

      boolean found = false;
      for (int clusterId : clusterIds) {
        if (clusterId == clusterToRemove) {
          found = true;
          break;
        }
      }

      if (found) {
        final int[] newClusterIds = new int[clusterIds.length - 1];
        for (int i = 0, k = 0; i < clusterIds.length; ++i) {
          if (clusterIds[i] == clusterToRemove)
          // JUMP IT
          {
            continue;
          }

          newClusterIds[k] = clusterIds[i];
          k++;
        }
        clusterIds = newClusterIds;

        onlyRemovePolymorphicClusterId(clusterToRemove);
      }

      if (defaultClusterId == clusterToRemove) {
        if (clusterIds.length >= 1) {
          defaultClusterId = clusterIds[0];
        } else {
          defaultClusterId = NOT_EXISTENT_CLUSTER_ID;
        }
      }

      ((SchemaEmbedded) owner).removeClusterForView(database, clusterToRemove);
    } finally {
      releaseSchemaWriteLock(database);
    }

  }

  public void dropProperty(DatabaseSession session, final String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaClass addCluster(DatabaseSession session, final String clusterNameOrId) {
    throw new UnsupportedOperationException();
  }

  public SchemaClass setOverSize(DatabaseSession session, final float overSize) {
    throw new UnsupportedOperationException();
  }

  public SchemaClass setAbstract(DatabaseSession session, boolean isAbstract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ViewRemovedMetadata replaceViewClusterAndIndex(
      DatabaseSessionInternal session, final int cluster, List<Index> indexes,
      long lastRefreshTime) {
    acquireSchemaWriteLock(session);
    try {
      this.lastRefreshTime = lastRefreshTime;
      List<String> oldIndexes = inactivateIndexes(session);
      int[] oldClusters = getClusterIds();
      addClusterId(session, cluster);
      for (int i : oldClusters) {
        removeClusterId(session, i);
      }
      addActiveIndexes(session, indexes.stream().map(Index::getName).collect(Collectors.toList()));
      return new ViewRemovedMetadata(oldClusters, oldIndexes);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  protected void addClusterIdToIndexes(DatabaseSessionInternal session, int iId) {
    final String clusterName = session.getClusterNameById(iId);
    final List<String> indexesToAdd = new ArrayList<>();

    for (Index index : getIndexes(session)) {
      indexesToAdd.add(index.getName());
    }

    final IndexManagerAbstract indexManager =
        session.getMetadata().getIndexManagerInternal();
    for (String indexName : indexesToAdd) {
      indexManager.addClusterToIndex(session, clusterName, indexName);
    }
  }
}
