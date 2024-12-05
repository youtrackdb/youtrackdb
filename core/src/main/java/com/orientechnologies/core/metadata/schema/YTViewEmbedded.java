package com.orientechnologies.core.metadata.schema;

import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTSchemaException;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.index.OIndexManagerAbstract;
import com.orientechnologies.core.metadata.security.ORole;
import com.orientechnologies.core.metadata.security.ORule;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class YTViewEmbedded extends YTViewImpl {

  protected YTViewEmbedded(OSchemaShared iOwner, String iName, OViewConfig cfg, int[] iClusterIds) {
    super(iOwner, iName, cfg, iClusterIds);
  }

  protected YTViewEmbedded(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  public YTProperty addProperty(
      YTDatabaseSessionInternal session, final String propertyName,
      final YTType type,
      final YTType linkedType,
      final YTClass linkedClass,
      final boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  public YTClassImpl setEncryption(YTDatabaseSessionInternal session, final String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass setClusterSelection(YTDatabaseSession session, final String value) {
    throw new UnsupportedOperationException();
  }

  public YTClassImpl setCustom(YTDatabaseSession session, final String name, final String value) {
    throw new UnsupportedOperationException();
  }

  public void clearCustom(YTDatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass setSuperClasses(YTDatabaseSession session, final List<? extends YTClass> classes) {
    throw new UnsupportedOperationException();
  }

  public YTClass removeBaseClassInternal(YTDatabaseSessionInternal session,
      final YTClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (subclasses == null) {
        return this;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (YTClassImpl) baseClass);
      }

      return this;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public YTClass addSuperClass(YTDatabaseSession session, final YTClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass removeSuperClass(YTDatabaseSession session, YTClass superClass) {
    throw new UnsupportedOperationException();
  }

  protected void setSuperClassesInternal(YTDatabaseSessionInternal session,
      final List<? extends YTClass> classes) {
    List<YTClassImpl> newSuperClasses = new ArrayList<>();
    YTClassImpl cls;
    for (YTClass superClass : classes) {
      if (superClass instanceof YTClassAbstractDelegate) {
        cls = (YTClassImpl) ((YTClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (YTClassImpl) superClass;
      }

      if (newSuperClasses.contains(cls)) {
        throw new YTSchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<YTClassImpl> toAddList = new ArrayList<>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<YTClassImpl> toRemoveList = new ArrayList<>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (YTClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (YTClassImpl addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }

  public YTClass setName(YTDatabaseSession session, final String name) {
    throw new UnsupportedOperationException();
  }

  public void setDefaultClusterId(YTDatabaseSession session, final int defaultClusterId) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    acquireSchemaWriteLock(sessionInternal);
    try {
      checkEmbedded();
      this.defaultClusterId = defaultClusterId;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  protected void addClusterIdInternal(YTDatabaseSessionInternal database, final int clusterId) {
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

      clusterIds = OArrays.copyOf(clusterIds, clusterIds.length + 1);
      clusterIds[clusterIds.length - 1] = clusterId;
      Arrays.sort(clusterIds);

      onlyAddPolymorphicClusterId(clusterId);

      if (defaultClusterId == NOT_EXISTENT_CLUSTER_ID) {
        defaultClusterId = clusterId;
      }

      ((OSchemaEmbedded) owner).addClusterForView(database, clusterId, this);
    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public YTClass setShortName(YTDatabaseSession session, String shortName) {
    throw new UnsupportedOperationException();
  }

  protected YTPropertyImpl createPropertyInstance() {
    return new YTPropertyEmbedded(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public YTClass truncateCluster(YTDatabaseSession session, String clusterName) {
    throw new UnsupportedOperationException();
  }

  public YTClass setStrictMode(YTDatabaseSession session, final boolean isStrict) {
    throw new UnsupportedOperationException();
  }

  public YTClass setDescription(YTDatabaseSession session, String iDescription) {
    throw new UnsupportedOperationException();
  }

  public YTClass addClusterId(YTDatabaseSession session, final int clusterId) {
    var internalSession = (YTDatabaseSessionInternal) session;
    internalSession.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new YTSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(internalSession);
    try {
      addClusterIdInternal(internalSession, clusterId);

    } finally {
      releaseSchemaWriteLock(internalSession);
    }
    return this;
  }

  public YTClass removeClusterId(YTDatabaseSession session, final int clusterId) {
    var sessionInternal = (YTDatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new YTDatabaseException(
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
      YTDatabaseSessionInternal database, final int clusterToRemove) {

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

      ((OSchemaEmbedded) owner).removeClusterForView(database, clusterToRemove);
    } finally {
      releaseSchemaWriteLock(database);
    }

  }

  public void dropProperty(YTDatabaseSession session, final String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTClass addCluster(YTDatabaseSession session, final String clusterNameOrId) {
    throw new UnsupportedOperationException();
  }

  public YTClass setOverSize(YTDatabaseSession session, final float overSize) {
    throw new UnsupportedOperationException();
  }

  public YTClass setAbstract(YTDatabaseSession session, boolean isAbstract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OViewRemovedMetadata replaceViewClusterAndIndex(
      YTDatabaseSessionInternal session, final int cluster, List<OIndex> indexes,
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
      addActiveIndexes(session, indexes.stream().map(OIndex::getName).collect(Collectors.toList()));
      return new OViewRemovedMetadata(oldClusters, oldIndexes);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  protected void addClusterIdToIndexes(YTDatabaseSessionInternal session, int iId) {
    final String clusterName = session.getClusterNameById(iId);
    final List<String> indexesToAdd = new ArrayList<>();

    for (OIndex index : getIndexes(session)) {
      indexesToAdd.add(index.getName());
    }

    final OIndexManagerAbstract indexManager =
        session.getMetadata().getIndexManagerInternal();
    for (String indexName : indexesToAdd) {
      indexManager.addClusterToIndex(session, clusterName, indexName);
    }
  }
}
