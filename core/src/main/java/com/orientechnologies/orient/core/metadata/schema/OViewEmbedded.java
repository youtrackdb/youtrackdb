package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OViewEmbedded extends OViewImpl {

  protected OViewEmbedded(OSchemaShared iOwner, String iName, OViewConfig cfg, int[] iClusterIds) {
    super(iOwner, iName, cfg, iClusterIds);
  }

  protected OViewEmbedded(OSchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  public OProperty addProperty(
      ODatabaseSessionInternal session, final String propertyName,
      final OType type,
      final OType linkedType,
      final OClass linkedClass,
      final boolean unsafe) {
    throw new UnsupportedOperationException();
  }

  public OClassImpl setEncryption(ODatabaseSessionInternal session, final String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass setClusterSelection(ODatabaseSession session, final String value) {
    throw new UnsupportedOperationException();
  }

  public OClassImpl setCustom(ODatabaseSession session, final String name, final String value) {
    throw new UnsupportedOperationException();
  }

  public void clearCustom(ODatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass setSuperClasses(ODatabaseSession session, final List<? extends OClass> classes) {
    throw new UnsupportedOperationException();
  }

  public OClass removeBaseClassInternal(ODatabaseSessionInternal session, final OClass baseClass) {
    acquireSchemaWriteLock(session);
    try {
      checkEmbedded();

      if (subclasses == null) {
        return this;
      }

      if (subclasses.remove(baseClass)) {
        removePolymorphicClusterIds(session, (OClassImpl) baseClass);
      }

      return this;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  @Override
  public OClass addSuperClass(ODatabaseSession session, final OClass superClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass removeSuperClass(ODatabaseSession session, OClass superClass) {
    throw new UnsupportedOperationException();
  }

  protected void setSuperClassesInternal(ODatabaseSessionInternal session,
      final List<? extends OClass> classes) {
    List<OClassImpl> newSuperClasses = new ArrayList<>();
    OClassImpl cls;
    for (OClass superClass : classes) {
      if (superClass instanceof OClassAbstractDelegate) {
        cls = (OClassImpl) ((OClassAbstractDelegate) superClass).delegate;
      } else {
        cls = (OClassImpl) superClass;
      }

      if (newSuperClasses.contains(cls)) {
        throw new OSchemaException("Duplicated superclass '" + cls.getName() + "'");
      }

      newSuperClasses.add(cls);
    }

    List<OClassImpl> toAddList = new ArrayList<>(newSuperClasses);
    toAddList.removeAll(superClasses);
    List<OClassImpl> toRemoveList = new ArrayList<>(superClasses);
    toRemoveList.removeAll(newSuperClasses);

    for (OClassImpl toRemove : toRemoveList) {
      toRemove.removeBaseClassInternal(session, this);
    }
    for (OClassImpl addTo : toAddList) {
      addTo.addBaseClass(session, this);
    }
    superClasses.clear();
    superClasses.addAll(newSuperClasses);
  }

  public OClass setName(ODatabaseSession session, final String name) {
    throw new UnsupportedOperationException();
  }

  public void setDefaultClusterId(ODatabaseSession session, final int defaultClusterId) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    acquireSchemaWriteLock(sessionInternal);
    try {
      checkEmbedded();
      this.defaultClusterId = defaultClusterId;
    } finally {
      releaseSchemaWriteLock(sessionInternal);
    }
  }

  protected void addClusterIdInternal(ODatabaseSessionInternal database, final int clusterId) {
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

  public OClass setShortName(ODatabaseSession session, String shortName) {
    throw new UnsupportedOperationException();
  }

  protected OPropertyImpl createPropertyInstance() {
    return new OPropertyEmbedded(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public OClass truncateCluster(ODatabaseSession session, String clusterName) {
    throw new UnsupportedOperationException();
  }

  public OClass setStrictMode(ODatabaseSession session, final boolean isStrict) {
    throw new UnsupportedOperationException();
  }

  public OClass setDescription(ODatabaseSession session, String iDescription) {
    throw new UnsupportedOperationException();
  }

  public OClass addClusterId(ODatabaseSession session, final int clusterId) {
    var internalSession = (ODatabaseSessionInternal) session;
    internalSession.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (isAbstract()) {
      throw new OSchemaException("Impossible to associate a cluster to an abstract class class");
    }

    acquireSchemaWriteLock(internalSession);
    try {
      addClusterIdInternal(internalSession, clusterId);

    } finally {
      releaseSchemaWriteLock(internalSession);
    }
    return this;
  }

  public OClass removeClusterId(ODatabaseSession session, final int clusterId) {
    var sessionInternal = (ODatabaseSessionInternal) session;
    sessionInternal.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_UPDATE);

    if (clusterIds.length == 1 && clusterId == clusterIds[0]) {
      throw new ODatabaseException(
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
      ODatabaseSessionInternal database, final int clusterToRemove) {

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

  public void dropProperty(ODatabaseSession session, final String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OClass addCluster(ODatabaseSession session, final String clusterNameOrId) {
    throw new UnsupportedOperationException();
  }

  public OClass setOverSize(ODatabaseSession session, final float overSize) {
    throw new UnsupportedOperationException();
  }

  public OClass setAbstract(ODatabaseSession session, boolean isAbstract) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OViewRemovedMetadata replaceViewClusterAndIndex(
      ODatabaseSessionInternal session, final int cluster, List<OIndex> indexes,
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

  protected void addClusterIdToIndexes(ODatabaseSessionInternal session, int iId) {
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
