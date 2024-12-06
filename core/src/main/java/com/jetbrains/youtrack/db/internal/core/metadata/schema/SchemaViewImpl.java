package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.core.collate.Collate;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.PropertyMapIndexDefinition.INDEX_BY;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class SchemaViewImpl extends SchemaClassImpl implements SchemaView {

  private ViewConfig cfg;
  private Set<String> activeIndexNames = new HashSet<>();
  protected long lastRefreshTime = 0;

  protected SchemaViewImpl(SchemaShared iOwner, String iName, ViewConfig cfg, int[] iClusterIds) {
    super(iOwner, iName, iClusterIds);
    this.cfg = cfg.copy();
  }

  protected SchemaViewImpl(SchemaShared iOwner, String iName) {
    super(iOwner, iName);
  }

  @Override
  public void fromStream(EntityImpl entity) {
    super.fromStream(entity);
    String query = entity.getProperty("query");
    this.cfg = new ViewConfig(getName(), query);
    this.cfg.setUpdatable(Boolean.TRUE.equals(entity.getProperty("updatable")));

    if (entity.getProperty("indexes") instanceof List) {
      List<Map<String, Object>> idxData = entity.getProperty("indexes");
      for (Map<String, Object> idx : idxData) {
        String type = (String) idx.get("type");
        String engine = (String) idx.get("engine");
        ViewIndexConfig indexConfig = this.cfg.addIndex(type, engine);
        if (idx.get("properties") instanceof Map) {
          Map<String, Object> props = (Map<String, Object>) idx.get("properties");
          for (Map.Entry<String, Object> prop : props.entrySet()) {
            PropertyType proType = null;
            PropertyType linkedType = null;
            String collateName = null;
            INDEX_BY indexBy = null;
            if (prop.getValue() instanceof Map) {
              Map<String, Object> value = (Map<String, Object>) prop.getValue();
              if (value.size() > 0 && value.get("type") != null) {
                proType = PropertyType.valueOf(value.get("type").toString());
              }
              if (value.size() > 1 && value.get("linkedType") != null) {
                linkedType = PropertyType.valueOf(value.get("linkedType").toString());
              }
              if (value.size() > 1 && value.get("collate") != null) {
                collateName = value.get("collate").toString();
              }
              if (value.size() > 1 && value.get("collate") != null) {
                indexBy = INDEX_BY.valueOf(value.get("collate").toString().toUpperCase());
              }
            } else {
              if (prop.getValue() != null) {
                proType = PropertyType.valueOf(prop.getValue().toString());
              }
            }
            Collate collate = SQLEngine.getCollate(collateName);
            indexConfig.addProperty(prop.getKey(), proType, linkedType, collate, indexBy);
          }
        }
      }
    }
    if (entity.getProperty("updateIntervalSeconds") instanceof Integer) {
      cfg.setUpdateIntervalSeconds(entity.getProperty("updateIntervalSeconds"));
    }
    if (entity.getProperty("updateStrategy") instanceof String) {
      cfg.setUpdateStrategy(entity.getProperty("updateStrategy"));
    }
    if (entity.getProperty("watchClasses") instanceof List) {
      cfg.setWatchClasses(entity.getProperty("watchClasses"));
    }
    if (entity.getProperty("originRidField") instanceof String) {
      cfg.setOriginRidField(entity.getProperty("originRidField"));
    }
    if (entity.getProperty("nodes") instanceof List) {
      cfg.setNodes(entity.getProperty("nodes"));
    }
    if (entity.getProperty("activeIndexNames") instanceof Set) {
      activeIndexNames = entity.getProperty("activeIndexNames");
    }
    if (entity.getProperty("lastRefreshTime") != null) {
      lastRefreshTime = entity.getProperty("lastRefreshTime");
    }
  }

  @Override
  public EntityImpl toStream() {
    EntityImpl result = super.toStream();
    result.setProperty("query", cfg.getQuery());
    result.setProperty("updatable", cfg.isUpdatable());

    List<Map<String, Object>> indexes = new ArrayList<>();
    for (ViewIndexConfig idx : cfg.indexes) {
      Map<String, Object> indexDescriptor = new HashMap<>();
      indexDescriptor.put("type", idx.type);
      indexDescriptor.put("engine", idx.engine);
      Map<String, Object> properties = new HashMap<>();
      for (IndexConfigProperty s : idx.props) {
        HashMap<String, Object> entry = new HashMap<>();
        entry.put("type", s.getType().toString());
        if (s.getLinkedType() != null) {
          entry.put("linkedType", s.getLinkedType().toString());
        }
        if (s.getIndexBy() != null) {
          entry.put("indexBy", s.getIndexBy().toString());
        }
        if (s.getCollate() != null) {
          entry.put("collate", s.getCollate().getName());
        }
        properties.put(s.getName(), entry);
      }
      indexDescriptor.put("properties", properties);
      indexes.add(indexDescriptor);
    }
    result.setProperty("indexes", indexes);
    result.setProperty("updateIntervalSeconds", cfg.getUpdateIntervalSeconds());
    result.setProperty("updateStrategy", cfg.getUpdateStrategy());
    result.setProperty("watchClasses", cfg.getWatchClasses());
    result.setProperty("originRidField", cfg.getOriginRidField());
    result.setProperty("nodes", cfg.getNodes());
    result.setProperty("activeIndexNames", activeIndexNames);
    result.setProperty("lastRefreshTime", lastRefreshTime);
    return result;
  }

  @Override
  public EntityImpl toNetworkStream() {
    EntityImpl result = super.toNetworkStream();
    result.setProperty("query", cfg.getQuery());
    result.setProperty("updatable", cfg.isUpdatable());
    List<Map<String, Object>> indexes = new ArrayList<>();
    for (ViewIndexConfig idx : cfg.indexes) {
      Map<String, Object> indexDescriptor = new HashMap<>();
      indexDescriptor.put("type", idx.type);
      indexDescriptor.put("engine", idx.engine);
      Map<String, Object> properties = new HashMap<>();
      for (IndexConfigProperty s : idx.props) {
        HashMap<String, Object> entry = new HashMap<>();
        entry.put("type", s.getType().toString());
        if (s.getLinkedType() != null) {
          entry.put("linkedType", s.getLinkedType().toString());
        }
        if (s.getIndexBy() != null) {
          entry.put("indexBy", s.getIndexBy().toString());
        }
        if (s.getCollate() != null) {
          entry.put("collate", s.getCollate().getName());
        }
        properties.put(s.getName(), entry);
        indexDescriptor.put("properties", properties);
      }
      indexes.add(indexDescriptor);
    }
    result.setProperty("indexes", indexes);
    result.setProperty("updateIntervalSeconds", cfg.getUpdateIntervalSeconds());
    result.setProperty("updateStrategy", cfg.getUpdateStrategy());
    result.setProperty("watchClasses", cfg.getWatchClasses());
    result.setProperty("originRidField", cfg.getOriginRidField());
    result.setProperty("nodes", cfg.getNodes());
    result.setProperty("activeIndexNames", activeIndexNames);
    result.setProperty("lastRefreshTime", lastRefreshTime);
    return result;
  }

  @Override
  public String getQuery() {
    return cfg.getQuery();
  }

  public long count(DatabaseSession session, final boolean isPolymorphic) {
    acquireSchemaReadLock();
    try {
      return ((DatabaseSessionInternal) session).countView(getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public int getUpdateIntervalSeconds() {
    return cfg.updateIntervalSeconds;
  }

  public List<String> getWatchClasses() {
    return cfg.getWatchClasses();
  }

  @Override
  public String getOriginRidField() {
    return cfg.getOriginRidField();
  }

  @Override
  public boolean isUpdatable() {
    return cfg.isUpdatable();
  }

  @Override
  public String getUpdateStrategy() {
    return cfg.getUpdateStrategy();
  }

  @Override
  public List<String> getNodes() {
    return cfg.getNodes();
  }

  @Override
  public List<ViewIndexConfig> getRequiredIndexesInfo() {
    return cfg.getIndexes();
  }

  public Set<Index> getClassIndexes(DatabaseSession session) {
    if (activeIndexNames == null || activeIndexNames.isEmpty()) {
      return new HashSet<>();
    }
    acquireSchemaReadLock();
    try {

      final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
      final IndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
      if (idxManager == null) {
        return new HashSet<>();
      }

      return activeIndexNames.stream()
          .map(name -> idxManager.getIndex(database, name))
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public void getClassIndexes(DatabaseSession session, final Collection<Index> indexes) {
    acquireSchemaReadLock();
    try {
      final DatabaseSessionInternal database = (DatabaseSessionInternal) session;
      final IndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
      if (idxManager == null) {
        return;
      }

      activeIndexNames.stream()
          .map(name -> idxManager.getIndex(database, name))
          .filter(Objects::nonNull)
          .forEach(indexes::add);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public List<String> inactivateIndexes(DatabaseSessionInternal session) {
    acquireSchemaWriteLock(session);
    try {
      List<String> oldIndexes = new ArrayList<>(activeIndexNames);
      this.activeIndexNames.clear();
      return oldIndexes;
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public void addActiveIndexes(DatabaseSessionInternal session, List<String> names) {
    acquireSchemaWriteLock(session);
    try {
      this.activeIndexNames.addAll(names);
    } finally {
      releaseSchemaWriteLock(session);
    }
  }

  public abstract ViewRemovedMetadata replaceViewClusterAndIndex(
      DatabaseSessionInternal session, int cluster, List<Index> indexes, long lastRefreshTime);

  public Set<String> getActiveIndexNames() {
    return new HashSet<>(activeIndexNames);
  }

  public long getLastRefreshTime() {
    return lastRefreshTime;
  }
}
