package com.orientechnologies.orient.client.remote.metadata.schema;

import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.YTDatabaseListener;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.exception.YTSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OSchemaShared;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTClassImpl;
import com.orientechnologies.orient.core.metadata.schema.YTView;
import com.orientechnologies.orient.core.metadata.schema.YTViewImpl;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class OSchemaRemote extends OSchemaShared {

  private final AtomicBoolean skipPush = new AtomicBoolean(false);

  public OSchemaRemote() {
    super();
  }

  @Override
  public YTClass getOrCreateClass(
      YTDatabaseSessionInternal database, String iClassName, YTClass... superClasses) {
    if (iClassName == null) {
      return null;
    }

    acquireSchemaReadLock();
    try {
      YTClass cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
      if (cls != null) {
        return cls;
      }
    } finally {
      releaseSchemaReadLock();
    }

    YTClass cls;

    int[] clusterIds = null;

    acquireSchemaWriteLock(database);
    try {
      cls = classes.get(iClassName.toLowerCase(Locale.ENGLISH));
      if (cls != null) {
        return cls;
      }

      cls = createClass(database, iClassName, clusterIds, superClasses);

      addClusterClassMap(cls);
    } finally {
      releaseSchemaWriteLock(database);
    }

    return cls;
  }

  protected YTClassImpl createClassInstance(String name) {
    return new YTClassRemote(this, name);
  }

  protected YTViewImpl createViewInstance(String name) {
    return new YTViewRemote(this, name);
  }

  public YTClass createClass(
      YTDatabaseSessionInternal database,
      final String className,
      int[] clusterIds,
      YTClass... superClasses) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + className
              + "'");
    }
    YTClass result;

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    if (superClasses != null) {
      YTClassImpl.checkParametersConflict(Arrays.asList(superClasses));
    }

    acquireSchemaWriteLock(database);
    try {

      final String key = className.toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key)) {
        throw new YTSchemaException("Class '" + className + "' already exists in current database");
      }

      checkClustersAreAbsent(clusterIds);

      StringBuilder cmd = new StringBuilder("create class ");
      cmd.append('`');
      cmd.append(className);
      cmd.append('`');

      List<YTClass> superClassesList = new ArrayList<YTClass>();
      if (superClasses != null && superClasses.length > 0) {
        boolean first = true;
        for (YTClass superClass : superClasses) {
          // Filtering for null
          if (superClass != null) {
            if (first) {
              cmd.append(" extends ");
            } else {
              cmd.append(", ");
            }
            cmd.append('`').append(superClass.getName()).append('`');
            first = false;
            superClassesList.add(superClass);
          }
        }
      }

      if (clusterIds != null) {
        if (clusterIds.length == 1 && clusterIds[0] == -1) {
          cmd.append(" abstract");
        } else {
          cmd.append(" cluster ");
          for (int i = 0; i < clusterIds.length; ++i) {
            if (i > 0) {
              cmd.append(',');
            } else {
              cmd.append(' ');
            }

            cmd.append(clusterIds[i]);
          }
        }
      }

      database.command(cmd.toString()).close();
      reload(database);

      result = classes.get(className.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onCreateClass(database, result);
      }

      for (Iterator<YTDatabaseListener> it = database.getListeners().iterator(); it.hasNext(); ) {
        it.next().onCreateClass(database, result);
      }

    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  public YTClass createClass(
      YTDatabaseSessionInternal database,
      final String className,
      int clusters,
      YTClass... superClasses) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(className);
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in class name '"
              + className
              + "'");
    }

    YTClass result;

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    if (superClasses != null) {
      YTClassImpl.checkParametersConflict(Arrays.asList(superClasses));
    }
    acquireSchemaWriteLock(database);
    try {

      final String key = className.toLowerCase(Locale.ENGLISH);
      if (classes.containsKey(key)) {
        throw new YTSchemaException("Class '" + className + "' already exists in current database");
      }

      StringBuilder cmd = new StringBuilder("create class ");
      cmd.append('`');
      cmd.append(className);
      cmd.append('`');

      List<YTClass> superClassesList = new ArrayList<YTClass>();
      if (superClasses != null && superClasses.length > 0) {
        boolean first = true;
        for (YTClass superClass : superClasses) {
          // Filtering for null
          if (superClass != null) {
            if (first) {
              cmd.append(" extends ");
            } else {
              cmd.append(", ");
            }
            cmd.append(superClass.getName());
            first = false;
            superClassesList.add(superClass);
          }
        }
      }

      if (clusters == 0) {
        cmd.append(" abstract");
      } else {
        cmd.append(" clusters ");
        cmd.append(clusters);
      }

      database.command(cmd.toString()).close();
      reload(database);
      result = classes.get(className.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onCreateClass(database, result);
      }

      for (Iterator<YTDatabaseListener> it = database.getListeners().iterator(); it.hasNext(); ) {
        it.next().onCreateClass(database, result);
      }

    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  public YTView createView(
      YTDatabaseSessionInternal database, OViewConfig cfg, ViewCreationListener listener)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTView createView(YTDatabaseSessionInternal database, OViewConfig cfg) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(cfg.getName());
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid view name found. Character '"
              + wrongCharacter
              + "' cannot be used in view name '"
              + cfg.getName()
              + "'");
    }

    YTView result;

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    acquireSchemaWriteLock(database);
    try {

      final String key = cfg.getName().toLowerCase(Locale.ENGLISH);
      if (views.containsKey(key)) {
        throw new YTSchemaException(
            "View '" + cfg.getName() + "' already exists in current database");
      }

      StringBuilder cmd = new StringBuilder("create view ");
      cmd.append('`');
      cmd.append(cfg.getName());
      cmd.append('`');
      cmd.append(" FROM (" + cfg.getQuery() + ") ");
      if (cfg.isUpdatable()) {
        cmd.append(" UPDATABLE");
      }
      // TODO METADATA!!!

      database.command(cmd.toString()).close();
      reload(database);
      result = views.get(cfg.getName().toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onCreateView(database, result);
      }

      for (Iterator<YTDatabaseListener> it = database.getListeners().iterator(); it.hasNext(); ) {
        it.next().onCreateView(database, result);
      }

    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  @Override
  public YTView createView(
      YTDatabaseSessionInternal database,
      String name,
      String statement,
      Map<String, Object> metadata) {
    final Character wrongCharacter = OSchemaShared.checkClassNameIfValid(name);
    if (wrongCharacter != null) {
      throw new YTSchemaException(
          "Invalid class name found. Character '"
              + wrongCharacter
              + "' cannot be used in view name '"
              + name
              + "'");
    }

    YTView result;

    database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_CREATE);
    acquireSchemaWriteLock(database);
    try {

      final String key = name.toLowerCase(Locale.ENGLISH);
      if (views.containsKey(key)) {
        throw new YTSchemaException("View '" + name + "' already exists in current database");
      }

      String cmd = "create view " + '`' + name + '`' + " FROM (" + statement + ") ";
      //      if (metadata!=null) {//TODO
      //        cmd.append(" METADATA");
      //      }

      database.command(cmd).close();
      reload(database);
      result = views.get(name.toLowerCase(Locale.ENGLISH));

      // WAKE UP DB LIFECYCLE LISTENER
      for (Iterator<ODatabaseLifecycleListener> it = YouTrackDBManager.instance()
          .getDbLifecycleListeners();
          it.hasNext(); ) {
        it.next().onCreateView(database, result);
      }

      for (Iterator<YTDatabaseListener> it = database.getListeners().iterator(); it.hasNext(); ) {
        it.next().onCreateView(database, result);
      }

    } finally {
      releaseSchemaWriteLock(database);
    }

    return result;
  }

  private void checkClustersAreAbsent(final int[] iClusterIds) {
    if (iClusterIds == null) {
      return;
    }

    for (int clusterId : iClusterIds) {
      if (clusterId < 0) {
        continue;
      }

      if (clustersToClasses.containsKey(clusterId)) {
        throw new YTSchemaException(
            "Cluster with id "
                + clusterId
                + " already belongs to class "
                + clustersToClasses.get(clusterId));
      }
    }
  }

  public void dropClass(YTDatabaseSessionInternal database, final String className) {

    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (className == null) {
        throw new IllegalArgumentException("Class name is null");
      }

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = className.toLowerCase(Locale.ENGLISH);

      YTClass cls = classes.get(key);

      if (cls == null) {
        throw new YTSchemaException("Class '" + className + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new YTSchemaException(
            "Class '"
                + className
                + "' cannot be dropped because it has sub classes "
                + cls.getSubclasses()
                + ". Remove the dependencies before trying to drop it again");
      }

      String cmd = "drop class `" + className + "` unsafe";
      database.command(cmd).close();
      reload(database);

      // FREE THE RECORD CACHE
      database.getLocalCache().freeCluster(cls.getDefaultClusterId());

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  public void dropView(YTDatabaseSessionInternal database, final String name) {

    acquireSchemaWriteLock(database);
    try {
      if (database.getTransaction().isActive()) {
        throw new IllegalStateException("Cannot drop a class inside a transaction");
      }

      if (name == null) {
        throw new IllegalArgumentException("View name is null");
      }

      database.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

      final String key = name.toLowerCase(Locale.ENGLISH);

      YTClass cls = views.get(key);

      if (cls == null) {
        throw new YTSchemaException("View '" + name + "' was not found in current database");
      }

      if (!cls.getSubclasses().isEmpty()) {
        throw new YTSchemaException(
            "View '"
                + name
                + "' cannot be dropped because it has sub classes "
                + cls.getSubclasses()
                + ". Remove the dependencies before trying to drop it again");
      }

      String cmd = "drop view " + name + " unsafe";
      database.command(cmd).close();
      reload(database);

      // FREE THE RECORD CACHE
      database.getLocalCache().freeCluster(cls.getDefaultClusterId());

    } finally {
      releaseSchemaWriteLock(database);
    }
  }

  @Override
  public void acquireSchemaWriteLock(YTDatabaseSessionInternal database) {
    skipPush.set(true);
  }

  @Override
  public void releaseSchemaWriteLock(YTDatabaseSessionInternal database, final boolean iSave) {
    skipPush.set(false);
  }

  @Override
  public void checkEmbedded() {
    throw new YTSchemaException(
        "'Internal' schema modification methods can be used only inside of embedded database");
  }

  public void update(YTDatabaseSessionInternal session, YTDocument schema) {
    if (!skipPush.get()) {
      fromStream(session, schema);
      this.snapshot = null;
    }
  }

  @Override
  public int addBlobCluster(YTDatabaseSessionInternal database, int clusterId) {
    throw new YTSchemaException(
        "Not supported operation use instead YTDatabaseSession.addBlobCluster");
  }

  @Override
  public void removeBlobCluster(YTDatabaseSessionInternal database, String clusterName) {
    throw new YTSchemaException(
        "Not supported operation use instead YTDatabaseSession.dropCluster");
  }
}
