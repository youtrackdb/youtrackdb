/* Generated By:JJTree: Do not edit this line. SQLCreateClassStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SQLCreateClassStatement extends DDLStatement {

  /**
   * Class name
   */
  public SQLIdentifier name;

  public boolean ifNotExists;

  /**
   * Direct superclasses for this class
   */
  protected List<SQLIdentifier> superclasses;

  /**
   * Cluster IDs for this class
   */
  protected List<SQLInteger> clusters;

  /**
   * Total number clusters for this class
   */
  protected SQLInteger totalClusterNo;

  protected boolean abstractClass = false;

  public SQLCreateClassStatement(int id) {
    super(id);
  }

  public SQLCreateClassStatement(YouTrackDBSql p, int id) {
    super(p, id);
  }

  @Override
  public ExecutionStream executeDDL(CommandContext ctx) {
    var db = ctx.getDatabase();
    Schema schema = db.getMetadata().getSchema();
    if (schema.existsClass(name.getStringValue())) {
      if (ifNotExists) {
        return ExecutionStream.empty();
      } else {
        throw new CommandExecutionException("Class " + name + " already exists");
      }
    }
    checkSuperclasses(schema, ctx);

    ResultInternal result = new ResultInternal(db);
    result.setProperty("operation", "create class");
    result.setProperty("className", name.getStringValue());

    SchemaClass clazz = null;
    SchemaClass[] superclasses = getSuperClasses(schema);
    if (abstractClass) {
      clazz = schema.createAbstractClass(name.getStringValue(), superclasses);
      result.setProperty("abstract", abstractClass);
    } else if (totalClusterNo != null) {
      clazz =
          schema.createClass(
              name.getStringValue(), totalClusterNo.getValue().intValue(), superclasses);
    } else if (clusters != null) {
      clusters.stream().map(x -> x.getValue().intValue()).toList();
      int[] clusterIds = new int[clusters.size()];
      for (int i = 0; i < clusters.size(); i++) {
        clusterIds[i] = clusters.get(i).getValue().intValue();
      }
      clazz = schema.createClass(name.getStringValue(), clusterIds, superclasses);
    } else {
      clazz = schema.createClass(name.getStringValue(), superclasses);
    }

    return ExecutionStream.singleton(result);
  }

  private SchemaClass[] getSuperClasses(Schema schema) {
    if (superclasses == null) {
      return new SchemaClass[]{};
    }
    return superclasses.stream()
        .map(x -> schema.getClass(x.getStringValue()))
        .filter(x -> x != null)
        .collect(Collectors.toList())
        .toArray(new SchemaClass[]{});
  }

  private void checkSuperclasses(Schema schema, CommandContext ctx) {
    if (superclasses != null) {
      for (SQLIdentifier superclass : superclasses) {
        if (!schema.existsClass(superclass.getStringValue())) {
          throw new CommandExecutionException("Superclass " + superclass + " not found");
        }
      }
    }
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("CREATE CLASS ");
    name.toString(params, builder);
    if (ifNotExists) {
      builder.append(" IF NOT EXISTS");
    }
    if (superclasses != null && superclasses.size() > 0) {
      builder.append(" EXTENDS ");
      boolean first = true;
      for (SQLIdentifier sup : superclasses) {
        if (!first) {
          builder.append(", ");
        }
        sup.toString(params, builder);
        first = false;
      }
    }
    if (clusters != null && clusters.size() > 0) {
      builder.append(" CLUSTER ");
      boolean first = true;
      for (SQLInteger cluster : clusters) {
        if (!first) {
          builder.append(",");
        }
        cluster.toString(params, builder);
        first = false;
      }
    }
    if (totalClusterNo != null) {
      builder.append(" CLUSTERS ");
      totalClusterNo.toString(params, builder);
    }
    if (abstractClass) {
      builder.append(" ABSTRACT");
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("CREATE CLASS ");
    name.toGenericStatement(builder);
    if (ifNotExists) {
      builder.append(" IF NOT EXISTS");
    }
    if (superclasses != null && superclasses.size() > 0) {
      builder.append(" EXTENDS ");
      boolean first = true;
      for (SQLIdentifier sup : superclasses) {
        if (!first) {
          builder.append(", ");
        }
        sup.toGenericStatement(builder);
        first = false;
      }
    }
    if (clusters != null && clusters.size() > 0) {
      builder.append(" CLUSTER ");
      boolean first = true;
      for (SQLInteger cluster : clusters) {
        if (!first) {
          builder.append(",");
        }
        cluster.toGenericStatement(builder);
        first = false;
      }
    }
    if (totalClusterNo != null) {
      builder.append(" CLUSTERS ");
      totalClusterNo.toGenericStatement(builder);
    }
    if (abstractClass) {
      builder.append(" ABSTRACT");
    }
  }

  @Override
  public SQLCreateClassStatement copy() {
    SQLCreateClassStatement result = new SQLCreateClassStatement(-1);
    result.name = name == null ? null : name.copy();
    result.superclasses =
        superclasses == null
            ? null
            : superclasses.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.clusters =
        clusters == null ? null : clusters.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.totalClusterNo = totalClusterNo == null ? null : totalClusterNo.copy();
    result.abstractClass = abstractClass;
    result.ifNotExists = ifNotExists;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SQLCreateClassStatement that = (SQLCreateClassStatement) o;

    if (abstractClass != that.abstractClass) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(superclasses, that.superclasses)) {
      return false;
    }
    if (!Objects.equals(clusters, that.clusters)) {
      return false;
    }
    if (!Objects.equals(totalClusterNo, that.totalClusterNo)) {
      return false;
    }
    return ifNotExists == that.ifNotExists;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (superclasses != null ? superclasses.hashCode() : 0);
    result = 31 * result + (clusters != null ? clusters.hashCode() : 0);
    result = 31 * result + (totalClusterNo != null ? totalClusterNo.hashCode() : 0);
    result = 31 * result + (abstractClass ? 1 : 0);
    return result;
  }

  public List<SQLIdentifier> getSuperclasses() {
    return superclasses;
  }

  public void addSuperclass(SQLIdentifier identifier) {
    if (this.superclasses == null) {
      this.superclasses = new ArrayList<>();
    }
    this.superclasses.add(identifier);
  }

  public void addCluster(SQLInteger id) {
    if (clusters == null) {
      this.clusters = new ArrayList<>();
    }
    this.clusters.add(id);
  }
}
/* JavaCC - OriginalChecksum=4043013624f55fdf0ea8fee6d4f211b0 (do not edit this line) */
