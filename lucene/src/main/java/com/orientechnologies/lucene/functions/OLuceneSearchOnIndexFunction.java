package com.orientechnologies.lucene.functions;

import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.orientechnologies.lucene.builder.OLuceneQueryBuilder;
import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.lucene.index.OLuceneFullTextIndex;
import com.orientechnologies.lucene.query.OLuceneKeyAndMetadata;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFromClause;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFromItem;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIdentifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.memory.MemoryIndex;

/**
 *
 */
public class OLuceneSearchOnIndexFunction extends OLuceneSearchFunctionTemplate {

  public static final String MEMORY_INDEX = "_memoryIndex";

  public static final String NAME = "search_index";

  public OLuceneSearchOnIndexFunction() {
    super(NAME, 2, 3);
  }

  @Override
  public String getName(YTDatabaseSession session) {
    return NAME;
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] params,
      OCommandContext ctx) {
    Entity element =
        iThis instanceof Entity ? (Entity) iThis : ((YTResult) iThis).toEntity();

    String indexName = (String) params[0];

    OLuceneFullTextIndex index = searchForIndex(ctx, indexName);

    if (index == null) {
      return false;
    }

    String query = (String) params[1];

    MemoryIndex memoryIndex = getOrCreateMemoryIndex(ctx);

    List<Object> key =
        index.getDefinition().getFields().stream()
            .map(s -> element.getProperty(s))
            .collect(Collectors.toList());

    for (IndexableField field : index.buildDocument(ctx.getDatabase(), key).getFields()) {
      memoryIndex.addField(field, index.indexAnalyzer());
    }

    var metadata = getMetadata(params);
    OLuceneKeyAndMetadata keyAndMetadata =
        new OLuceneKeyAndMetadata(
            new OLuceneCompositeKey(Collections.singletonList(query)).setContext(ctx), metadata);

    return memoryIndex.search(index.buildQuery(keyAndMetadata)) > 0.0f;
  }

  private Map<String, ?> getMetadata(Object[] params) {

    if (params.length == 3) {
      return (Map<String, ?>) params[2];
    }

    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  private static MemoryIndex getOrCreateMemoryIndex(OCommandContext ctx) {
    MemoryIndex memoryIndex = (MemoryIndex) ctx.getVariable(MEMORY_INDEX);
    if (memoryIndex == null) {
      memoryIndex = new MemoryIndex();
      ctx.setVariable(MEMORY_INDEX, memoryIndex);
    }

    memoryIndex.reset();
    return memoryIndex;
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return "SEARCH_INDEX( indexName, [ metdatada {} ] )";
  }

  @Override
  public boolean filterResult() {
    return true;
  }

  @Override
  public Iterable<YTIdentifiable> searchFromTarget(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      OCommandContext ctx,
      OExpression... args) {

    OLuceneFullTextIndex index = searchForIndex(target, ctx, args);

    OExpression expression = args[1];
    String query = (String) expression.execute((YTIdentifiable) null, ctx);
    if (index != null && query != null) {

      var meta = getMetadata(args, ctx);

      List<YTIdentifiable> luceneResultSet;
      try (Stream<YTRID> rids =
          index
              .getInternal()
              .getRids(ctx.getDatabase(),
                  new OLuceneKeyAndMetadata(
                      new OLuceneCompositeKey(List.of(query)).setContext(ctx), meta))) {
        luceneResultSet = rids.collect(Collectors.toList());
      }

      return luceneResultSet;
    }
    return Collections.emptyList();
  }

  private Map<String, ?> getMetadata(OExpression[] args, OCommandContext ctx) {
    if (args.length == 3) {
      return getMetadata(args[2], ctx);
    }
    return OLuceneQueryBuilder.EMPTY_METADATA;
  }

  @Override
  protected OLuceneFullTextIndex searchForIndex(
      OFromClause target, OCommandContext ctx, OExpression... args) {

    OFromItem item = target.getItem();
    OIdentifier identifier = item.getIdentifier();
    return searchForIndex(identifier.getStringValue(), ctx, args);
  }

  private OLuceneFullTextIndex searchForIndex(
      String className, OCommandContext ctx, OExpression... args) {

    String indexName = (String) args[0].execute((YTIdentifiable) null, ctx);

    final YTDatabaseSessionInternal database = ctx.getDatabase();
    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, className, indexName);

    if (index != null && index.getInternal() instanceof OLuceneFullTextIndex) {
      return (OLuceneFullTextIndex) index;
    }

    return null;
  }

  private OLuceneFullTextIndex searchForIndex(OCommandContext ctx, String indexName) {
    final YTDatabaseSessionInternal database = ctx.getDatabase();
    OIndex index = database.getMetadata().getIndexManagerInternal().getIndex(database, indexName);

    if (index != null && index.getInternal() instanceof OLuceneFullTextIndex) {
      return (OLuceneFullTextIndex) index;
    }

    return null;
  }

  @Override
  public Object getResult() {
    return super.getResult();
  }
}
