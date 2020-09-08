/**
 *    Copyright 2009-2020 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.executor.resultset;

import org.apache.ibatis.annotations.AutomapConstructor;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.cursor.defaults.DefaultCursor;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.loader.ResultLoader;
import org.apache.ibatis.executor.loader.ResultLoaderMap;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.executor.result.DefaultResultHandler;
import org.apache.ibatis.executor.result.ResultMapException;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.*;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.lang.reflect.Constructor;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Iwao AVE!
 * @author Kazuki Shimizu
 */
public class DefaultResultSetHandler implements ResultSetHandler {

  private static final Object DEFERRED = new Object();

  /**
   * sql执行器
   */
  private final Executor executor;
  /**
   * 核心配置对象
   */
  private final Configuration configuration;
  /**
   * mappedStatement
   */
  private final MappedStatement mappedStatement;
  /**
   * 分页参数
   */
  private final RowBounds rowBounds;
  /**
   * 参数处理器
   */
  private final ParameterHandler parameterHandler;
  /**
   * 结果处理器
   */
  private final ResultHandler<?> resultHandler;
  /**
   * boundSql
   */
  private final BoundSql boundSql;
  /**
   * 类型处理器的注册器, 维护jdbcType和对应的处理器的关系
   */
  private final TypeHandlerRegistry typeHandlerRegistry;
  /**
   * 对象工厂
   */
  private final ObjectFactory objectFactory;
  /**
   * 反射工厂
   */
  private final ReflectorFactory reflectorFactory;

  // nested resultmaps
  /**
   *
   */
  private final Map<CacheKey, Object> nestedResultObjects = new HashMap<>();
  /**
   *
   */
  private final Map<String, Object> ancestorObjects = new HashMap<>();
  /**
   * 前一行的值
   */
  private Object previousRowValue;

  // multiple resultsets
  private final Map<String, ResultMapping> nextResultMaps = new HashMap<>();
  private final Map<CacheKey, List<PendingRelation>> pendingRelations = new HashMap<>();

  // Cached Automappings
  private final Map<String, List<UnMappedColumnAutoMapping>> autoMappingsCache = new HashMap<>();

  // temporary marking flag that indicate using constructor mapping (use field to reduce memory usage)
  private boolean useConstructorMappings;

  private static class PendingRelation {
    public MetaObject metaObject;
    public ResultMapping propertyMapping;
  }

  private static class UnMappedColumnAutoMapping {
    private final String column;
    private final String property;
    private final TypeHandler<?> typeHandler;
    private final boolean primitive;

    public UnMappedColumnAutoMapping(String column, String property, TypeHandler<?> typeHandler, boolean primitive) {
      this.column = column;
      this.property = property;
      this.typeHandler = typeHandler;
      this.primitive = primitive;
    }
  }

  /**构造函数
   * @param executor              sql执行器
   * @param mappedStatement       mappedStatement
   * @param parameterHandler      参数处理器
   * @param resultHandler         结果处理器
   * @param boundSql              boundSql
   * @param rowBounds             分页对象
   */
  public DefaultResultSetHandler(Executor executor, MappedStatement mappedStatement, ParameterHandler parameterHandler, ResultHandler<?> resultHandler, BoundSql boundSql,
                                 RowBounds rowBounds) {
    //给类属性赋值
    this.executor = executor;
    this.configuration = mappedStatement.getConfiguration();
    this.mappedStatement = mappedStatement;
    this.rowBounds = rowBounds;
    this.parameterHandler = parameterHandler;
    this.boundSql = boundSql;
    this.typeHandlerRegistry = configuration.getTypeHandlerRegistry();
    this.objectFactory = configuration.getObjectFactory();
    this.reflectorFactory = configuration.getReflectorFactory();
    this.resultHandler = resultHandler;
  }

  //
  // HANDLE OUTPUT PARAMETER
  //

  /**处理输出参数
   * @param cs                  CallableStatement
   * @throws SQLException       异常
   */
  @Override
  public void handleOutputParameters(CallableStatement cs) throws SQLException {
    //获取参数对象
    final Object parameterObject = parameterHandler.getParameterObject();
    //构建参数对象的元数据对象
    final MetaObject metaParam = configuration.newMetaObject(parameterObject);
    //获取参数匹配关系
    final List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    for (int i = 0; i < parameterMappings.size(); i++) {
      final ParameterMapping parameterMapping = parameterMappings.get(i);
      if (parameterMapping.getMode() == ParameterMode.OUT || parameterMapping.getMode() == ParameterMode.INOUT) {
        //如果是输出类型的参数
        if (ResultSet.class.equals(parameterMapping.getJavaType())) {
          //如果<parameter>中指定的javaType为ResultSet
          //处理引用游标的输出参数
          handleRefCursorOutputParameter((ResultSet) cs.getObject(i + 1), parameterMapping, metaParam);
        } else {
          //获取参数的类型处理器
          final TypeHandler<?> typeHandler = parameterMapping.getTypeHandler();
          //利用参数对象的元数据对象进行代理,设置属性值
          metaParam.setValue(parameterMapping.getProperty(), typeHandler.getResult(cs, i + 1));
        }
      }
    }
  }

  /**处理引用游标的输出参数
   * @param rs                      结果集
   * @param parameterMapping        参数映射关系
   * @param metaParam               参数对象的元数据对象
   * @throws SQLException           异常
   */
  private void handleRefCursorOutputParameter(ResultSet rs, ParameterMapping parameterMapping, MetaObject metaParam) throws SQLException {
    if (rs == null) {
      return;
    }
    try {
      //获取resultMapId
      final String resultMapId = parameterMapping.getResultMapId();
      //根据id获取ResultMap
      final ResultMap resultMap = configuration.getResultMap(resultMapId);
      //构造结果集包装器
      final ResultSetWrapper rsw = new ResultSetWrapper(rs, configuration);
      if (this.resultHandler == null) {
        //如果结果处理器为空,创建一个默认的结果处理器
        final DefaultResultHandler resultHandler = new DefaultResultHandler(objectFactory);
        //处理行数据值
        handleRowValues(rsw, resultMap, resultHandler, new RowBounds(), null);
        //给参数设置值
        metaParam.setValue(parameterMapping.getProperty(), resultHandler.getResultList());
      } else {
        //处理行数据值
        handleRowValues(rsw, resultMap, resultHandler, new RowBounds(), null);
        //因为指定了结果处理器,不会设置参数的值
      }
    } finally {
      // issue #228 (close resultsets)
      //关闭结果集
      closeResultSet(rs);
    }
  }

  /**处理结果集
   * @param stmt              Statement
   * @return                  结果列表
   * @throws SQLException     异常
   */
  //
  // HANDLE RESULT SETS
  //
  @Override
  public List<Object> handleResultSets(Statement stmt) throws SQLException {
    ErrorContext.instance().activity("handling results").object(mappedStatement.getId());

    //初始化多结果的结果列表
    final List<Object> multipleResults = new ArrayList<>();

    int resultSetCount = 0;
    //获取第一个结果集的包装器
    ResultSetWrapper rsw = getFirstResultSet(stmt);
    //从mappedStatement中获取多结果的映射关系
    List<ResultMap> resultMaps = mappedStatement.getResultMaps();
    int resultMapCount = resultMaps.size();
    //验证结果映射的count
    validateResultMapsCount(rsw, resultMapCount);
    while (rsw != null && resultMapCount > resultSetCount) {
      //有结果集,并且结果集映射的数量大于结果集的count时,循环处理每个结果期

      //获取当前结果集的映射关系
      ResultMap resultMap = resultMaps.get(resultSetCount);
      //处理当前结果集
      handleResultSet(rsw, resultMap, multipleResults, null);
      //获取下一个结果集
      rsw = getNextResultSet(stmt);
      //处理完结果集后清除nestedResultObjects中数据
      cleanUpAfterHandlingResultSet();
      //索引++
      resultSetCount++;
    }

    String[] resultSets = mappedStatement.getResultSets();
    if (resultSets != null) {
      while (rsw != null && resultSetCount < resultSets.length) {
        ResultMapping parentMapping = nextResultMaps.get(resultSets[resultSetCount]);
        if (parentMapping != null) {
          String nestedResultMapId = parentMapping.getNestedResultMapId();
          ResultMap resultMap = configuration.getResultMap(nestedResultMapId);
          handleResultSet(rsw, resultMap, null, parentMapping);
        }
        rsw = getNextResultSet(stmt);
        cleanUpAfterHandlingResultSet();
        resultSetCount++;
      }
    }

    //如果只有一个结果集,返回列表中第一个元素, 否则返回整个结果集列表
    return collapseSingleResultList(multipleResults);
  }

  /**处理游标结果集
   * @param stmt                Statement
   * @param <E>                 结果泛型
   * @return                    游标结果集
   * @throws SQLException       异常
   */
  @Override
  public <E> Cursor<E> handleCursorResultSets(Statement stmt) throws SQLException {
    ErrorContext.instance().activity("handling cursor results").object(mappedStatement.getId());

    //获取第一个结果集的包装器
    ResultSetWrapper rsw = getFirstResultSet(stmt);

    //从mappedStatement中获取结果的映射关系
    List<ResultMap> resultMaps = mappedStatement.getResultMaps();

    int resultMapCount = resultMaps.size();
    //验证结果映射的count
    validateResultMapsCount(rsw, resultMapCount);
    if (resultMapCount != 1) {
      //游标类型的查询不支持映射多个resultMap
      throw new ExecutorException("Cursor results cannot be mapped to multiple resultMaps");
    }

    ResultMap resultMap = resultMaps.get(0);
    //返回一个默认游标
    return new DefaultCursor<>(this, resultMap, rsw, rowBounds);
  }

  /**获取第一个结果集
   * @param stmt            Statement
   * @return                结果集包装器
   * @throws SQLException   异常
   */
  private ResultSetWrapper getFirstResultSet(Statement stmt) throws SQLException {
    //从statement中获取一次结果集
    ResultSet rs = stmt.getResultSet();
    while (rs == null) {
      // move forward to get the first resultset in case the driver
      // doesn't return the resultset as the first result (HSQLDB 2.1)
      if (stmt.getMoreResults()) {
        //如果有结果
        //再执行一次获取结果集
        rs = stmt.getResultSet();
      } else {
        if (stmt.getUpdateCount() == -1) {
          //如果已经没有结果,break
          // no more results. Must be no resultset
          break;
        }
      }
    }
    //结果集不为空时,返回结果集的包装器,否则返回null
    return rs != null ? new ResultSetWrapper(rs, configuration) : null;
  }

  /**获取下一个结果集
   * @param stmt      Statement
   * @return          结果集包装器
   */
  private ResultSetWrapper getNextResultSet(Statement stmt) {
    // Making this method tolerant of bad JDBC drivers
    try {
      if (stmt.getConnection().getMetaData().supportsMultipleResultSets()) {
        //如果连接支持多结果集
        // Crazy Standard JDBC way of determining if there are more results
        if (!(!stmt.getMoreResults() && stmt.getUpdateCount() == -1)) {
          //只有有结果,或者更新条数不为-1,就获取一次结果集
          ResultSet rs = stmt.getResultSet();
          if (rs == null) {
            //为空时递归获取
            return getNextResultSet(stmt);
          } else {
            //不为空时返回包装器
            return new ResultSetWrapper(rs, configuration);
          }
        }
      }
    } catch (Exception e) {
      // Intentionally ignored.
    }
    return null;
  }

  /**关闭结果集
   * @param rs
   */
  private void closeResultSet(ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      // ignore
    }
  }

  /**
   * 清除层级对象的缓存
   */
  private void cleanUpAfterHandlingResultSet() {
    nestedResultObjects.clear();
  }

  /**验证结果映射的数量
   * @param rsw               结果集包装器
   * @param resultMapCount    结果映射的数量
   */
  private void validateResultMapsCount(ResultSetWrapper rsw, int resultMapCount) {
    //有结果的时候,结果映射的数量不能小于1
    if (rsw != null && resultMapCount < 1) {
      throw new ExecutorException("A query was run and no Result Maps were found for the Mapped Statement '" + mappedStatement.getId()
          + "'.  It's likely that neither a Result Type nor a Result Map was specified.");
    }
  }

  /**处理结果集
   * @param rsw                   结果集包装器
   * @param resultMap             结果映射关系
   * @param multipleResults       最终的结果列表对象
   * @param parentMapping         父级结果映射关系
   * @throws SQLException         异常
   */
  private void handleResultSet(ResultSetWrapper rsw, ResultMap resultMap, List<Object> multipleResults, ResultMapping parentMapping) throws SQLException {
    try {
      if (parentMapping != null) {
        //处理行数据值
        handleRowValues(rsw, resultMap, null, RowBounds.DEFAULT, parentMapping);
      } else {
        if (resultHandler == null) {
          //构造默认的结果处理器
          DefaultResultHandler defaultResultHandler = new DefaultResultHandler(objectFactory);
          //处理行数据值
          handleRowValues(rsw, resultMap, defaultResultHandler, rowBounds, null);
          //处理后的结果加入到最终的结果列表中
          multipleResults.add(defaultResultHandler.getResultList());
        } else {
          //处理行数据值
          handleRowValues(rsw, resultMap, resultHandler, rowBounds, null);
        }
      }
    } finally {
      // issue #228 (close resultsets)
      //关闭结果集
      closeResultSet(rsw.getResultSet());
    }
  }

  /**将结果转化成单个结果集的形式
   * @param multipleResults       多结果集的结果列表
   * @return                      结果列表
   */
  @SuppressWarnings("unchecked")
  private List<Object> collapseSingleResultList(List<Object> multipleResults) {
    //如果列表数量是1, 取第一个结果列表返回, 否则返回原始结果列表
    return multipleResults.size() == 1 ? (List<Object>) multipleResults.get(0) : multipleResults;
  }

  //
  // HANDLE ROWS FOR SIMPLE RESULTMAP
  //

  /**处理每行数据,封装到resultMap
   * @param rsw               结果集包装器
   * @param resultMap         结果映射关系
   * @param resultHandler     结果处理器
   * @param rowBounds         分页对象
   * @param parentMapping     父结果映射关系
   * @throws SQLException     异常
   */
  public void handleRowValues(ResultSetWrapper rsw, ResultMap resultMap, ResultHandler<?> resultHandler, RowBounds rowBounds, ResultMapping parentMapping) throws SQLException {
    if (resultMap.hasNestedResultMaps()) {
      //如果是嵌套层级的resultMap
      //检查分页
      ensureNoRowBounds();
      //检查结果处理器
      checkResultHandler();
      //处理嵌套层级的行数据
      handleRowValuesForNestedResultMap(rsw, resultMap, resultHandler, rowBounds, parentMapping);
    } else {
      //处理非嵌套层级的行数据
      handleRowValuesForSimpleResultMap(rsw, resultMap, resultHandler, rowBounds, parentMapping);
    }
  }

  /**
   * 检查分页参数
   */
  private void ensureNoRowBounds() {
    //如果开启了分页安全检查
    //limit需要小于最大的limit
    //offset需要大于0
    if (configuration.isSafeRowBoundsEnabled() && rowBounds != null && (rowBounds.getLimit() < RowBounds.NO_ROW_LIMIT || rowBounds.getOffset() > RowBounds.NO_ROW_OFFSET)) {
      throw new ExecutorException("Mapped Statements with nested result mappings cannot be safely constrained by RowBounds. "
          + "Use safeRowBoundsEnabled=false setting to bypass this check.");
    }
  }

  /**
   * 检查结果处理器
   */
  protected void checkResultHandler() {
    if (resultHandler != null && configuration.isSafeResultHandlerEnabled() && !mappedStatement.isResultOrdered()) {
      throw new ExecutorException("Mapped Statements with nested result mappings cannot be safely used with a custom ResultHandler. "
          + "Use safeResultHandlerEnabled=false setting to bypass this check "
          + "or ensure your statement returns ordered data and set resultOrdered=true on it.");
    }
  }

  /**处理非嵌套层级的行数据
   * @param rsw               结果集包装器
   * @param resultMap         结果映射关系
   * @param resultHandler     结果处理器
   * @param rowBounds         分页对象
   * @param parentMapping     父结果映射关系
   * @throws SQLException     异常
   */
  private void handleRowValuesForSimpleResultMap(ResultSetWrapper rsw, ResultMap resultMap, ResultHandler<?> resultHandler, RowBounds rowBounds, ResultMapping parentMapping)
      throws SQLException {
    //构建一个默认的结果上线文对象
    DefaultResultContext<Object> resultContext = new DefaultResultContext<>();
    ResultSet resultSet = rsw.getResultSet();
    //根据分页参数跳转到对应的行, 采取的是内存分页的方式
    skipRows(resultSet, rowBounds);
    while (shouldProcessMoreRows(resultContext, rowBounds) && !resultSet.isClosed() && resultSet.next()) {
      //有数据可以处理时

      //解析鉴别器指定的resultMap
      ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(resultSet, resultMap, null);
      //取出行值映射到结果对象中
      Object rowValue = getRowValue(rsw, discriminatedResultMap, null);
      //存储对象
      storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
    }
  }

  /**存储对象
   * @param resultHandler       结果处理器
   * @param resultContext       结果上线文对象
   * @param rowValue            实际的结果值
   * @param parentMapping       父结果映射关系
   * @param rs                  结果集
   * @throws SQLException       异常
   */
  private void storeObject(ResultHandler<?> resultHandler, DefaultResultContext<Object> resultContext, Object rowValue, ResultMapping parentMapping, ResultSet rs) throws SQLException {
    if (parentMapping != null) {
      //处理父级数据
      linkToParents(rs, parentMapping, rowValue);
    } else {
      //调用结果处理器处理数据
      callResultHandler(resultHandler, resultContext, rowValue);
    }
  }

  /**调用结果处理器处理数据
   * @param resultHandler       结果处理器
   * @param resultContext       结果上线文对象
   * @param rowValue            实际的结果值
   */
  @SuppressWarnings("unchecked" /* because ResultHandler<?> is always ResultHandler<Object>*/)
  private void callResultHandler(ResultHandler<?> resultHandler, DefaultResultContext<Object> resultContext, Object rowValue) {
    //将数据放入上下文对象
    resultContext.nextResultObject(rowValue);
    //调用结果处理器处理结果
    ((ResultHandler<Object>) resultHandler).handleResult(resultContext);
  }

  /**判断是否需要处理更多行数据
   * @param context           结果上线文对象
   * @param rowBounds         分页对象
   * @return
   */
  private boolean shouldProcessMoreRows(ResultContext<?> context, RowBounds rowBounds) {
    //结果上下文对象没有停止,并且结果数小于分页的limit
    return !context.isStopped() && context.getResultCount() < rowBounds.getLimit();
  }

  /**跳过分页的行
   * @param rs                结果集
   * @param rowBounds         分页参数
   * @throws SQLException     异常
   */
  private void skipRows(ResultSet rs, RowBounds rowBounds) throws SQLException {
    if (rs.getType() != ResultSet.TYPE_FORWARD_ONLY) {
      //如果结果集类型不是只能前向移动的类型
      if (rowBounds.getOffset() != RowBounds.NO_ROW_OFFSET) {
        //将游标跳转到offset的位置
        rs.absolute(rowBounds.getOffset());
      }
    } else {
      //将游标跳转到offset的位置
      for (int i = 0; i < rowBounds.getOffset(); i++) {
        if (!rs.next()) {
          break;
        }
      }
    }
  }

  //
  // GET VALUE FROM ROW FOR SIMPLE RESULT MAP
  //

  /**获取行值
   * @param rsw             结果集包装器
   * @param resultMap       结果映射关系
   * @param columnPrefix    列名前缀
   * @return                resultMap中指定类的对象
   * @throws SQLException   异常
   */
  private Object getRowValue(ResultSetWrapper rsw, ResultMap resultMap, String columnPrefix) throws SQLException {
    final ResultLoaderMap lazyLoader = new ResultLoaderMap();
    //使用反射,初始化一个返回值对象
    Object rowValue = createResultObject(rsw, resultMap, lazyLoader, columnPrefix);
    if (rowValue != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {
      //如果对象不为空, 并且该对象没有类型处理器
      //构建一个返回值对象的元数据对象
      final MetaObject metaObject = configuration.newMetaObject(rowValue);
      //是否找到对象值标志
      boolean foundValues = this.useConstructorMappings;
      if (shouldApplyAutomaticMappings(resultMap, false)) {
        //如果需要进行自动映射
        //应用自动映射
        foundValues = applyAutomaticMappings(rsw, resultMap, metaObject, columnPrefix) || foundValues;
      }
      //引用属性映射
      foundValues = applyPropertyMappings(rsw, resultMap, metaObject, lazyLoader, columnPrefix) || foundValues;
      //如果指定了懒加载,也认为是找到了值
      foundValues = lazyLoader.size() > 0 || foundValues;
      //如果找到值或者配置空指定了即使没数据返回空对象,返回值对象rowValue, 否则返回null
      rowValue = foundValues || configuration.isReturnInstanceForEmptyRow() ? rowValue : null;
    }
    return rowValue;
  }

  //
  // GET VALUE FROM ROW FOR NESTED RESULT MAP
  //

  /**获取行值
   * @param rsw                 结果集包装器
   * @param resultMap           结果映射关系
   * @param combinedKey         每一行数据生成的缓存key
   * @param columnPrefix        列名前缀
   * @param partialObject       部分对象
   * @return                    resultMap中指定类的对象
   * @throws SQLException       异常
   */
  private Object getRowValue(ResultSetWrapper rsw, ResultMap resultMap, CacheKey combinedKey, String columnPrefix, Object partialObject) throws SQLException {
    final String resultMapId = resultMap.getId();
    Object rowValue = partialObject;
    if (rowValue != null) {
      //创建一个值对象的元数据对象
      final MetaObject metaObject = configuration.newMetaObject(rowValue);
      //将值和resultMapId存放在map中
      putAncestor(rowValue, resultMapId);
      //
      applyNestedResultMappings(rsw, resultMap, metaObject, columnPrefix, combinedKey, false);
      ancestorObjects.remove(resultMapId);
    } else {
      final ResultLoaderMap lazyLoader = new ResultLoaderMap();
      rowValue = createResultObject(rsw, resultMap, lazyLoader, columnPrefix);
      if (rowValue != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {
        final MetaObject metaObject = configuration.newMetaObject(rowValue);
        boolean foundValues = this.useConstructorMappings;
        if (shouldApplyAutomaticMappings(resultMap, true)) {
          foundValues = applyAutomaticMappings(rsw, resultMap, metaObject, columnPrefix) || foundValues;
        }
        foundValues = applyPropertyMappings(rsw, resultMap, metaObject, lazyLoader, columnPrefix) || foundValues;
        putAncestor(rowValue, resultMapId);
        foundValues = applyNestedResultMappings(rsw, resultMap, metaObject, columnPrefix, combinedKey, true) || foundValues;
        ancestorObjects.remove(resultMapId);
        foundValues = lazyLoader.size() > 0 || foundValues;
        rowValue = foundValues || configuration.isReturnInstanceForEmptyRow() ? rowValue : null;
      }
      if (combinedKey != CacheKey.NULL_CACHE_KEY) {
        nestedResultObjects.put(combinedKey, rowValue);
      }
    }
    return rowValue;
  }

  /**将键值对放入map
   * @param resultObject        结果对象
   * @param resultMapId         resultMapId
   */
  private void putAncestor(Object resultObject, String resultMapId) {
    ancestorObjects.put(resultMapId, resultObject);
  }

  /**是否自动映射
   * @param resultMap           ResultMap
   * @param isNested            是否是嵌套层级
   * @return                    是否自动映射
   */
  private boolean shouldApplyAutomaticMappings(ResultMap resultMap, boolean isNested) {
    if (resultMap.getAutoMapping() != null) {
      //如果配置了自动映射属性,直接返回
      return resultMap.getAutoMapping();
    } else {
      if (isNested) {
        //如果是嵌套层级并且是全匹配,返回自动映射
        return AutoMappingBehavior.FULL == configuration.getAutoMappingBehavior();
      } else {
        //如果是非嵌套层级,只要不是不自动匹配, 返回自动映射
        return AutoMappingBehavior.NONE != configuration.getAutoMappingBehavior();
      }
    }
  }

  //
  // PROPERTY MAPPINGS
  //

  /**根据属性匹配,设置属性值
   * @param rsw                 结果集包装器
   * @param resultMap           结果映射关系
   * @param metaObject          元数据
   * @param lazyLoader          懒加载器
   * @param columnPrefix        列名前缀
   * @return                    对象是否赋值
   * @throws SQLException       异常
   */
  private boolean applyPropertyMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject, ResultLoaderMap lazyLoader, String columnPrefix)
      throws SQLException {
    //获取列名
    final List<String> mappedColumnNames = rsw.getMappedColumnNames(resultMap, columnPrefix);
    boolean foundValues = false;
    //获取属性值的映射关系
    final List<ResultMapping> propertyMappings = resultMap.getPropertyResultMappings();
    for (ResultMapping propertyMapping : propertyMappings) {
      //属性名加上列名前缀
      String column = prependPrefix(propertyMapping.getColumn(), columnPrefix);
      if (propertyMapping.getNestedResultMapId() != null) {
        // the user added a column attribute to a nested result map, ignore it
        column = null;
      }
      if (propertyMapping.isCompositeResult()
          || (column != null && mappedColumnNames.contains(column.toUpperCase(Locale.ENGLISH)))
          || propertyMapping.getResultSet() != null) {
        //获取属性的值
        Object value = getPropertyMappingValue(rsw.getResultSet(), metaObject, propertyMapping, lazyLoader, columnPrefix);
        // issue #541 make property optional
        final String property = propertyMapping.getProperty();
        if (property == null) {
          continue;
        } else if (value == DEFERRED) {
          //如果是懒加载,认为该属性有值
          foundValues = true;
          continue;
        }
        if (value != null) {
          //值不为空,认为属性有值
          foundValues = true;
        }
        if (value != null || (configuration.isCallSettersOnNulls() && !metaObject.getSetterType(property).isPrimitive())) {
          // gcode issue #377, call setter on nulls (value is not 'found')
          //给对象设置值
          metaObject.setValue(property, value);
        }
      }
    }
    return foundValues;
  }

  /**获取属性的映射值
   * @param rs                    结果集
   * @param metaResultObject      元数据对象
   * @param propertyMapping       属性映射关系
   * @param lazyLoader            懒加载集
   * @param columnPrefix          列名前缀
   * @return                      属性值
   * @throws SQLException         异常
   */
  private Object getPropertyMappingValue(ResultSet rs, MetaObject metaResultObject, ResultMapping propertyMapping, ResultLoaderMap lazyLoader, String columnPrefix)
      throws SQLException {
    if (propertyMapping.getNestedQueryId() != null) {
      //获取嵌套查询结果
      return getNestedQueryMappingValue(rs, metaResultObject, propertyMapping, lazyLoader, columnPrefix);
    } else if (propertyMapping.getResultSet() != null) {
      //
      addPendingChildRelation(rs, metaResultObject, propertyMapping);   // TODO is that OK?
      return DEFERRED;
    } else {
      //获取类型处理器
      final TypeHandler<?> typeHandler = propertyMapping.getTypeHandler();
      //列名拼上前缀
      final String column = prependPrefix(propertyMapping.getColumn(), columnPrefix);
      //获取值
      return typeHandler.getResult(rs, column);
    }
  }

  private List<UnMappedColumnAutoMapping> createAutomaticMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject, String columnPrefix) throws SQLException {
    final String mapKey = resultMap.getId() + ":" + columnPrefix;
    List<UnMappedColumnAutoMapping> autoMapping = autoMappingsCache.get(mapKey);
    if (autoMapping == null) {
      autoMapping = new ArrayList<>();
      final List<String> unmappedColumnNames = rsw.getUnmappedColumnNames(resultMap, columnPrefix);
      for (String columnName : unmappedColumnNames) {
        String propertyName = columnName;
        if (columnPrefix != null && !columnPrefix.isEmpty()) {
          // When columnPrefix is specified,
          // ignore columns without the prefix.
          if (columnName.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {
            propertyName = columnName.substring(columnPrefix.length());
          } else {
            continue;
          }
        }
        final String property = metaObject.findProperty(propertyName, configuration.isMapUnderscoreToCamelCase());
        if (property != null && metaObject.hasSetter(property)) {
          if (resultMap.getMappedProperties().contains(property)) {
            continue;
          }
          final Class<?> propertyType = metaObject.getSetterType(property);
          if (typeHandlerRegistry.hasTypeHandler(propertyType, rsw.getJdbcType(columnName))) {
            final TypeHandler<?> typeHandler = rsw.getTypeHandler(propertyType, columnName);
            autoMapping.add(new UnMappedColumnAutoMapping(columnName, property, typeHandler, propertyType.isPrimitive()));
          } else {
            configuration.getAutoMappingUnknownColumnBehavior()
                .doAction(mappedStatement, columnName, property, propertyType);
          }
        } else {
          configuration.getAutoMappingUnknownColumnBehavior()
              .doAction(mappedStatement, columnName, (property != null) ? property : propertyName, null);
        }
      }
      autoMappingsCache.put(mapKey, autoMapping);
    }
    return autoMapping;
  }

  private boolean applyAutomaticMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject, String columnPrefix) throws SQLException {
    List<UnMappedColumnAutoMapping> autoMapping = createAutomaticMappings(rsw, resultMap, metaObject, columnPrefix);
    boolean foundValues = false;
    if (!autoMapping.isEmpty()) {
      for (UnMappedColumnAutoMapping mapping : autoMapping) {
        final Object value = mapping.typeHandler.getResult(rsw.getResultSet(), mapping.column);
        if (value != null) {
          foundValues = true;
        }
        if (value != null || (configuration.isCallSettersOnNulls() && !mapping.primitive)) {
          // gcode issue #377, call setter on nulls (value is not 'found')
          metaObject.setValue(mapping.property, value);
        }
      }
    }
    return foundValues;
  }

  // MULTIPLE RESULT SETS

  private void linkToParents(ResultSet rs, ResultMapping parentMapping, Object rowValue) throws SQLException {
    CacheKey parentKey = createKeyForMultipleResults(rs, parentMapping, parentMapping.getColumn(), parentMapping.getForeignColumn());
    List<PendingRelation> parents = pendingRelations.get(parentKey);
    if (parents != null) {
      for (PendingRelation parent : parents) {
        if (parent != null && rowValue != null) {
          linkObjects(parent.metaObject, parent.propertyMapping, rowValue);
        }
      }
    }
  }

  private void addPendingChildRelation(ResultSet rs, MetaObject metaResultObject, ResultMapping parentMapping) throws SQLException {
    CacheKey cacheKey = createKeyForMultipleResults(rs, parentMapping, parentMapping.getColumn(), parentMapping.getColumn());
    PendingRelation deferLoad = new PendingRelation();
    deferLoad.metaObject = metaResultObject;
    deferLoad.propertyMapping = parentMapping;
    List<PendingRelation> relations = pendingRelations.computeIfAbsent(cacheKey, k -> new ArrayList<>());
    // issue #255
    relations.add(deferLoad);
    ResultMapping previous = nextResultMaps.get(parentMapping.getResultSet());
    if (previous == null) {
      nextResultMaps.put(parentMapping.getResultSet(), parentMapping);
    } else {
      if (!previous.equals(parentMapping)) {
        throw new ExecutorException("Two different properties are mapped to the same resultSet");
      }
    }
  }

  private CacheKey createKeyForMultipleResults(ResultSet rs, ResultMapping resultMapping, String names, String columns) throws SQLException {
    CacheKey cacheKey = new CacheKey();
    cacheKey.update(resultMapping);
    if (columns != null && names != null) {
      String[] columnsArray = columns.split(",");
      String[] namesArray = names.split(",");
      for (int i = 0; i < columnsArray.length; i++) {
        Object value = rs.getString(columnsArray[i]);
        if (value != null) {
          cacheKey.update(namesArray[i]);
          cacheKey.update(value);
        }
      }
    }
    return cacheKey;
  }

  //
  // INSTANTIATION & CONSTRUCTOR MAPPING
  //

  private Object createResultObject(ResultSetWrapper rsw, ResultMap resultMap, ResultLoaderMap lazyLoader, String columnPrefix) throws SQLException {
    this.useConstructorMappings = false; // reset previous mapping result
    final List<Class<?>> constructorArgTypes = new ArrayList<>();
    final List<Object> constructorArgs = new ArrayList<>();
    Object resultObject = createResultObject(rsw, resultMap, constructorArgTypes, constructorArgs, columnPrefix);
    if (resultObject != null && !hasTypeHandlerForResultObject(rsw, resultMap.getType())) {
      final List<ResultMapping> propertyMappings = resultMap.getPropertyResultMappings();
      for (ResultMapping propertyMapping : propertyMappings) {
        // issue gcode #109 && issue #149
        if (propertyMapping.getNestedQueryId() != null && propertyMapping.isLazy()) {
          resultObject = configuration.getProxyFactory().createProxy(resultObject, lazyLoader, configuration, objectFactory, constructorArgTypes, constructorArgs);
          break;
        }
      }
    }
    this.useConstructorMappings = resultObject != null && !constructorArgTypes.isEmpty(); // set current mapping result
    return resultObject;
  }

  private Object createResultObject(ResultSetWrapper rsw, ResultMap resultMap, List<Class<?>> constructorArgTypes, List<Object> constructorArgs, String columnPrefix)
      throws SQLException {
    final Class<?> resultType = resultMap.getType();
    final MetaClass metaType = MetaClass.forClass(resultType, reflectorFactory);
    final List<ResultMapping> constructorMappings = resultMap.getConstructorResultMappings();
    if (hasTypeHandlerForResultObject(rsw, resultType)) {
      return createPrimitiveResultObject(rsw, resultMap, columnPrefix);
    } else if (!constructorMappings.isEmpty()) {
      return createParameterizedResultObject(rsw, resultType, constructorMappings, constructorArgTypes, constructorArgs, columnPrefix);
    } else if (resultType.isInterface() || metaType.hasDefaultConstructor()) {
      return objectFactory.create(resultType);
    } else if (shouldApplyAutomaticMappings(resultMap, false)) {
      return createByConstructorSignature(rsw, resultType, constructorArgTypes, constructorArgs);
    }
    throw new ExecutorException("Do not know how to create an instance of " + resultType);
  }

  Object createParameterizedResultObject(ResultSetWrapper rsw, Class<?> resultType, List<ResultMapping> constructorMappings,
                                         List<Class<?>> constructorArgTypes, List<Object> constructorArgs, String columnPrefix) {
    boolean foundValues = false;
    for (ResultMapping constructorMapping : constructorMappings) {
      final Class<?> parameterType = constructorMapping.getJavaType();
      final String column = constructorMapping.getColumn();
      final Object value;
      try {
        if (constructorMapping.getNestedQueryId() != null) {
          value = getNestedQueryConstructorValue(rsw.getResultSet(), constructorMapping, columnPrefix);
        } else if (constructorMapping.getNestedResultMapId() != null) {
          final ResultMap resultMap = configuration.getResultMap(constructorMapping.getNestedResultMapId());
          value = getRowValue(rsw, resultMap, getColumnPrefix(columnPrefix, constructorMapping));
        } else {
          final TypeHandler<?> typeHandler = constructorMapping.getTypeHandler();
          value = typeHandler.getResult(rsw.getResultSet(), prependPrefix(column, columnPrefix));
        }
      } catch (ResultMapException | SQLException e) {
        throw new ExecutorException("Could not process result for mapping: " + constructorMapping, e);
      }
      constructorArgTypes.add(parameterType);
      constructorArgs.add(value);
      foundValues = value != null || foundValues;
    }
    return foundValues ? objectFactory.create(resultType, constructorArgTypes, constructorArgs) : null;
  }

  private Object createByConstructorSignature(ResultSetWrapper rsw, Class<?> resultType, List<Class<?>> constructorArgTypes, List<Object> constructorArgs) throws SQLException {
    final Constructor<?>[] constructors = resultType.getDeclaredConstructors();
    final Constructor<?> defaultConstructor = findDefaultConstructor(constructors);
    if (defaultConstructor != null) {
      return createUsingConstructor(rsw, resultType, constructorArgTypes, constructorArgs, defaultConstructor);
    } else {
      for (Constructor<?> constructor : constructors) {
        if (allowedConstructorUsingTypeHandlers(constructor, rsw.getJdbcTypes())) {
          return createUsingConstructor(rsw, resultType, constructorArgTypes, constructorArgs, constructor);
        }
      }
    }
    throw new ExecutorException("No constructor found in " + resultType.getName() + " matching " + rsw.getClassNames());
  }

  private Object createUsingConstructor(ResultSetWrapper rsw, Class<?> resultType, List<Class<?>> constructorArgTypes, List<Object> constructorArgs, Constructor<?> constructor) throws SQLException {
    boolean foundValues = false;
    for (int i = 0; i < constructor.getParameterTypes().length; i++) {
      Class<?> parameterType = constructor.getParameterTypes()[i];
      String columnName = rsw.getColumnNames().get(i);
      TypeHandler<?> typeHandler = rsw.getTypeHandler(parameterType, columnName);
      Object value = typeHandler.getResult(rsw.getResultSet(), columnName);
      constructorArgTypes.add(parameterType);
      constructorArgs.add(value);
      foundValues = value != null || foundValues;
    }
    return foundValues ? objectFactory.create(resultType, constructorArgTypes, constructorArgs) : null;
  }

  private Constructor<?> findDefaultConstructor(final Constructor<?>[] constructors) {
    if (constructors.length == 1) {
      return constructors[0];
    }

    for (final Constructor<?> constructor : constructors) {
      if (constructor.isAnnotationPresent(AutomapConstructor.class)) {
        return constructor;
      }
    }
    return null;
  }

  private boolean allowedConstructorUsingTypeHandlers(final Constructor<?> constructor, final List<JdbcType> jdbcTypes) {
    final Class<?>[] parameterTypes = constructor.getParameterTypes();
    if (parameterTypes.length != jdbcTypes.size()) {
      return false;
    }
    for (int i = 0; i < parameterTypes.length; i++) {
      if (!typeHandlerRegistry.hasTypeHandler(parameterTypes[i], jdbcTypes.get(i))) {
        return false;
      }
    }
    return true;
  }

  private Object createPrimitiveResultObject(ResultSetWrapper rsw, ResultMap resultMap, String columnPrefix) throws SQLException {
    final Class<?> resultType = resultMap.getType();
    final String columnName;
    if (!resultMap.getResultMappings().isEmpty()) {
      final List<ResultMapping> resultMappingList = resultMap.getResultMappings();
      final ResultMapping mapping = resultMappingList.get(0);
      columnName = prependPrefix(mapping.getColumn(), columnPrefix);
    } else {
      columnName = rsw.getColumnNames().get(0);
    }
    final TypeHandler<?> typeHandler = rsw.getTypeHandler(resultType, columnName);
    return typeHandler.getResult(rsw.getResultSet(), columnName);
  }

  //
  // NESTED QUERY
  //

  /**获取嵌套查询的值
   * @param rs                    结果集
   * @param constructorMapping    构造函数的映射
   * @param columnPrefix          列名前缀
   * @return                      结果值
   * @throws SQLException         异常
   */
  private Object getNestedQueryConstructorValue(ResultSet rs, ResultMapping constructorMapping, String columnPrefix) throws SQLException {
    //获取嵌套查询id
    final String nestedQueryId = constructorMapping.getNestedQueryId();
    //获取嵌套查询的语句对象
    final MappedStatement nestedQuery = configuration.getMappedStatement(nestedQueryId);
    //嵌套查询的参数类型
    final Class<?> nestedQueryParameterType = nestedQuery.getParameterMap().getType();
    //构建嵌套查询的参数对象
    final Object nestedQueryParameterObject = prepareParameterForNestedQuery(rs, constructorMapping, nestedQueryParameterType, columnPrefix);
    Object value = null;
    if (nestedQueryParameterObject != null) {
      //如果参数对象不为空
      //获取语句
      final BoundSql nestedBoundSql = nestedQuery.getBoundSql(nestedQueryParameterObject);
      //生成缓存key
      final CacheKey key = executor.createCacheKey(nestedQuery, nestedQueryParameterObject, RowBounds.DEFAULT, nestedBoundSql);
      //获取构造函数的类型
      final Class<?> targetType = constructorMapping.getJavaType();
      //构建结果加载器
      final ResultLoader resultLoader = new ResultLoader(configuration, executor, nestedQuery, nestedQueryParameterObject, targetType, key, nestedBoundSql);
      //加载结果
      value = resultLoader.loadResult();
    }
    return value;
  }

  /**获取嵌套查询的值
   * @param rs                  结果集
   * @param metaResultObject    结果的元数据对象
   * @param propertyMapping     属性映射对象
   * @param lazyLoader          懒加载
   * @param columnPrefix        列名前缀
   * @return                    结果值
   * @throws SQLException       异常
   */
  private Object getNestedQueryMappingValue(ResultSet rs, MetaObject metaResultObject, ResultMapping propertyMapping, ResultLoaderMap lazyLoader, String columnPrefix)
      throws SQLException {
    //获取嵌套查询id
    final String nestedQueryId = propertyMapping.getNestedQueryId();
    //获取属性名
    final String property = propertyMapping.getProperty();
    //获取嵌套查询的语句对象
    final MappedStatement nestedQuery = configuration.getMappedStatement(nestedQueryId);
    //嵌套查询的参数类型
    final Class<?> nestedQueryParameterType = nestedQuery.getParameterMap().getType();
    //构建嵌套查询的参数对象
    final Object nestedQueryParameterObject = prepareParameterForNestedQuery(rs, propertyMapping, nestedQueryParameterType, columnPrefix);
    Object value = null;
    if (nestedQueryParameterObject != null) {
      //如果有参数
      final BoundSql nestedBoundSql = nestedQuery.getBoundSql(nestedQueryParameterObject);
      final CacheKey key = executor.createCacheKey(nestedQuery, nestedQueryParameterObject, RowBounds.DEFAULT, nestedBoundSql);
      final Class<?> targetType = propertyMapping.getJavaType();
      if (executor.isCached(nestedQuery, key)) {
        //缓存中有数据
        //调用延迟加载
        executor.deferLoad(nestedQuery, metaResultObject, property, key, targetType);
        value = DEFERRED;
      } else {
        //构建一个ResultLoader
        final ResultLoader resultLoader = new ResultLoader(configuration, executor, nestedQuery, nestedQueryParameterObject, targetType, key, nestedBoundSql);
        if (propertyMapping.isLazy()) {
          //如果是懒加载,把当前结果的加载器加入到懒加载中
          lazyLoader.addLoader(property, metaResultObject, resultLoader);
          value = DEFERRED;
        } else {
          //直接加载对象值
          value = resultLoader.loadResult();
        }
      }
    }
    return value;
  }

  /**为嵌套查询准备餐宿
   * @param rs                结果集
   * @param resultMapping     属性映射关系对象
   * @param parameterType     参数类型
   * @param columnPrefix      列名前缀
   * @return                  参数对象
   * @throws SQLException     异常
   */
  private Object prepareParameterForNestedQuery(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType, String columnPrefix) throws SQLException {
    if (resultMapping.isCompositeResult()) {
      //多参数的嵌套查询
      return prepareCompositeKeyParameter(rs, resultMapping, parameterType, columnPrefix);
    } else {
      //单个参数的嵌套查询
      return prepareSimpleKeyParameter(rs, resultMapping, parameterType, columnPrefix);
    }
  }

  /**准备单个参数的参数对象
   * @param rs                结果集
   * @param resultMapping     属性映射关系对象
   * @param parameterType     参数类型
   * @param columnPrefix      列名前缀
   * @return                  参数对象
   * @throws SQLException     异常
   */
  private Object prepareSimpleKeyParameter(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType, String columnPrefix) throws SQLException {
    final TypeHandler<?> typeHandler;
    if (typeHandlerRegistry.hasTypeHandler(parameterType)) {
      //获取参数类型的处理器
      typeHandler = typeHandlerRegistry.getTypeHandler(parameterType);
    } else {
      //设置为未知类型的类型处理器
      typeHandler = typeHandlerRegistry.getUnknownTypeHandler();
    }
    //获取结果
    return typeHandler.getResult(rs, prependPrefix(resultMapping.getColumn(), columnPrefix));
  }

  /**准备多参数的参数对象
   * @param rs                结果集
   * @param resultMapping     属性映射关系对象
   * @param parameterType     参数类型
   * @param columnPrefix      列名前缀
   * @return                  参数对象
   * @throws SQLException     异常
   */
  private Object prepareCompositeKeyParameter(ResultSet rs, ResultMapping resultMapping, Class<?> parameterType, String columnPrefix) throws SQLException {
    //实例化参数对象
    final Object parameterObject = instantiateParameterObject(parameterType);
    //获取参数对象的元数据对象
    final MetaObject metaObject = configuration.newMetaObject(parameterObject);
    boolean foundValues = false;
    for (ResultMapping innerResultMapping : resultMapping.getComposites()) {
      //遍历嵌套查询的属性映射关系对象列表
      //获取setter方法的参数类型
      final Class<?> propType = metaObject.getSetterType(innerResultMapping.getProperty());
      //获取类型处理器
      final TypeHandler<?> typeHandler = typeHandlerRegistry.getTypeHandler(propType);
      //获取属性值
      final Object propValue = typeHandler.getResult(rs, prependPrefix(innerResultMapping.getColumn(), columnPrefix));
      // issue #353 & #560 do not execute nested query if key is null
      if (propValue != null) {
        //属性值不为null,给参数对象设置属性值
        metaObject.setValue(innerResultMapping.getProperty(), propValue);
        foundValues = true;
      }
    }
    //如果当前查询结果不为空且参数对象的属性值不为null,返回参数对象
    return foundValues ? parameterObject : null;
  }

  /**实例化参数对象
   * @param parameterType         参数类型
   * @return                      参数对象
   */
  private Object instantiateParameterObject(Class<?> parameterType) {
    if (parameterType == null) {
      //未指定类型,默认HashMap
      return new HashMap<>();
    } else if (ParamMap.class.equals(parameterType)) {
      //如果指定的是ParamMap,返回HashMap
      return new HashMap<>(); // issue #649
    } else {
      //使用对象工厂创建对象
      return objectFactory.create(parameterType);
    }
  }

  //
  // DISCRIMINATOR
  //

  /**解析鉴别器的ResultMap
   * @param rs              结果集
   * @param resultMap       对象关系映射
   * @param columnPrefix    列名前缀
   * @return                鉴别器的对象关系映射
   * @throws SQLException   异常
   */
  public ResultMap resolveDiscriminatedResultMap(ResultSet rs, ResultMap resultMap, String columnPrefix) throws SQLException {
    Set<String> pastDiscriminators = new HashSet<>();
    //获取鉴别器
    Discriminator discriminator = resultMap.getDiscriminator();
    while (discriminator != null) {
      //获取鉴别器的中值
      final Object value = getDiscriminatorValue(rs, discriminator, columnPrefix);
      //很久属性值获取鉴别器的resultMapId
      final String discriminatedMapId = discriminator.getMapIdFor(String.valueOf(value));
      if (configuration.hasResultMap(discriminatedMapId)) {
        //如果有鉴别器resultMapId对应的resultMap
        resultMap = configuration.getResultMap(discriminatedMapId);
        Discriminator lastDiscriminator = discriminator;
        discriminator = resultMap.getDiscriminator();
        if (discriminator == lastDiscriminator || !pastDiscriminators.add(discriminatedMapId)) {
          //如果当前的鉴别器和上个鉴别器一样
          //或者已经处理过当前鉴别器了
          //break
          break;
        }
      } else {
        break;
      }
    }
    return resultMap;
  }

  /**获取鉴别器的值
   * @param rs
   * @param discriminator
   * @param columnPrefix
   * @return
   * @throws SQLException
   */
  private Object getDiscriminatorValue(ResultSet rs, Discriminator discriminator, String columnPrefix) throws SQLException {
    //获取鉴别器的结果映射对象
    final ResultMapping resultMapping = discriminator.getResultMapping();
    //获取类型处理器
    final TypeHandler<?> typeHandler = resultMapping.getTypeHandler();
    //获取结果
    return typeHandler.getResult(rs, prependPrefix(resultMapping.getColumn(), columnPrefix));
  }

  /**拼接前缀
   * @param columnName          列名
   * @param prefix              前缀
   * @return                    前缀拼上列名
   */
  private String prependPrefix(String columnName, String prefix) {
    if (columnName == null || columnName.length() == 0 || prefix == null || prefix.length() == 0) {
      return columnName;
    }
    return prefix + columnName;
  }

  //
  // HANDLE NESTED RESULT MAPS
  //

  /**处理嵌套层级的resultMapping
   * @param rsw                 结果集包装器
   * @param resultMap           结果映射关系对象
   * @param resultHandler       结果处理器
   * @param rowBounds           分页参数
   * @param parentMapping       父级的结果映射关系对象
   * @throws SQLException       异常
   */
  private void handleRowValuesForNestedResultMap(ResultSetWrapper rsw, ResultMap resultMap, ResultHandler<?> resultHandler, RowBounds rowBounds, ResultMapping parentMapping) throws SQLException {
    //构建一个默认的结果上线文对象
    final DefaultResultContext<Object> resultContext = new DefaultResultContext<>();
    //获取结果集
    ResultSet resultSet = rsw.getResultSet();
    ////根据分页参数跳转到对应的行, 采取的是内存分页的方式
    skipRows(resultSet, rowBounds);
    Object rowValue = previousRowValue;
    while (shouldProcessMoreRows(resultContext, rowBounds) && !resultSet.isClosed() && resultSet.next()) {
      //有数据可以处理时

      //解析鉴别器指定的resultMap
      final ResultMap discriminatedResultMap = resolveDiscriminatedResultMap(resultSet, resultMap, null);
      //创建行缓存key
      final CacheKey rowKey = createRowKey(discriminatedResultMap, rsw, null);
      //根据缓存key获取部分对象
      Object partialObject = nestedResultObjects.get(rowKey);
      // issue #577 && #542
      if (mappedStatement.isResultOrdered()) {
        //如果为 true，就是假设包含了嵌套结果集或是分组了，这样的话当返回一个主结果行的时候，就不会发生有对前面结果集的引用的情况。这就使得在获取嵌套的结果集的时候不至于导致内存不够用。
        if (partialObject == null && rowValue != null) {
          //清除嵌套结果对象缓存
          nestedResultObjects.clear();
          //存储对象
          storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
        }
        //获取行值
        rowValue = getRowValue(rsw, discriminatedResultMap, rowKey, null, partialObject);
      } else {
        //获取行值
        rowValue = getRowValue(rsw, discriminatedResultMap, rowKey, null, partialObject);
        if (partialObject == null) {
          //存储对象
          storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
        }
      }
    }
    if (rowValue != null && mappedStatement.isResultOrdered() && shouldProcessMoreRows(resultContext, rowBounds)) {
      //获取行值
      storeObject(resultHandler, resultContext, rowValue, parentMapping, resultSet);
      previousRowValue = null;
    } else if (rowValue != null) {
      previousRowValue = rowValue;
    }
  }

  //
  // NESTED RESULT MAP (JOIN MAPPING)
  //

  /**处理嵌套层级的resultMapping
   * @param rsw               结果集包装器
   * @param resultMap         结果映射关系对象
   * @param metaObject        元数据
   * @param parentPrefix      父层级的列名前缀
   * @param parentRowKey      父层级的行缓存key
   * @param newObject         是否是新对象
   * @return                  是否有值
   */
  private boolean applyNestedResultMappings(ResultSetWrapper rsw, ResultMap resultMap, MetaObject metaObject, String parentPrefix, CacheKey parentRowKey, boolean newObject) {
    boolean foundValues = false;
    for (ResultMapping resultMapping : resultMap.getPropertyResultMappings()) {
      //遍历所有的属性映射关系
      final String nestedResultMapId = resultMapping.getNestedResultMapId();
      if (nestedResultMapId != null && resultMapping.getResultSet() == null) {
        //如果当前属性指定了嵌套查询的resultMapId并且resultSet属性为空
        try {
          //获取列名前缀
          final String columnPrefix = getColumnPrefix(parentPrefix, resultMapping);
          //获取嵌套层级的resultMap
          final ResultMap nestedResultMap = getNestedResultMap(rsw.getResultSet(), nestedResultMapId, columnPrefix);
          if (resultMapping.getColumnPrefix() == null) {
            // try to fill circular reference only when columnPrefix
            // is not specified for the nested result map (issue #215)
            Object ancestorObject = ancestorObjects.get(nestedResultMapId);
            if (ancestorObject != null) {
              if (newObject) {
                //根据属性的类型是否是集合类型设置属性值
                linkObjects(metaObject, resultMapping, ancestorObject); // issue #385
              }
              continue;
            }
          }
          //创建当前行缓存key
          final CacheKey rowKey = createRowKey(nestedResultMap, rsw, columnPrefix);
          //创建父节点和子节点组合的缓存key
          final CacheKey combinedKey = combineKeys(rowKey, parentRowKey);
          //从缓存中获取对象
          Object rowValue = nestedResultObjects.get(combinedKey);
          boolean knownValue = rowValue != null;
          //处理集合属性
          instantiateCollectionPropertyIfAppropriate(resultMapping, metaObject); // mandatory
          if (anyNotNullColumnHasValue(resultMapping, columnPrefix, rsw)) {
            //如果有非空列有值
            //获取值
            rowValue = getRowValue(rsw, nestedResultMap, combinedKey, columnPrefix, rowValue);
            if (rowValue != null && !knownValue) {
              //根据属性的类型是否是集合类型设置属性值
              linkObjects(metaObject, resultMapping, rowValue);
              foundValues = true;
            }
          }
        } catch (SQLException e) {
          throw new ExecutorException("Error getting nested result map values for '" + resultMapping.getProperty() + "'.  Cause: " + e, e);
        }
      }
    }
    return foundValues;
  }

  /**获取列的前缀
   * @param parentPrefix        父层级的前缀
   * @param resultMapping       当前属性的映射关系
   * @return                    前缀
   */
  private String getColumnPrefix(String parentPrefix, ResultMapping resultMapping) {
    final StringBuilder columnPrefixBuilder = new StringBuilder();
    if (parentPrefix != null) {
      //拼接父级的列前缀
      columnPrefixBuilder.append(parentPrefix);
    }
    if (resultMapping.getColumnPrefix() != null) {
      //拼接自己的前缀
      columnPrefixBuilder.append(resultMapping.getColumnPrefix());
    }
    //返回前缀
    return columnPrefixBuilder.length() == 0 ? null : columnPrefixBuilder.toString().toUpperCase(Locale.ENGLISH);
  }

  /**判断是否非空的列有值
   * @param resultMapping         属性映射关系对象
   * @param columnPrefix          列名前缀
   * @param rsw                   结果集包装对象
   * @return                      是否有值
   * @throws SQLException         异常
   */
  private boolean anyNotNullColumnHasValue(ResultMapping resultMapping, String columnPrefix, ResultSetWrapper rsw) throws SQLException {
    //获取当前属性指定的列名
    Set<String> notNullColumns = resultMapping.getNotNullColumns();
    if (notNullColumns != null && !notNullColumns.isEmpty()) {
      //获取结果集
      ResultSet rs = rsw.getResultSet();
      for (String column : notNullColumns) {
        //读取列的值
        rs.getObject(prependPrefix(column, columnPrefix));
        if (!rs.wasNull()) {
          //不为空返回true
          return true;
        }
      }
      return false;
    } else if (columnPrefix != null) {
      for (String columnName : rsw.getColumnNames()) {
        if (columnName.toUpperCase().startsWith(columnPrefix.toUpperCase())) {
          //如果结果集列中中包含当前列,返回true
          return true;
        }
      }
      return false;
    }
    return true;
  }

  /**获取嵌套层级的resultMap
   * @param rs
   * @param nestedResultMapId
   * @param columnPrefix
   * @return
   * @throws SQLException
   */
  private ResultMap getNestedResultMap(ResultSet rs, String nestedResultMapId, String columnPrefix) throws SQLException {
    //根据嵌套的resultMapId获取resultMap
    ResultMap nestedResultMap = configuration.getResultMap(nestedResultMapId);
    //解析鉴别器
    return resolveDiscriminatedResultMap(rs, nestedResultMap, columnPrefix);
  }

  //
  // UNIQUE RESULT KEY
  //

  /**计算缓存key
   * @param resultMap           结果映射关系对象
   * @param rsw                 结果集包装器
   * @param columnPrefix        列前缀
   * @return                    缓存key
   * @throws SQLException       异常
   */
  private CacheKey createRowKey(ResultMap resultMap, ResultSetWrapper rsw, String columnPrefix) throws SQLException {
    //创建一个缓存key对象
    final CacheKey cacheKey = new CacheKey();
    //使用resultMapId计算一次缓存key
    cacheKey.update(resultMap.getId());
    ///获取用于计算缓存key的resultMapping集合
    List<ResultMapping> resultMappings = getResultMappingsForRowKey(resultMap);
    if (resultMappings.isEmpty()) {
      //如果是空集合
      if (Map.class.isAssignableFrom(resultMap.getType())) {
        //指定ResultMap的类型指定的是Map时
        //使用map的方式计算缓存key
        createRowKeyForMap(rsw, cacheKey);
      } else {
        //对未匹配的属性计算缓存key
        createRowKeyForUnmappedProperties(resultMap, rsw, cacheKey, columnPrefix);
      }
    } else {
      //使用匹配属性的方式计算缓存key
      createRowKeyForMappedProperties(resultMap, rsw, cacheKey, resultMappings, columnPrefix);
    }
    if (cacheKey.getUpdateCount() < 2) {
      //如果最终缓存key的计算次数小于2,返回一个空缓存key
      return CacheKey.NULL_CACHE_KEY;
    }
    return cacheKey;
  }

  /**组合缓存key值
   * @param rowKey            当前行的缓存key
   * @param parentRowKey      父级缓存key
   * @return                  新的缓存key
   */
  private CacheKey combineKeys(CacheKey rowKey, CacheKey parentRowKey) {
    if (rowKey.getUpdateCount() > 1 && parentRowKey.getUpdateCount() > 1) {
      CacheKey combinedKey;
      try {
        //clone新对象,不影响之前的缓存key
        combinedKey = rowKey.clone();
      } catch (CloneNotSupportedException e) {
        throw new ExecutorException("Error cloning cache key.  Cause: " + e, e);
      }
      //用父级key重新计算新的缓存key
      combinedKey.update(parentRowKey);
      return combinedKey;
    }
    //如果当前行的key或父级key是未计算过缓存hash值的cacheKey,返回空缓存key
    return CacheKey.NULL_CACHE_KEY;
  }

  /**获取用于生成行数据缓存的属性映射关系对象结合
   * @param resultMap
   * @return
   */
  private List<ResultMapping> getResultMappingsForRowKey(ResultMap resultMap) {
    //id可以唯一确定行值,优先取id
    //先获取id类型的映射关系集合
    List<ResultMapping> resultMappings = resultMap.getIdResultMappings();
    if (resultMappings.isEmpty()) {
      //如果未指定id字段,再获取属性映射关系集合
      resultMappings = resultMap.getPropertyResultMappings();
    }
    return resultMappings;
  }

  /**为匹配的自动创建缓存key
   * @param resultMap           结果映射关系对象
   * @param rsw                 结果集包装器
   * @param cacheKey            缓存key
   * @param resultMappings      属性的映射关系对象集合
   * @param columnPrefix        列前缀
   * @throws SQLException       异常
   */
  private void createRowKeyForMappedProperties(ResultMap resultMap, ResultSetWrapper rsw, CacheKey cacheKey, List<ResultMapping> resultMappings, String columnPrefix) throws SQLException {
    for (ResultMapping resultMapping : resultMappings) {
      //遍历属性映射对象集合
      if (resultMapping.isSimple()) {
        //只处理简单类型的映射关系
        //属性拼接列前缀
        final String column = prependPrefix(resultMapping.getColumn(), columnPrefix);
        //获取类型处理器
        final TypeHandler<?> th = resultMapping.getTypeHandler();
        //获取已匹配的列名
        List<String> mappedColumnNames = rsw.getMappedColumnNames(resultMap, columnPrefix);
        // Issue #114
        if (column != null && mappedColumnNames.contains(column.toUpperCase(Locale.ENGLISH))) {
          //如果包含当前列名
          //获取列的值
          final Object value = th.getResult(rsw.getResultSet(), column);
          if (value != null || configuration.isReturnInstanceForEmptyRow()) {
            //用有值得列和值计算缓存key
            cacheKey.update(column);
            cacheKey.update(value);
          }
        }
      }
    }
  }

  /**用未匹配的属性创建缓存key
   * @param resultMap           结果映射关系对象
   * @param rsw                 结果集包装器
   * @param cacheKey            缓存key
   * @param columnPrefix        列前缀
   * @throws SQLException       异常
   */
  private void createRowKeyForUnmappedProperties(ResultMap resultMap, ResultSetWrapper rsw, CacheKey cacheKey, String columnPrefix) throws SQLException {
    //创建一个resultMap的java类的元Class对象
    final MetaClass metaType = MetaClass.forClass(resultMap.getType(), reflectorFactory);
    //获取未匹配的列
    List<String> unmappedColumnNames = rsw.getUnmappedColumnNames(resultMap, columnPrefix);
    for (String column : unmappedColumnNames) {
      String property = column;
      if (columnPrefix != null && !columnPrefix.isEmpty()) {
        //如果指定了列前缀
        // When columnPrefix is specified, ignore columns without the prefix.
        if (column.toUpperCase(Locale.ENGLISH).startsWith(columnPrefix)) {
          //去除前缀
          property = column.substring(columnPrefix.length());
        } else {
          continue;
        }
      }
      if (metaType.findProperty(property, configuration.isMapUnderscoreToCamelCase()) != null) {
        //如果找到该属性值
        String value = rsw.getResultSet().getString(column);
        if (value != null) {
          //用有值得列和值计算缓存key
          cacheKey.update(column);
          cacheKey.update(value);
        }
      }
    }
  }

  /**对Map类型的行数据创建缓存key
   * @param rsw                   结果集包装对象
   * @param cacheKey              缓存key
   * @throws SQLException         异常
   */
  private void createRowKeyForMap(ResultSetWrapper rsw, CacheKey cacheKey) throws SQLException {
    List<String> columnNames = rsw.getColumnNames();
    for (String columnName : columnNames) {
      //遍历所有列
      final String value = rsw.getResultSet().getString(columnName);
      if (value != null) {
        //用列值非空的列和值进行缓存key的计算
        cacheKey.update(columnName);
        cacheKey.update(value);
      }
    }
  }

  /**根据属性的类型是否是集合类型设置属性值
   * @param metaObject          元数据对象
   * @param resultMapping       当前属性的映射关系
   * @param rowValue            当前值
   */
  private void linkObjects(MetaObject metaObject, ResultMapping resultMapping, Object rowValue) {
    //判断当前属性是否是集合类型,如果是,返回集合
    final Object collectionProperty = instantiateCollectionPropertyIfAppropriate(resultMapping, metaObject);
    if (collectionProperty != null) {
      //返回的集合对象不为nul
      //为集合对象创建一个元数据
      final MetaObject targetMetaObject = configuration.newMetaObject(collectionProperty);
      //讲值设置add到集合中
      targetMetaObject.add(rowValue);
    } else {
      //不是集合类型,直接设置值
      metaObject.setValue(resultMapping.getProperty(), rowValue);
    }
  }

  /**实例化一个集合属性
   * @param resultMapping           列和属性值得匹配关系
   * @param metaObject              元数据对象
   * @return                        结果对象
   */
  private Object instantiateCollectionPropertyIfAppropriate(ResultMapping resultMapping, MetaObject metaObject) {
    //获取属性名
    final String propertyName = resultMapping.getProperty();
    //获取属性值
    Object propertyValue = metaObject.getValue(propertyName);
    if (propertyValue == null) {
      //获取javaType
      Class<?> type = resultMapping.getJavaType();
      if (type == null) {
        //类型为空时从元数据中获取Setter方法的类型
        type = metaObject.getSetterType(propertyName);
      }
      try {
        if (objectFactory.isCollection(type)) {
          //如果类型为集合类型,创建一个集合
          propertyValue = objectFactory.create(type);
          //给属性设置值
          metaObject.setValue(propertyName, propertyValue);
          //返回集合
          return propertyValue;
        }
      } catch (Exception e) {
        throw new ExecutorException("Error instantiating collection property for result '" + resultMapping.getProperty() + "'.  Cause: " + e, e);
      }
    } else if (objectFactory.isCollection(propertyValue.getClass())) {
      //如果是集合类型,直接返回属性值
      return propertyValue;
    }
    return null;
  }

  /**判断返回的结果对象是否有类型处理器
   * @param rsw                   结果集包装对象
   * @param resultType            返回对象的类对象
   * @return                      是否有类型处理器
   */
  private boolean hasTypeHandlerForResultObject(ResultSetWrapper rsw, Class<?> resultType) {
    if (rsw.getColumnNames().size() == 1) {
      return typeHandlerRegistry.hasTypeHandler(resultType, rsw.getJdbcType(rsw.getColumnNames().get(0)));
    }
    return typeHandlerRegistry.hasTypeHandler(resultType);
  }

}
