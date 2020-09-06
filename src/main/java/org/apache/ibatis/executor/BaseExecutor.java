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
package org.apache.ibatis.executor;

import static org.apache.ibatis.executor.ExecutionPlaceholder.EXECUTION_PLACEHOLDER;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementUtil;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.logging.jdbc.ConnectionLogger;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.type.TypeHandlerRegistry;

/**
 * @author Clinton Begin
 */
public abstract class BaseExecutor implements Executor {

  private static final Log log = LogFactory.getLog(BaseExecutor.class);

  /**
   * 事务
   */
  protected Transaction transaction;
  /**
   * 包装当前对象的Executor
   */
  protected Executor wrapper;

  /**
   * 延迟加载的队列
   */
  protected ConcurrentLinkedQueue<DeferredLoad> deferredLoads;
  /**
   * 本地缓存, 一级缓存
   */
  protected PerpetualCache localCache;
  /**
   * 本地输出参数的一级缓存
   */
  protected PerpetualCache localOutputParameterCache;
  /**
   * 核心配置对象
   */
  protected Configuration configuration;

  /**
   * //todo
   */
  protected int queryStack;
  /**
   * 是否关闭
   */
  private boolean closed;

  /**受保护的构造方法
   * @param configuration       核心配置对象
   * @param transaction         事务
   */
  protected BaseExecutor(Configuration configuration, Transaction transaction) {
    this.transaction = transaction;
    this.deferredLoads = new ConcurrentLinkedQueue<>();
    this.localCache = new PerpetualCache("LocalCache");
    this.localOutputParameterCache = new PerpetualCache("LocalOutputParameterCache");
    this.closed = false;
    this.configuration = configuration;
    this.wrapper = this;
  }

  /**获取事务
   * @return                    事务
   */
  @Override
  public Transaction getTransaction() {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    return transaction;
  }

  /**关闭executor
   * @param forceRollback       是否强制回滚
   */
  @Override
  public void close(boolean forceRollback) {
    try {
      try {
        //根据需要回滚
        rollback(forceRollback);
      } finally {
        if (transaction != null) {
          //关闭事务
          transaction.close();
        }
      }
    } catch (SQLException e) {
      // Ignore. There's nothing that can be done at this point.
      log.warn("Unexpected exception on closing transaction.  Cause: " + e);
    } finally {
      //清除引用
      transaction = null;
      deferredLoads = null;
      localCache = null;
      localOutputParameterCache = null;
      closed = true;
    }
  }

  /**是否关闭
   * @return                    是否关闭
   */
  @Override
  public boolean isClosed() {
    return closed;
  }

  /**执行更新语句
   * @param ms              MappedStatement
   * @param parameter       参数
   * @return                影响的行数
   * @throws SQLException   异常
   */
  @Override
  public int update(MappedStatement ms, Object parameter) throws SQLException {
    ErrorContext.instance().resource(ms.getResource()).activity("executing an update").object(ms.getId());
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    //清除本地缓存
    clearLocalCache();
    //执行doUpdate
    return doUpdate(ms, parameter);
  }

  /**执行批量语句
   * @return                    批处理结果
   * @throws SQLException       异常
   */
  @Override
  public List<BatchResult> flushStatements() throws SQLException {
    return flushStatements(false);
  }

  /**执行批量语句
   * @param isRollBack          是否回滚
   * @return                    批处理结果
   * @throws SQLException       异常
   */
  public List<BatchResult> flushStatements(boolean isRollBack) throws SQLException {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    //调用doFlushStatements
    return doFlushStatements(isRollBack);
  }

  /**执行带结果返回的查询
   * @param ms              MappedStatement
   * @param parameter       参数
   * @param rowBounds       分页对象
   * @param resultHandler   结果处理器
   * @param <E>             返回对象的泛型
   * @return                对象列表
   * @throws SQLException   异常
   */
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler) throws SQLException {
    //获取boundsql对象
    BoundSql boundSql = ms.getBoundSql(parameter);
    //创建缓存key对象
    CacheKey key = createCacheKey(ms, parameter, rowBounds, boundSql);
    //调用query方法
    return query(ms, parameter, rowBounds, resultHandler, key, boundSql);
  }

  /**执行带结果返回的查询
   * @param ms              MappedStatement
   * @param parameter       参数
   * @param rowBounds       分页对象
   * @param resultHandler   结果处理器
   * @param key        缓存key
   * @param boundSql        boundSql
   * @param <E>             返回对象的泛型
   * @return                对象列表
   * @throws SQLException   异常
   */
  @SuppressWarnings("unchecked")
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, CacheKey key, BoundSql boundSql) throws SQLException {
    ErrorContext.instance().resource(ms.getResource()).activity("executing a query").object(ms.getId());
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    if (queryStack == 0 && ms.isFlushCacheRequired()) {
      //清除一级缓存
      clearLocalCache();
    }
    List<E> list;
    try {
      queryStack++;
      //从一级缓存中查找数据
      list = resultHandler == null ? (List<E>) localCache.getObject(key) : null;
      if (list != null) {
        //如果找到数据,处理输出参数, 对callable类型方法处理
        handleLocallyCachedOutputParameters(ms, key, parameter, boundSql);
      } else {
        //调用数据库查询
        list = queryFromDatabase(ms, parameter, rowBounds, resultHandler, key, boundSql);
      }
    } finally {
      queryStack--;
    }
    if (queryStack == 0) {
      //todo 为何需要判断queryStack == 0, 不是应该一直为0吗
      for (DeferredLoad deferredLoad : deferredLoads) {
        deferredLoad.load();
      }
      // issue #601
      deferredLoads.clear();
      if (configuration.getLocalCacheScope() == LocalCacheScope.STATEMENT) {
        // issue #482
        //清除一级缓存
        clearLocalCache();
      }
    }
    return list;
  }

  @Override
  public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
    //获取boundsql
    BoundSql boundSql = ms.getBoundSql(parameter);
    //调用doQueryCursor
    return doQueryCursor(ms, parameter, rowBounds, boundSql);
  }

  /**延迟加载
   * @param ms                  MappedStatement
   * @param resultObject        元数据对象
   * @param property            属性名
   * @param key                 缓存key
   * @param targetType          对象类型
   */
  @Override
  public void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key, Class<?> targetType) {
    //todo
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    DeferredLoad deferredLoad = new DeferredLoad(resultObject, property, key, localCache, configuration, targetType);
    if (deferredLoad.canLoad()) {
      deferredLoad.load();
    } else {
      deferredLoads.add(new DeferredLoad(resultObject, property, key, localCache, configuration, targetType));
    }
  }

  @Override
  public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
    if (closed) {
      throw new ExecutorException("Executor was closed.");
    }
    //创建一个cacheKey
    CacheKey cacheKey = new CacheKey();
    //使用mappedStatement的id,作为计算缓存key值的一部分
    cacheKey.update(ms.getId());
    //使用分页对象的offset,作为计算缓存key值的一部分
    cacheKey.update(rowBounds.getOffset());
    //使用分页对象的limit,作为计算缓存key值的一部分
    cacheKey.update(rowBounds.getLimit());
    //使用原始的sql语句,作为计算缓存key值的一部分
    cacheKey.update(boundSql.getSql());
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    TypeHandlerRegistry typeHandlerRegistry = ms.getConfiguration().getTypeHandlerRegistry();
    // mimic DefaultParameterHandler logic
    for (ParameterMapping parameterMapping : parameterMappings) {
      //使用sql的输入参数,作为计算缓存key值的一部分
      if (parameterMapping.getMode() != ParameterMode.OUT) {
        Object value;
        String propertyName = parameterMapping.getProperty();
        if (boundSql.hasAdditionalParameter(propertyName)) {
          value = boundSql.getAdditionalParameter(propertyName);
        } else if (parameterObject == null) {
          value = null;
        } else if (typeHandlerRegistry.hasTypeHandler(parameterObject.getClass())) {
          value = parameterObject;
        } else {
          MetaObject metaObject = configuration.newMetaObject(parameterObject);
          value = metaObject.getValue(propertyName);
        }
        cacheKey.update(value);
      }
    }
    //使用环境参数,作为计算缓存key值的一部分
    if (configuration.getEnvironment() != null) {
      // issue #176
      cacheKey.update(configuration.getEnvironment().getId());
    }
    return cacheKey;
  }

  /**当前CacheKey是否有对应的缓存
   * @param ms        MappedStatement
   * @param key       CacheKey
   * @return          是否有对应的缓存
   */
  @Override
  public boolean isCached(MappedStatement ms, CacheKey key) {
    //如果缓存中能命中,返回已经缓存
    return localCache.getObject(key) != null;
  }

  /**提交事务
   * @param required          是否需要
   * @throws SQLException     异常
   */
  @Override
  public void commit(boolean required) throws SQLException {
    if (closed) {
      throw new ExecutorException("Cannot commit, transaction is already closed");
    }
    //清除本地缓存
    clearLocalCache();
    //执行批处理语句
    flushStatements();
    //根据需要提交事务
    if (required) {
      transaction.commit();
    }
  }

  /**回滚事务
   * @param required          是否需求
   * @throws SQLException     异常
   */
  @Override
  public void rollback(boolean required) throws SQLException {
    if (!closed) {
      try {
        //清除本地缓存
        clearLocalCache();
        //执行批处理语句
        flushStatements(true);
      } finally {
        //根据需要回滚事务
        if (required) {
          transaction.rollback();
        }
      }
    }
  }

  /**
   * 清除本地缓存
   */
  @Override
  public void clearLocalCache() {
    if (!closed) {
      //清除本地缓存
      localCache.clear();
      //清除输出参数缓存
      localOutputParameterCache.clear();
    }
  }

  /**执行update
   * @param ms                  MappedStatement
   * @param parameter           更新参数
   * @return
   * @throws SQLException
   */
  protected abstract int doUpdate(MappedStatement ms, Object parameter) throws SQLException;

  /**执行批处理
   * @param isRollback          是否需要回滚
   * @return                    批处理结果
   * @throws SQLException       异常
   */
  protected abstract List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException;

  /**执行query
   * @param ms                  MappedStatement
   * @param parameter           参数
   * @param rowBounds           分页对象
   * @param resultHandler       结果处理器
   * @param boundSql            boundSql
   * @param <E>                 结果对象泛型
   * @return                    结果
   * @throws SQLException       异常
   */
  protected abstract <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql)
      throws SQLException;

  /**执行游标查询
   * @param ms                  MappedStatement
   * @param parameter           参数
   * @param rowBounds           分页对象
   * @param boundSql            boundSql
   * @param <E>                 结果对象泛型
   * @return                    结果
   * @throws SQLException       异常
   */
  protected abstract <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql)
      throws SQLException;

  /**关闭Statement
   * @param statement
   */
  protected void closeStatement(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }

  /**
   * Apply a transaction timeout.
   *
   * @param statement
   *          a current statement
   * @throws SQLException
   *           if a database access error occurs, this method is called on a closed <code>Statement</code>
   * @since 3.4.0
   * @see StatementUtil#applyTransactionTimeout(Statement, Integer, Integer)
   */
  protected void applyTransactionTimeout(Statement statement) throws SQLException {
    StatementUtil.applyTransactionTimeout(statement, statement.getQueryTimeout(), transaction.getTimeout());
  }

  /**处理本地缓存中的输出参数
   * @param ms              MappedStatement
   * @param key             缓存key
   * @param parameter       传入的参数对象
   * @param boundSql        boundsql
   */
  private void handleLocallyCachedOutputParameters(MappedStatement ms, CacheKey key, Object parameter, BoundSql boundSql) {
    //对callable类型的语句处理输出参数
    if (ms.getStatementType() == StatementType.CALLABLE) {
      //从输出参数的本地缓存中获取缓存
      final Object cachedParameter = localOutputParameterCache.getObject(key);
      if (cachedParameter != null && parameter != null) {
        //如果传入的参数不为空,并且命中缓存
        //创建一个被缓存对象的元数据对象
        final MetaObject metaCachedParameter = configuration.newMetaObject(cachedParameter);
        //创建一个传入参数的的元数据对象
        final MetaObject metaParameter = configuration.newMetaObject(parameter);
        for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
          if (parameterMapping.getMode() != ParameterMode.IN) {
            final String parameterName = parameterMapping.getProperty();
            final Object cachedValue = metaCachedParameter.getValue(parameterName);
            //给输入参数中的属性设置值
            metaParameter.setValue(parameterName, cachedValue);
          }
        }
      }
    }
  }

  /**查询数据库
   * @param ms                  MappedStatement
   * @param parameter           传入的参数
   * @param rowBounds           分页对象
   * @param resultHandler       结果处理器
   * @param key                 缓存key
   * @param boundSql            boundSql
   * @param <E>                 结果对象泛型
   * @return                    结果
   * @throws SQLException       异常
   */
  private <E> List<E> queryFromDatabase(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, CacheKey key, BoundSql boundSql) throws SQLException {
    List<E> list;
    //todo 为何先放入一个站位对象
    //在本地缓存中放入一个占位符对象
    localCache.putObject(key, EXECUTION_PLACEHOLDER);
    try {
      //调用doQuery
      list = doQuery(ms, parameter, rowBounds, resultHandler, boundSql);
    } finally {
      //从缓存中移除key
      localCache.removeObject(key);
    }
    //将查询结果放入缓存
    localCache.putObject(key, list);
    if (ms.getStatementType() == StatementType.CALLABLE) {
      //如果是callable类型的调用,处理输出参数缓存
      localOutputParameterCache.putObject(key, parameter);
    }
    return list;
  }

  /**获取日志增强的连接
   * @param statementLog          log对象
   * @return                      增强后的连接
   * @throws SQLException         异常
   */
  protected Connection getConnection(Log statementLog) throws SQLException {
    //从事务中获取连接对象
    Connection connection = transaction.getConnection();
    //如果开启了日志调试
    if (statementLog.isDebugEnabled()) {
      //构造一个使用ConnectionLogger动态代理的连接
      return ConnectionLogger.newInstance(connection, statementLog, queryStack);
    } else {
      return connection;
    }
  }

  /**设置Executor的包装Executor
   * @param wrapper               包装的executor
   */
  @Override
  public void setExecutorWrapper(Executor wrapper) {
    this.wrapper = wrapper;
  }

  private static class DeferredLoad {

    private final MetaObject resultObject;
    private final String property;
    private final Class<?> targetType;
    private final CacheKey key;
    private final PerpetualCache localCache;
    private final ObjectFactory objectFactory;
    private final ResultExtractor resultExtractor;

    // issue #781
    public DeferredLoad(MetaObject resultObject,
                        String property,
                        CacheKey key,
                        PerpetualCache localCache,
                        Configuration configuration,
                        Class<?> targetType) {
      this.resultObject = resultObject;
      this.property = property;
      this.key = key;
      this.localCache = localCache;
      this.objectFactory = configuration.getObjectFactory();
      this.resultExtractor = new ResultExtractor(configuration, objectFactory);
      this.targetType = targetType;
    }

    public boolean canLoad() {
      return localCache.getObject(key) != null && localCache.getObject(key) != EXECUTION_PLACEHOLDER;
    }

    public void load() {
      @SuppressWarnings("unchecked")
      // we suppose we get back a List
      List<Object> list = (List<Object>) localCache.getObject(key);
      Object value = resultExtractor.extractObjectFromList(list, targetType);
      resultObject.setValue(property, value);
    }

  }

}
