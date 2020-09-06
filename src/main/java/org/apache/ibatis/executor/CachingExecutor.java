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

import java.sql.SQLException;
import java.util.List;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cache.TransactionalCacheManager;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public class CachingExecutor implements Executor {

  /**
   * 执行代理, 实际crud的执行者
   */
  private final Executor delegate;
  /**
   * 事务缓存管理器
   */
  private final TransactionalCacheManager tcm = new TransactionalCacheManager();

  /**构造函数
   * @param delegate          代理对象, 为BaseExecutor的子类
   */
  public CachingExecutor(Executor delegate) {
    this.delegate = delegate;
    //设置代理类的包装为当期的cacheExecutor
    delegate.setExecutorWrapper(this);
  }

  /**获取事务
   * @return                    事务
   */
  @Override
  public Transaction getTransaction() {
    return delegate.getTransaction();
  }

  /**关闭executor
   * @param forceRollback       是否强制回滚
   */
  @Override
  public void close(boolean forceRollback) {
    try {
      // issues #499, #524 and #573
      if (forceRollback) {
        //如果需要强制回滚,则回滚
        tcm.rollback();
      } else {
        //否则提交
        tcm.commit();
      }
    } finally {
      //关闭代理执行器
      delegate.close(forceRollback);
    }
  }

  /**是否关闭
   * @return                    是否关闭
   */
  @Override
  public boolean isClosed() {
    //返回代理对象的关闭状态
    return delegate.isClosed();
  }

  /**执行更新语句
   * @param ms              MappedStatement
   * @param parameterObject 参数
   * @return                影响的行数
   * @throws SQLException   异常
   */
  @Override
  public int update(MappedStatement ms, Object parameterObject) throws SQLException {
    //根据配置清除缓存
    flushCacheIfRequired(ms);
    //调用代理类的更新
    return delegate.update(ms, parameterObject);
  }

  /**查询游标
   * @param ms              MappedStatement
   * @param parameter       参数
   * @param rowBounds       分页对象
   * @param <E>             返回对象的泛型
   * @return                游标列表
   * @throws SQLException   异常
   */
  @Override
  public <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException {
    //根据配置清除缓存
    flushCacheIfRequired(ms);
    //调用代理类的查询游标
    return delegate.queryCursor(ms, parameter, rowBounds);
  }

  /**执行带结果返回的查询
   * @param ms              MappedStatement
   * @param parameterObject 参数
   * @param rowBounds       分页对象
   * @param resultHandler   结果处理器
   * @param <E>             返回对象的泛型
   * @return                对象列表
   * @throws SQLException   异常
   */
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler) throws SQLException {
    //获取boundSql
    BoundSql boundSql = ms.getBoundSql(parameterObject);
    //计算缓存key
    CacheKey key = createCacheKey(ms, parameterObject, rowBounds, boundSql);
    //调用query
    return query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
  }

  /**执行带结果返回的查询
   * @param ms              MappedStatement
   * @param parameterObject 参数
   * @param rowBounds       分页对象
   * @param resultHandler   结果处理器
   * @param key             缓存key
   * @param boundSql        boundSql
   * @param <E>             返回对象的泛型
   * @return                对象列表
   * @throws SQLException   异常
   */
  @Override
  public <E> List<E> query(MappedStatement ms, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler, CacheKey key, BoundSql boundSql)
      throws SQLException {
    //从statement中获取缓存
    Cache cache = ms.getCache();
    if (cache != null) {
      //如果statement中配置了<cache>
      //根据配置清除缓存
      flushCacheIfRequired(ms);
      if (ms.isUseCache() && resultHandler == null) {
        //如果使用缓存,并且结果处理器为空
        //确保未使用输出参数
        ensureNoOutParams(ms, boundSql);
        @SuppressWarnings("unchecked")
        //获取缓存
        List<E> list = (List<E>) tcm.getObject(cache, key);
        if (list == null) {
          //缓存为空,调用代理类查询
          list = delegate.query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
          //查询后的对象放入缓存
          tcm.putObject(cache, key, list); // issue #578 and #116
        }
        return list;
      }
    }
    //未配置二级缓存,调用代理类查询并返回
    return delegate.query(ms, parameterObject, rowBounds, resultHandler, key, boundSql);
  }

  /**执行批量语句
   * @return                  批处理结果
   * @throws SQLException     异常
   */
  @Override
  public List<BatchResult> flushStatements() throws SQLException {
    return delegate.flushStatements();
  }

  /**提交事务
   * @param required          是否需要
   * @throws SQLException     异常
   */
  @Override
  public void commit(boolean required) throws SQLException {
    //提交代理类
    delegate.commit(required);
    //事务缓存管理器提交
    tcm.commit();
  }

  /**回滚事务
   * @param required          是否需求
   * @throws SQLException     异常
   */
  @Override
  public void rollback(boolean required) throws SQLException {
    try {
      //回滚代理类
      delegate.rollback(required);
    } finally {
      if (required) {
        //事务缓存管理器回滚
        tcm.rollback();
      }
    }
  }

  /**验证mapperStatement中没有输出类型的参数
   * @param ms                MappedStatement
   * @param boundSql          boundSql
   */
  private void ensureNoOutParams(MappedStatement ms, BoundSql boundSql) {
    if (ms.getStatementType() == StatementType.CALLABLE) {
      //判断callable类型的statement
      for (ParameterMapping parameterMapping : boundSql.getParameterMappings()) {
        //如果参数类型不为IN,抛出异常
        if (parameterMapping.getMode() != ParameterMode.IN) {
          throw new ExecutorException("Caching stored procedures with OUT params is not supported.  Please configure useCache=false in " + ms.getId() + " statement.");
        }
      }
    }
  }

  /**创建一个缓存的key对象
   * @param ms                MappedStatement
   * @param parameterObject   参数对象
   * @param rowBounds         分页对象
   * @param boundSql          boundSql
   * @return                  计算出的缓存key
   */
  @Override
  public CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql) {
    //调用代理类创建缓存key
    return delegate.createCacheKey(ms, parameterObject, rowBounds, boundSql);
  }

  /**当前CacheKey是否有对应的缓存
   * @param ms        MappedStatement
   * @param key       CacheKey
   * @return          是否有对应的缓存
   */
  @Override
  public boolean isCached(MappedStatement ms, CacheKey key) {
    //调用代理类判断是否缓存
    return delegate.isCached(ms, key);
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
    //调用代理类
    delegate.deferLoad(ms, resultObject, property, key, targetType);
  }

  /**
   * 清除本地缓存
   */
  @Override
  public void clearLocalCache() {
    //调用代理类
    delegate.clearLocalCache();
  }

  /**根据需要清空二级缓存
   * @param ms                 MappedStatement
   */
  private void flushCacheIfRequired(MappedStatement ms) {
    Cache cache = ms.getCache();
    //如果开启了二级缓存,并且配置了刷新缓存
    if (cache != null && ms.isFlushCacheRequired()) {
      //清除二级缓存
      tcm.clear(cache);
    }
  }

  /**设置Executor的包装Executor
   * @param executor            包装的executor
   */
  @Override
  public void setExecutorWrapper(Executor executor) {
    //CacheExecutor不支持设置包装器
    throw new UnsupportedOperationException("This method should not be called");
  }

}
