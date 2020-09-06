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
package org.apache.ibatis.session.defaults;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.exceptions.TooManyResultsException;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.result.DefaultMapResultHandler;
import org.apache.ibatis.executor.result.DefaultResultContext;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

/**
 * The default implementation for {@link SqlSession}.
 * Note that this class is not Thread-Safe.
 *
 * @author Clinton Begin
 */
public class DefaultSqlSession implements SqlSession {

  /**
   * 全局配置对象
   */
  private final Configuration configuration;
  /**
   * sql执行器
   */
  private final Executor executor;

  /**
   * 是否自动提交
   */
  private final boolean autoCommit;
  /**
   * 是否脏数据
   */
  private boolean dirty;
  /**
   * 游标列表
   */
  private List<Cursor<?>> cursorList;

  /**构造函数
   * @param configuration         核心配置对象
   * @param executor              sql执行器
   * @param autoCommit            是否自动提交
   */
  public DefaultSqlSession(Configuration configuration, Executor executor, boolean autoCommit) {
    this.configuration = configuration;
    this.executor = executor;
    this.dirty = false;
    this.autoCommit = autoCommit;
  }

  /**构造函数
   * @param configuration         核心配置对象
   * @param executor              sql执行器
   */
  public DefaultSqlSession(Configuration configuration, Executor executor) {
    this(configuration, executor, false);
  }

  /**查询单条数据并返回对象
   * @param statement             the statement
   * @param <T>                   返回对象类型
   * @return                      对象
   */
  @Override
  public <T> T selectOne(String statement) {
    return this.selectOne(statement, null);
  }

  /**查询单条数据并返回对象
   * @param statement             the statement
   * @param parameter             sql语句参数
   * @param <T>                   返回对象类型
   * @return                      对象
   */
  @Override
  public <T> T selectOne(String statement, Object parameter) {
    // Popular vote was to return null on 0 results and throw exception on too many.
    //调用selectList
    List<T> list = this.selectList(statement, parameter);
    if (list.size() == 1) {
      //结果为1条数据时,返回第0条
      return list.get(0);
    } else if (list.size() > 1) {
      //结果数大于1时,抛出异常
      throw new TooManyResultsException("Expected one result (or null) to be returned by selectOne(), but found: " + list.size());
    } else {
      //返回null
      return null;
    }
  }

  /**map查询
   * @param statement Unique identifier matching the statement to use.
   * @param mapKey    The property to use as key for each value in the list.
   * @param <K>       返回的map的key的泛型
   * @param <V>       返回的map的值的泛型
   * @return          返回map<K,V>
   */
  @Override
  public <K, V> Map<K, V> selectMap(String statement, String mapKey) {
    //调用selectMap
    return this.selectMap(statement, null, mapKey, RowBounds.DEFAULT);
  }

  /**map查询
   * @param statement Unique identifier matching the statement to use.
   * @param parameter A parameter object to pass to the statement.
   * @param mapKey    The property to use as key for each value in the list.
   * @param <K>       返回的map的key的泛型
   * @param <V>       返回的map的值的泛型
   * @return          返回map<K,V>
   */
  @Override
  public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey) {
    //调用selectMap
    return this.selectMap(statement, parameter, mapKey, RowBounds.DEFAULT);
  }

  /**map查询
   * @param statement Unique identifier matching the statement to use.
   * @param parameter A parameter object to pass to the statement.
   * @param mapKey    The property to use as key for each value in the list.
   * @param rowBounds Bounds to limit object retrieval
   * @param <K>       返回的map的key的泛型
   * @param <V>       返回的map的值的泛型
   * @return          返回map<K,V>
   */
  @Override
  public <K, V> Map<K, V> selectMap(String statement, Object parameter, String mapKey, RowBounds rowBounds) {
    //调用selectList获取结果
    final List<? extends V> list = selectList(statement, parameter, rowBounds);
    //初始化一个默认的Map结果处理器
    final DefaultMapResultHandler<K, V> mapResultHandler = new DefaultMapResultHandler<>(mapKey,
            configuration.getObjectFactory(), configuration.getObjectWrapperFactory(), configuration.getReflectorFactory());
    //初始化一个默认的结果上下文对象
    final DefaultResultContext<V> context = new DefaultResultContext<>();
    for (V o : list) {
      //遍历结果列表,放入context中
      context.nextResultObject(o);
      //使用Map结果处理器处理上下文
      mapResultHandler.handleResult(context);
    }
    //从结果处理器中获取处理完的map<K,V>返回
    return mapResultHandler.getMappedResults();
  }

  /**游标查询
   * @param statement Unique identifier matching the statement to use.
   * @param <T>       返回对象类型
   * @return          对象
   */
  @Override
  public <T> Cursor<T> selectCursor(String statement) {
    return selectCursor(statement, null);
  }

  /**游标查询
   * @param statement Unique identifier matching the statement to use.
   * @param parameter A parameter object to pass to the statement.
   * @param <T>       返回对象类型
   * @return          对象
   */
  @Override
  public <T> Cursor<T> selectCursor(String statement, Object parameter) {
    return selectCursor(statement, parameter, RowBounds.DEFAULT);
  }

  /**游标查询
   * @param statement Unique identifier matching the statement to use.
   * @param parameter A parameter object to pass to the statement.
   * @param rowBounds Bounds to limit object retrieval
   * @param <T>       返回对象类型
   * @return          对象
   */
  @Override
  public <T> Cursor<T> selectCursor(String statement, Object parameter, RowBounds rowBounds) {
    try {
      //使用全局配置对象中的mappedStatements获取MappedStatement对象
      MappedStatement ms = configuration.getMappedStatement(statement);
      //调用sql处理器查询
      Cursor<T> cursor = executor.queryCursor(ms, wrapCollection(parameter), rowBounds);
      //把游标注册到游标列表中,方便后续统一管理游标
      registerCursor(cursor);
      return cursor;
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error querying database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**查询列表
   * @param statement Unique identifier matching the statement to use.
   * @param <E>       结果的泛型
   * @return          结果列表
   */
  @Override
  public <E> List<E> selectList(String statement) {
    return this.selectList(statement, null);
  }

  /**查询列表
   * @param statement Unique identifier matching the statement to use.
   * @param parameter A parameter object to pass to the statement.
   * @param <E>       结果的泛型
   * @return          结果列表
   */
  @Override
  public <E> List<E> selectList(String statement, Object parameter) {
    //调用selectList
    return this.selectList(statement, parameter, RowBounds.DEFAULT);
  }

  /**查询列表
   * @param statement Unique identifier matching the statement to use.
   * @param parameter A parameter object to pass to the statement.
   * @param rowBounds Bounds to limit object retrieval
   * @param <E>       结果的泛型
   * @return          结果列表
   */
  @Override
  public <E> List<E> selectList(String statement, Object parameter, RowBounds rowBounds) {
    try {
      //使用全局配置对象中的mappedStatements获取MappedStatement对象
      MappedStatement ms = configuration.getMappedStatement(statement);
      //调用sql处理器查询
      return executor.query(ms, wrapCollection(parameter), rowBounds, Executor.NO_RESULT_HANDLER);
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error querying database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**带结果处理器的查询
   * @param statement Unique identifier matching the statement to use.
   * @param parameter A parameter object to pass to the statement.
   * @param handler   ResultHandler that will handle each retrieved row
   */
  @Override
  public void select(String statement, Object parameter, ResultHandler handler) {
    //调用select
    select(statement, parameter, RowBounds.DEFAULT, handler);
  }

  /**带结果处理器的查询
   * @param statement Unique identifier matching the statement to use.
   * @param handler   ResultHandler that will handle each retrieved row
   */
  @Override
  public void select(String statement, ResultHandler handler) {
    select(statement, null, RowBounds.DEFAULT, handler);
  }

  /**带结果处理器的查询
   * @param statement Unique identifier matching the statement to use.
   * @param parameter the parameter
   * @param rowBounds RowBound instance to limit the query results
   * @param handler
   */
  @Override
  public void select(String statement, Object parameter, RowBounds rowBounds, ResultHandler handler) {
    try {
      //使用全局配置对象中的mappedStatements获取MappedStatement对象
      MappedStatement ms = configuration.getMappedStatement(statement);
      //调用sql处理器查询
      executor.query(ms, wrapCollection(parameter), rowBounds, handler);
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error querying database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**插入数据
   * @param statement Unique identifier matching the statement to execute.
   * @return          影响的行数
   */
  @Override
  public int insert(String statement) {
    //调用insert
    return insert(statement, null);
  }

  /**插入数据
   * @param statement Unique identifier matching the statement to execute.
   * @param parameter A parameter object to pass to the statement.
   * @return          影响的行数
   */
  @Override
  public int insert(String statement, Object parameter) {
    //调用update
    return update(statement, parameter);
  }

  /**更新数据
   * @param statement Unique identifier matching the statement to execute.
   * @return          影响的行数
   */
  @Override
  public int update(String statement) {
    //调用update
    return update(statement, null);
  }

  /**更新数据
   * @param statement Unique identifier matching the statement to execute.
   * @param parameter A parameter object to pass to the statement.
   * @return          影响的行数
   */
  @Override
  public int update(String statement, Object parameter) {
    try {
      //设置脏数据状态为true
      dirty = true;
      //使用全局配置对象中的mappedStatements获取MappedStatement对象
      MappedStatement ms = configuration.getMappedStatement(statement);
      //调用sql处理器更新
      return executor.update(ms, wrapCollection(parameter));
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error updating database.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**删除数据
   * @param statement Unique identifier matching the statement to execute.
   * @return          影响的行数
   */
  @Override
  public int delete(String statement) {
    //调用update
    return update(statement, null);
  }

  /**删除数据
   * @param statement Unique identifier matching the statement to execute.
   * @param parameter A parameter object to pass to the statement.
   * @return          影响的行数
   */
  @Override
  public int delete(String statement, Object parameter) {
    //调用更新
    return update(statement, parameter);
  }

  /**
   * 提交事务
   */
  @Override
  public void commit() {
    commit(false);
  }

  /**提交事务
   * @param force     是否强制提交
   */
  @Override
  public void commit(boolean force) {
    try {
      //调用执行器提交事务
      executor.commit(isCommitOrRollbackRequired(force));
      //设置脏数据状态为false
      dirty = false;
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error committing transaction.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**
   * 回滚事务
   */
  @Override
  public void rollback() {
    rollback(false);
  }

  /**回滚事务
   * @param force forces connection rollback
   */
  @Override
  public void rollback(boolean force) {
    try {
      //调用执行器回滚事务
      executor.rollback(isCommitOrRollbackRequired(force));
      //设置脏数据状态为false
      dirty = false;
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error rolling back transaction.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**批量处理
   * @return        返回批量处理的结果
   */
  @Override
  public List<BatchResult> flushStatements() {
    try {
      //调用执行器进行批量操作
      return executor.flushStatements();
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error flushing statements.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**
   * 关闭sqlsession
   */
  @Override
  public void close() {
    try {
      //调用执行器进行关闭
      executor.close(isCommitOrRollbackRequired(false));
      //关闭所有游标
      closeCursors();
      //设置脏数据状态为false
      dirty = false;
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**
   * 关闭全部游标
   */
  private void closeCursors() {
    if (cursorList != null && !cursorList.isEmpty()) {
      for (Cursor<?> cursor : cursorList) {
        //遍历游标列表,进行关闭操作
        try {
          cursor.close();
        } catch (IOException e) {
          throw ExceptionFactory.wrapException("Error closing cursor.  Cause: " + e, e);
        }
      }
      //清空游标
      cursorList.clear();
    }
  }

  /**获取全局配置对象
   * @return        全局配置对象
   */
  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  /**获取Mapper<T>的动态代理类
   * @param type  Mapper interface class
   * @param <T>   返回对象的泛型
   * @return      Mapper接口的动态代理类
   */
  @Override
  public <T> T getMapper(Class<T> type) {
    //从核心配置对象中获取mapper, 实际是从mapperRegistry中获取mapper接口的代理工厂,使用代理工厂创建的动态代理类
    return configuration.getMapper(type, this);
  }

  /**获取数据库连接
   * @return      数据库连接
   */
  @Override
  public Connection getConnection() {
    try {
      //调用执行器获取连接
      return executor.getTransaction().getConnection();
    } catch (SQLException e) {
      throw ExceptionFactory.wrapException("Error getting a new connection.  Cause: " + e, e);
    }
  }

  /**
   * 清除缓存
   */
  @Override
  public void clearCache() {
    //调用执行器清除本地缓存
    executor.clearLocalCache();
  }

  /**注册游标
   * @param cursor      游标对象
   * @param <T>         泛型
   */
  private <T> void registerCursor(Cursor<T> cursor) {
    if (cursorList == null) {
      cursorList = new ArrayList<>();
    }
    //把游标加入到cursorList
    cursorList.add(cursor);
  }

  /**判断是否需求提交或者回滚
   * @param force         是否强制提交或回滚
   * @return              是否需求提交或者回滚
   */
  private boolean isCommitOrRollbackRequired(boolean force) {
    //如果不是自动提交并且有脏数据返回true
    //如果强制提交或回滚返回true
    return (!autoCommit && dirty) || force;
  }

  /**将Collection或者数组类型的参数转换成Map
   * @param object      参数对象
   * @return            包装后的对象
   */
  private Object wrapCollection(final Object object) {
    return ParamNameResolver.wrapToMapIfCollection(object, null);
  }

  /**
   * @deprecated Since 3.5.5
   */
  @Deprecated
  public static class StrictMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -5741767162221585340L;

    @Override
    public V get(Object key) {
      if (!super.containsKey(key)) {
        throw new BindingException("Parameter '" + key + "' not found. Available parameters are " + this.keySet());
      }
      return super.get(key);
    }

  }

}
