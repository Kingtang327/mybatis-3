/**
 *    Copyright 2009-2015 the original author or authors.
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

import org.apache.ibatis.cache.CacheKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

/**
 * @author Clinton Begin
 */
public interface Executor {

  ResultHandler NO_RESULT_HANDLER = null;

  /**执行更新语句
   * @param ms              MappedStatement
   * @param parameter       参数
   * @return                影响的行数
   * @throws SQLException   异常
   */
  int update(MappedStatement ms, Object parameter) throws SQLException;

  /**执行带结果返回的查询
   * @param ms              MappedStatement
   * @param parameter       参数
   * @param rowBounds       分页对象
   * @param resultHandler   结果处理器
   * @param cacheKey        缓存key
   * @param boundSql        boundSql
   * @param <E>             返回对象的泛型
   * @return                对象列表
   * @throws SQLException   异常
   */
  <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, CacheKey cacheKey, BoundSql boundSql) throws SQLException;

  /**执行带结果返回的查询
   * @param ms              MappedStatement
   * @param parameter       参数
   * @param rowBounds       分页对象
   * @param resultHandler   结果处理器
   * @param <E>             返回对象的泛型
   * @return                对象列表
   * @throws SQLException   异常
   */
  <E> List<E> query(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler) throws SQLException;

  /**查询游标
   * @param ms              MappedStatement
   * @param parameter       参数
   * @param rowBounds       分页对象
   * @param <E>             返回对象的泛型
   * @return                游标列表
   * @throws SQLException   异常
   */
  <E> Cursor<E> queryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds) throws SQLException;

  /**执行批量语句
   * @return                  批处理结果
   * @throws SQLException     异常
   */
  List<BatchResult> flushStatements() throws SQLException;

  /**提交事务
   * @param required          是否需要
   * @throws SQLException     异常
   */
  void commit(boolean required) throws SQLException;

  /**回滚事务
   * @param required          是否需求
   * @throws SQLException     异常
   */
  void rollback(boolean required) throws SQLException;

  /**创建一个缓存的key对象
   * @param ms                MappedStatement
   * @param parameterObject   参数对象
   * @param rowBounds         分页对象
   * @param boundSql          boundSql
   * @return                  计算出的缓存key
   */
  CacheKey createCacheKey(MappedStatement ms, Object parameterObject, RowBounds rowBounds, BoundSql boundSql);

  /**当前CacheKey是否有对应的缓存
   * @param ms        MappedStatement
   * @param key       CacheKey
   * @return          是否有对应的缓存
   */
  boolean isCached(MappedStatement ms, CacheKey key);

  /**
   * 清除本地缓存
   */
  void clearLocalCache();

  /**延迟加载
   * @param ms                  MappedStatement
   * @param resultObject        元数据对象
   * @param property            属性名
   * @param key                 缓存key
   * @param targetType          对象类型
   */
  void deferLoad(MappedStatement ms, MetaObject resultObject, String property, CacheKey key, Class<?> targetType);

  /**获取事务
   * @return                    事务
   */
  Transaction getTransaction();

  /**关闭executor
   * @param forceRollback       是否强制回滚
   */
  void close(boolean forceRollback);

  /**是否关闭
   * @return                    是否关闭
   */
  boolean isClosed();

  /**设置Executor的包装Executor
   * @param executor            包装的executor
   */
  void setExecutorWrapper(Executor executor);

}
