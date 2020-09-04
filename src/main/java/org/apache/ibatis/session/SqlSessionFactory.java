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
package org.apache.ibatis.session;

import java.sql.Connection;

/**
 * Creates an {@link SqlSession} out of a connection or a DataSource
 *
 * @author Clinton Begin
 */
public interface SqlSessionFactory {

  /**获取一个默认配置的SqlSession
   * @return              默认配置的SqlSession
   */
  SqlSession openSession();

  /**获取一个指定事务提交方式的SqlSession
   * @param autoCommit    事务提交方式, true,false 自动或手动
   * @return              指定事务提交方式的SqlSession
   */
  SqlSession openSession(boolean autoCommit);

  /**获取一个给定数据库连接的SqlSession
   * @param connection    数据库连接
   * @return              指定数据库连接的SqlSession
   */
  SqlSession openSession(Connection connection);

  /**获取一个给定事务隔离级别的SqlSession
   * @param level         事务隔离级别
   * @return              给定事务隔离级别的SqlSession
   */
  SqlSession openSession(TransactionIsolationLevel level);

  /**获取一个给定执行器类型的SqlSession
   * @param execType      DefaultSqlSession中维护的一个执行器的类型
   * @return              给定执行器类型的SqlSession
   */
  SqlSession openSession(ExecutorType execType);

  /**获取一个给定执行器类型和事务提交方式的SqlSession
   * @param execType      DefaultSqlSession中维护的一个执行器的类型
   * @param autoCommit    事务提交方式, true,false 自动或手动
   * @return              给定执行器类型和事务提交方式的SqlSession
   */
  SqlSession openSession(ExecutorType execType, boolean autoCommit);

  /**获取一个给定执行器类型和事务隔离级别的SqlSession
   * @param execType      DefaultSqlSession中维护的一个执行器的类型
   * @param level         事务隔离级别
   * @return              给定执行器类型和事务隔离级别的SqlSession
   */
  SqlSession openSession(ExecutorType execType, TransactionIsolationLevel level);

  /**获取一个给定执行器类型和数据库连接的SqlSession
   * @param execType      DefaultSqlSession中维护的一个执行器的类型
   * @param connection    数据库连接
   * @return              给定执行器类型和数据库连接的SqlSession
   */
  SqlSession openSession(ExecutorType execType, Connection connection);

  /**获取核心配置文件对象
   * @return              核心配置文件对象
   */
  Configuration getConfiguration();

}
