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

import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.*;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Clinton Begin
 */
public class DefaultSqlSessionFactory implements SqlSessionFactory {

  /**
   * mybatis核心配置对象
   */
  private final Configuration configuration;

  /**构造函数
   * @param configuration     mybatis核心配置对象
   */
  public DefaultSqlSessionFactory(Configuration configuration) {
    this.configuration = configuration;
  }

  /**获取一个默认配置的SqlSession
   * @return              默认配置的SqlSession
   */
  @Override
  public SqlSession openSession() {
    //调用内部方法获取SqlSession
    return openSessionFromDataSource(configuration.getDefaultExecutorType(), null, false);
  }

  /**获取一个指定事务提交方式的SqlSession
   * @param autoCommit    事务提交方式, true,false 自动或手动
   * @return              指定事务提交方式的SqlSession
   */
  @Override
  public SqlSession openSession(boolean autoCommit) {
    //调用内部方法获取SqlSession
    return openSessionFromDataSource(configuration.getDefaultExecutorType(), null, autoCommit);
  }

  /**获取一个给定执行器类型的SqlSession
   * @param execType      DefaultSqlSession中维护的一个执行器的类型
   * @return              给定执行器类型的SqlSession
   */
  @Override
  public SqlSession openSession(ExecutorType execType) {
    //调用内部方法获取SqlSession
    return openSessionFromDataSource(execType, null, false);
  }

  /**获取一个给定事务隔离级别的SqlSession
   * @param level         事务隔离级别
   * @return              给定事务隔离级别的SqlSession
   */
  @Override
  public SqlSession openSession(TransactionIsolationLevel level) {
    //调用内部方法获取SqlSession
    return openSessionFromDataSource(configuration.getDefaultExecutorType(), level, false);
  }

  /**获取一个给定执行器类型和事务隔离级别的SqlSession
   * @param execType      DefaultSqlSession中维护的一个执行器的类型
   * @param level         事务隔离级别
   * @return              给定执行器类型和事务隔离级别的SqlSession
   */
  @Override
  public SqlSession openSession(ExecutorType execType, TransactionIsolationLevel level) {
    //调用内部方法获取SqlSession
    return openSessionFromDataSource(execType, level, false);
  }

  /**获取一个给定执行器类型和事务提交方式的SqlSession
   * @param execType      DefaultSqlSession中维护的一个执行器的类型
   * @param autoCommit    事务提交方式, true,false 自动或手动
   * @return              给定执行器类型和事务提交方式的SqlSession
   */
  @Override
  public SqlSession openSession(ExecutorType execType, boolean autoCommit) {
    //调用内部方法获取SqlSession
    return openSessionFromDataSource(execType, null, autoCommit);
  }

  /**获取一个给定数据库连接的SqlSession
   * @param connection    数据库连接
   * @return              指定数据库连接的SqlSession
   */
  @Override
  public SqlSession openSession(Connection connection) {
    //调用内部方法获取SqlSession
    return openSessionFromConnection(configuration.getDefaultExecutorType(), connection);
  }

  /**获取一个给定执行器类型和数据库连接的SqlSession
   * @param execType      DefaultSqlSession中维护的一个执行器的类型
   * @param connection    数据库连接
   * @return              给定执行器类型和数据库连接的SqlSession
   */
  @Override
  public SqlSession openSession(ExecutorType execType, Connection connection) {
    //调用内部方法获取SqlSession
    return openSessionFromConnection(execType, connection);
  }

  /**获取核心配置文件对象
   * @return              核心配置文件对象
   */
  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  /**从数据源中获取SqlSession
   * @param execType      执行器的类型
   * @param level         事务隔离级别
   * @param autoCommit    事务提交方式, true,false 自动或手动
   * @return              DefaultSqlSession
   */
  private SqlSession openSessionFromDataSource(ExecutorType execType, TransactionIsolationLevel level, boolean autoCommit) {
    Transaction tx = null;
    try {
      //读取环境参数
      final Environment environment = configuration.getEnvironment();
      //获取事务工厂
      final TransactionFactory transactionFactory = getTransactionFactoryFromEnvironment(environment);
      //使用事务工厂创建事务
      tx = transactionFactory.newTransaction(environment.getDataSource(), level, autoCommit);
      //创建一个sql执行器
      final Executor executor = configuration.newExecutor(tx, execType);
      //构建一个DefaultSqlSession
      return new DefaultSqlSession(configuration, executor, autoCommit);
    } catch (Exception e) {
      //如果失败,关闭事务
      closeTransaction(tx); // may have fetched a connection so lets call close()
      throw ExceptionFactory.wrapException("Error opening session.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**从连接中获取SqlSession
   * @param execType      执行器的类型
   * @param connection    数据库连接
   * @return              DefaultSqlSession
   */
  private SqlSession openSessionFromConnection(ExecutorType execType, Connection connection) {
    try {
      boolean autoCommit;
      try {
        //从连接中获取事务提交类型
        autoCommit = connection.getAutoCommit();
      } catch (SQLException e) {
        // Failover to true, as most poor drivers
        // or databases won't support transactions
        //默认为true, 有的数据库或驱动不支持事务
        autoCommit = true;
      }
      //读取环境参数
      final Environment environment = configuration.getEnvironment();
      //获取事务工厂
      final TransactionFactory transactionFactory = getTransactionFactoryFromEnvironment(environment);
      //使用事务工厂创建事务
      final Transaction tx = transactionFactory.newTransaction(connection);
      //创建一个sql执行器
      final Executor executor = configuration.newExecutor(tx, execType);
      //构建一个DefaultSqlSession
      return new DefaultSqlSession(configuration, executor, autoCommit);
    } catch (Exception e) {
      throw ExceptionFactory.wrapException("Error opening session.  Cause: " + e, e);
    } finally {
      ErrorContext.instance().reset();
    }
  }

  /**从环境中获取事务工厂
   * @param environment   环境对象
   * @return              事务工厂
   */
  private TransactionFactory getTransactionFactoryFromEnvironment(Environment environment) {
    if (environment == null || environment.getTransactionFactory() == null) {
      //环境为空,或者环境中的事务工厂为空
      //返回一个ManagedTransactionFactory
      return new ManagedTransactionFactory();
    }
    //返回环境中配置的事务工厂
    return environment.getTransactionFactory();
  }

  /**关闭事务
   * @param tx            事务对象
   */
  private void closeTransaction(Transaction tx) {
    if (tx != null) {
      try {
        //关闭事务
        tx.close();
      } catch (SQLException ignore) {
        // Intentionally ignore. Prefer previous error.
      }
    }
  }

}
