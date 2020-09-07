/**
 *    Copyright 2009-2019 the original author or authors.
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
package org.apache.ibatis.executor.statement;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author Clinton Begin
 */
public class RoutingStatementHandler implements StatementHandler {

  /**
   * 实际处理Statement的代理对象
   */
  private final StatementHandler delegate;

  /**构造函数
   * @param executor        sql执行器
   * @param ms              MappedStatement
   * @param parameter       参数
   * @param rowBounds       分页信息
   * @param resultHandler   结果处理器
   * @param boundSql        boundsql
   */
  public RoutingStatementHandler(Executor executor, MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
    //根据Statement类型,创建不同的代理处理器
    switch (ms.getStatementType()) {
      case STATEMENT:
        //创建简单类型的处理器对象
        delegate = new SimpleStatementHandler(executor, ms, parameter, rowBounds, resultHandler, boundSql);
        break;
      case PREPARED:
        //创建预处理型的处理器
        delegate = new PreparedStatementHandler(executor, ms, parameter, rowBounds, resultHandler, boundSql);
        break;
      case CALLABLE:
        //创建存储过程类型的处理器
        delegate = new CallableStatementHandler(executor, ms, parameter, rowBounds, resultHandler, boundSql);
        break;
      default:
        throw new ExecutorException("Unknown statement type: " + ms.getStatementType());
    }

  }

  /**预处理Statement
   * @param connection            连接对象
   * @param transactionTimeout    超时时间
   * @return                      Statement
   * @throws SQLException         异常
   */
  @Override
  public Statement prepare(Connection connection, Integer transactionTimeout) throws SQLException {
    //调用内部的代理进行预处理Statement
    return delegate.prepare(connection, transactionTimeout);
  }

  /**参数化
   * @param statement             statement
   * @throws SQLException         异常
   */
  @Override
  public void parameterize(Statement statement) throws SQLException {
    //调用内部的代理进行参数化处理
    delegate.parameterize(statement);
  }

  /**批处理
   * @param statement             statement
   * @throws SQLException         异常
   */
  @Override
  public void batch(Statement statement) throws SQLException {
    //调用内部的代理进行批处理
    delegate.batch(statement);
  }

  /**更新
   * @param statement             statement
   * @return                      影响的行数
   * @throws SQLException         异常
   */
  @Override
  public int update(Statement statement) throws SQLException {
    //调用内部的代理进行更新
    return delegate.update(statement);
  }

  /**查询
   * @param statement           statement
   * @param resultHandler       结果处理器
   * @param <E>                 返回对象泛型
   * @return                    返回对象集合
   * @throws SQLException       异常
   */
  @Override
  public <E> List<E> query(Statement statement, ResultHandler resultHandler) throws SQLException {
    //调用内部的代理进行查询
    return delegate.query(statement, resultHandler);
  }

  /**查询游标
   * @param statement           statement
   * @param <E>                 返回对象泛型
   * @return                    返回游标
   * @throws SQLException       异常
   */
  @Override
  public <E> Cursor<E> queryCursor(Statement statement) throws SQLException {
    //调用内部的代理进行游标查询
    return delegate.queryCursor(statement);
  }

  /**获取BoundSql对象
   * @return                    返回BoundSql
   */
  @Override
  public BoundSql getBoundSql() {
    //调用内部的代理进行获取BoundSql对象
    return delegate.getBoundSql();
  }

  /**获取参数处理器
   * @return                    参数处理器对象
   */
  @Override
  public ParameterHandler getParameterHandler() {
    //调用内部的代理进行获取参数处理器
    return delegate.getParameterHandler();
  }
}
