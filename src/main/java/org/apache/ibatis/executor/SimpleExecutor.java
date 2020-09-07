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
package org.apache.ibatis.executor;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

/**
 * @author Clinton Begin
 */
public class SimpleExecutor extends BaseExecutor {

  /**构造函数
   * @param configuration       sql执行器
   * @param transaction         事务
   */
  public SimpleExecutor(Configuration configuration, Transaction transaction) {
    //调用父类的构造方法
    super(configuration, transaction);
  }

  /**执行更新
   * @param ms              MappedStatement
   * @param parameter       更新参数
   * @return                更新的条数
   * @throws SQLException   异常
   */
  @Override
  public int doUpdate(MappedStatement ms, Object parameter) throws SQLException {
    Statement stmt = null;
    try {
      //获取配置对象
      Configuration configuration = ms.getConfiguration();
      //构建一个RoutingStatementHandler
      StatementHandler handler = configuration.newStatementHandler(this, ms, parameter, RowBounds.DEFAULT, null, null);
      //预处理statement,获取一个日志增强的statement
      stmt = prepareStatement(handler, ms.getStatementLog());
      //调用handler进行更新
      return handler.update(stmt);
    } finally {
      //关闭statement
      closeStatement(stmt);
    }
  }

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
  @Override
  public <E> List<E> doQuery(MappedStatement ms, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) throws SQLException {
    Statement stmt = null;
    try {
      //获取配置对象
      Configuration configuration = ms.getConfiguration();
      //构建一个RoutingStatementHandler
      StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, resultHandler, boundSql);
      //预处理statement,获取一个日志增强的statement
      stmt = prepareStatement(handler, ms.getStatementLog());
      //调用handler进行查询
      return handler.query(stmt, resultHandler);
    } finally {
      //关闭statement
      closeStatement(stmt);
    }
  }

  /**执行游标查询
   * @param ms                  MappedStatement
   * @param parameter           参数
   * @param rowBounds           分页对象
   * @param boundSql            boundSql
   * @param <E>                 结果对象泛型
   * @return                    结果
   * @throws SQLException       异常
   */
  @Override
  protected <E> Cursor<E> doQueryCursor(MappedStatement ms, Object parameter, RowBounds rowBounds, BoundSql boundSql) throws SQLException {
    //获取配置对象
    Configuration configuration = ms.getConfiguration();
    //构建一个RoutingStatementHandler
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, null, boundSql);
    //预处理statement,获取一个日志增强的statement
    Statement stmt = prepareStatement(handler, ms.getStatementLog());
    //调用handler进行查询
    Cursor<E> cursor = handler.queryCursor(stmt);
    //设置statement在完成时关闭
    stmt.closeOnCompletion();
    return cursor;
  }

  /**执行批处理
   * @param isRollback          是否需要回滚
   * @return                    批处理结果
   * @throws SQLException       异常
   */
  @Override
  public List<BatchResult> doFlushStatements(boolean isRollback) {
    //simpleExecutor不支持批量执行
    //执行返回一个空集合
    return Collections.emptyList();
  }

  /**预处理statement
   * @param handler             StatementHandler
   * @param statementLog        statementLog
   * @return                    增强后的statement
   * @throws SQLException       异常
   */
  private Statement prepareStatement(StatementHandler handler, Log statementLog) throws SQLException {
    Statement stmt;
    //获取一个日志增强的connection
    Connection connection = getConnection(statementLog);
    //connection的增强类ConnectionLogger会在执行prepareStatement,prepareCall,createStatement
    //方法时进行增强,并返回一个日志增强型Statement
    //准备一个Statement
    stmt = handler.prepare(connection, transaction.getTimeout());
    //参数处理
    handler.parameterize(stmt);
    return stmt;
  }

}
