/**
 *    Copyright 2009-2016 the original author or authors.
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
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.session.ResultHandler;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author Clinton Begin
 */
public interface StatementHandler {

  /**预处理Statement
   * @param connection            连接对象
   * @param transactionTimeout    超时时间
   * @return                      Statement
   * @throws SQLException         异常
   */
  Statement prepare(Connection connection, Integer transactionTimeout)
      throws SQLException;

  /**参数化
   * @param statement             statement
   * @throws SQLException         异常
   */
  void parameterize(Statement statement)
      throws SQLException;

  /**批处理
   * @param statement             statement
   * @throws SQLException         异常
   */
  void batch(Statement statement)
      throws SQLException;

  /**更新
   * @param statement             statement
   * @return                      影响的行数
   * @throws SQLException         异常
   */
  int update(Statement statement)
      throws SQLException;

  /**查询
   * @param statement             statement
   * @param resultHandler         结果处理器
   * @param <E>                   返回对象泛型
   * @return                      返回对象集合
   * @throws SQLException         异常
   */
  <E> List<E> query(Statement statement, ResultHandler resultHandler)
      throws SQLException;

  /**查询游标
   * @param statement             statement
   * @param <E>                   返回对象泛型
   * @return                      返回游标
   * @throws SQLException         异常
   */
  <E> Cursor<E> queryCursor(Statement statement)
      throws SQLException;

  /**获取BoundSql对象
   * @return                      返回BoundSql
   */
  BoundSql getBoundSql();

  /**获取参数处理器
   * @return                      参数处理器对象
   */
  ParameterHandler getParameterHandler();

}
