/**
 *    Copyright 2009-2018 the original author or authors.
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
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * @author Clinton Begin
 */
public class SimpleStatementHandler extends BaseStatementHandler {

  /**构造函数
   * @param executor              sql执行器
   * @param mappedStatement       mappedStatement
   * @param parameter             参数
   * @param rowBounds             分页参数
   * @param resultHandler         结果处理器
   * @param boundSql              boundSql
   */
  public SimpleStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
    //调用父类构造函数
    super(executor, mappedStatement, parameter, rowBounds, resultHandler, boundSql);
  }

  /**更新
   * @param statement             statement
   * @return                      影响的行数
   * @throws SQLException         异常
   */
  @Override
  public int update(Statement statement) throws SQLException {
    //获取sql
    String sql = boundSql.getSql();
    //参数对象
    Object parameterObject = boundSql.getParameterObject();
    //注解生成器
    KeyGenerator keyGenerator = mappedStatement.getKeyGenerator();
    int rows;
    if (keyGenerator instanceof Jdbc3KeyGenerator) {
      //执行sql,并且返回自动生成的主键
      statement.execute(sql, Statement.RETURN_GENERATED_KEYS);
      //获取影响的行数
      rows = statement.getUpdateCount();
      //后置处理主键
      keyGenerator.processAfter(executor, mappedStatement, statement, parameterObject);
    } else if (keyGenerator instanceof SelectKeyGenerator) {
      //执行sql
      statement.execute(sql);
      //获取影响的行数
      rows = statement.getUpdateCount();
      //后置处理主键
      keyGenerator.processAfter(executor, mappedStatement, statement, parameterObject);
    } else {
      //不处理主键
      //执行sql
      statement.execute(sql);
      //获取影响的行数
      rows = statement.getUpdateCount();
    }
    //返回影响的行数
    return rows;
  }

  /**批处理
   * @param statement             statement
   * @throws SQLException         异常
   */
  @Override
  public void batch(Statement statement) throws SQLException {
    //获取sql语句
    String sql = boundSql.getSql();
    //加入到批处理
    statement.addBatch(sql);
  }

  /**查询
   * @param statement             statement
   * @param resultHandler         结果处理器
   * @param <E>                   返回对象泛型
   * @return                      返回对象集合
   * @throws SQLException         异常
   */
  @Override
  public <E> List<E> query(Statement statement, ResultHandler resultHandler) throws SQLException {
    //获取sql
    String sql = boundSql.getSql();
    //执行sql
    statement.execute(sql);
    //处理结果集并返回
    return resultSetHandler.handleResultSets(statement);
  }

  /**查询游标
   * @param statement             statement
   * @param <E>                   返回对象泛型
   * @return                      返回游标
   * @throws SQLException         异常
   */
  @Override
  public <E> Cursor<E> queryCursor(Statement statement) throws SQLException {
    //获取sql
    String sql = boundSql.getSql();
    //执行sql
    statement.execute(sql);
    //处理游标结果集并返回
    return resultSetHandler.handleCursorResultSets(statement);
  }

  /**实例化一个statement
   * @param connection        数据库连接
   * @return                  statement
   * @throws SQLException     异常
   */
  @Override
  protected Statement instantiateStatement(Connection connection) throws SQLException {
    if (mappedStatement.getResultSetType() == ResultSetType.DEFAULT) {
      //创建statement
      //这里的createStatement方法会被代理增强, 增加日志功能
      return connection.createStatement();
    } else {
      //根据结果集类型,创建statement
      //这里的createStatement方法会被代理增强, 增加日志功能
      return connection.createStatement(mappedStatement.getResultSetType().getValue(), ResultSet.CONCUR_READ_ONLY);
    }
  }

  /**参数化
   * @param statement             statement
   * @throws SQLException         异常
   */
  @Override
  public void parameterize(Statement statement) {
    //简单类型的sql不支持参数处理
    // N/A
  }

}
