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
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.sql.*;
import java.util.List;

/**
 * @author Clinton Begin
 */
public class PreparedStatementHandler extends BaseStatementHandler {

  /**构造函数
   * @param executor            sql执行器
   * @param mappedStatement     mappedStatement
   * @param parameter           参数
   * @param rowBounds           分页参数
   * @param resultHandler       结果处理器
   * @param boundSql            boundSql
   */
  public PreparedStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
    //调用父类构造方法
    super(executor, mappedStatement, parameter, rowBounds, resultHandler, boundSql);
  }

  /**更新
   * @param statement             statement
   * @return                      影响的行数
   * @throws SQLException         异常
   */
  @Override
  public int update(Statement statement) throws SQLException {
    //强转为PreparedStatement
    PreparedStatement ps = (PreparedStatement) statement;
    //执行sql
    ps.execute();
    //获取影响的行数
    int rows = ps.getUpdateCount();
    //获取参数
    Object parameterObject = boundSql.getParameterObject();
    //获取主键生成器
    KeyGenerator keyGenerator = mappedStatement.getKeyGenerator();
    //后置处理主键
    keyGenerator.processAfter(executor, mappedStatement, ps, parameterObject);
    return rows;
  }

  /**批处理
   * @param statement             statement
   * @throws SQLException         异常
   */
  @Override
  public void batch(Statement statement) throws SQLException {
    //强转为PreparedStatement
    PreparedStatement ps = (PreparedStatement) statement;
    //加入到批处理
    ps.addBatch();
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
    //强转为PreparedStatement
    PreparedStatement ps = (PreparedStatement) statement;
    //执行sql
    ps.execute();
    //处理结果集并返回
    return resultSetHandler.handleResultSets(ps);
  }

  /**查询游标
   * @param statement             statement
   * @param <E>                   返回对象泛型
   * @return                      返回游标
   * @throws SQLException         异常
   */
  @Override
  public <E> Cursor<E> queryCursor(Statement statement) throws SQLException {
    //强转为PreparedStatement
    PreparedStatement ps = (PreparedStatement) statement;
    //执行sql
    ps.execute();
    //处理游标结果集并返回
    return resultSetHandler.handleCursorResultSets(ps);
  }

  /**实例化一个statement
   * @param connection        数据库连接
   * @return                  statement
   * @throws SQLException     异常
   */
  @Override
  protected Statement instantiateStatement(Connection connection) throws SQLException {
    String sql = boundSql.getSql();
    if (mappedStatement.getKeyGenerator() instanceof Jdbc3KeyGenerator) {
      //如果指定的keyGenerator为Jdbc3KeyGenerator
      //获取指定主键的列表
      String[] keyColumnNames = mappedStatement.getKeyColumns();
      if (keyColumnNames == null) {
        //为指定主键列,使用自动生成的主键
        //这里的prepareStatement方法会被代理增强, 增加日志功能
        return connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
      } else {
        //使用指定主键列的方式预处理Statement
        //这里的prepareStatement方法会被代理增强, 增加日志功能
        return connection.prepareStatement(sql, keyColumnNames);
      }
    } else if (mappedStatement.getResultSetType() == ResultSetType.DEFAULT) {
      //根据结果集类型,预编译statement
      //这里的prepareStatement方法会被代理增强, 增加日志功能
      return connection.prepareStatement(sql);
    } else {
      //根据结果集类型,预编译statement
      //这里的prepareStatement方法会被代理增强, 增加日志功能
      return connection.prepareStatement(sql, mappedStatement.getResultSetType().getValue(), ResultSet.CONCUR_READ_ONLY);
    }
  }

  /**参数化
   * @param statement             statement
   * @throws SQLException         异常
   */
  @Override
  public void parameterize(Statement statement) throws SQLException {
    //使用参数处理器处理参数
    parameterHandler.setParameters((PreparedStatement) statement);
  }

}
