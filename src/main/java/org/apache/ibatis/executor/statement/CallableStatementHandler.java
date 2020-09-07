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
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.JdbcType;

import java.sql.*;
import java.util.List;

/**
 * @author Clinton Begin
 */
public class CallableStatementHandler extends BaseStatementHandler {

  /**构造函数
   * @param executor            sql执行器
   * @param mappedStatement     mappedStatement
   * @param parameter           参数
   * @param rowBounds           分页参数
   * @param resultHandler       结果处理器
   * @param boundSql            boundSql
   */
  public CallableStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameter, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
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
    //强转成存储过程Statement
    CallableStatement cs = (CallableStatement) statement;
    //调用执行
    cs.execute();
    //获取影响的行数
    int rows = cs.getUpdateCount();
    Object parameterObject = boundSql.getParameterObject();
    KeyGenerator keyGenerator = mappedStatement.getKeyGenerator();
    //后置处理主键返回
    keyGenerator.processAfter(executor, mappedStatement, cs, parameterObject);
    //处理输出参数
    resultSetHandler.handleOutputParameters(cs);
    //返回影响的行数
    return rows;
  }

  /**批处理
   * @param statement             statement
   * @throws SQLException         异常
   */
  @Override
  public void batch(Statement statement) throws SQLException {
    //强转成存储过程Statement
    CallableStatement cs = (CallableStatement) statement;
    //加入批处理
    cs.addBatch();
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
    //强转成存储过程Statement
    CallableStatement cs = (CallableStatement) statement;
    //调用执行
    cs.execute();
    //处理结果集
    List<E> resultList = resultSetHandler.handleResultSets(cs);
    //处理输出参数
    resultSetHandler.handleOutputParameters(cs);
    //返回数据
    return resultList;
  }

  /**查询游标
   * @param statement           statement
   * @param <E>                 返回对象泛型
   * @return                    返回游标
   * @throws SQLException       异常
   */
  @Override
  public <E> Cursor<E> queryCursor(Statement statement) throws SQLException {
    CallableStatement cs = (CallableStatement) statement;
    cs.execute();
    Cursor<E> resultList = resultSetHandler.handleCursorResultSets(cs);
    resultSetHandler.handleOutputParameters(cs);
    return resultList;
  }

  /**实例化一个statement
   * @param connection        数据库连接
   * @return                  statement
   * @throws SQLException     异常
   */
  @Override
  protected Statement instantiateStatement(Connection connection) throws SQLException {
    //获取sql语句
    String sql = boundSql.getSql();
    if (mappedStatement.getResultSetType() == ResultSetType.DEFAULT) {
      //如果是默认的结果集类型,调用prepareCall
      return connection.prepareCall(sql);
    } else {
      //根据指定的结果集类型,调用prepareCall
      return connection.prepareCall(sql, mappedStatement.getResultSetType().getValue(), ResultSet.CONCUR_READ_ONLY);
    }
  }

  /**参数化
   * @param statement             statement
   * @throws SQLException         异常
   */
  @Override
  public void parameterize(Statement statement) throws SQLException {
    //注册输出参数
    registerOutputParameters((CallableStatement) statement);
    //设置参数
    parameterHandler.setParameters((CallableStatement) statement);
  }

  /**注册输出参数
   * @param cs              CallableStatement
   * @throws SQLException   异常
   */
  private void registerOutputParameters(CallableStatement cs) throws SQLException {
    //获取参数映射对象
    List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
    for (int i = 0, n = parameterMappings.size(); i < n; i++) {
      //遍历
      ParameterMapping parameterMapping = parameterMappings.get(i);
      if (parameterMapping.getMode() == ParameterMode.OUT || parameterMapping.getMode() == ParameterMode.INOUT) {
        //处理OUT和INTOUT类型参数
        if (null == parameterMapping.getJdbcType()) {
          //输出参数必须指定jdbcType
          throw new ExecutorException("The JDBC Type must be specified for output parameter.  Parameter: " + parameterMapping.getProperty());
        } else {
          if (parameterMapping.getNumericScale() != null && (parameterMapping.getJdbcType() == JdbcType.NUMERIC || parameterMapping.getJdbcType() == JdbcType.DECIMAL)) {
            //对数字类型和Decimal类型的属性,如果指定了精度
            //注册有精度的参数
            cs.registerOutParameter(i + 1, parameterMapping.getJdbcType().TYPE_CODE, parameterMapping.getNumericScale());
          } else {
            if (parameterMapping.getJdbcTypeName() == null) {
              //未指定jdbc名称
              //注册参数
              cs.registerOutParameter(i + 1, parameterMapping.getJdbcType().TYPE_CODE);
            } else {
              //注册参数
              cs.registerOutParameter(i + 1, parameterMapping.getJdbcType().TYPE_CODE, parameterMapping.getJdbcTypeName());
            }
          }
        }
      }
    }
  }

}
