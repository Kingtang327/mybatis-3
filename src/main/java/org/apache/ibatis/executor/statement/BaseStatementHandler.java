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

import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.executor.ExecutorException;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Clinton Begin
 */
public abstract class BaseStatementHandler implements StatementHandler {

  /**
   * 核心配置对象
   */
  protected final Configuration configuration;
  /**
   * 对象工厂,用于对象的创建和赋值
   */
  protected final ObjectFactory objectFactory;
  /**
   * 类型处理器的注册器, 维护jdbcType和对应的处理器的关系
   */
  protected final TypeHandlerRegistry typeHandlerRegistry;
  /**
   * 结果集处理器
   */
  protected final ResultSetHandler resultSetHandler;
  /**
   * 参数处理器
   */
  protected final ParameterHandler parameterHandler;

  /**
   * sql执行器
   */
  protected final Executor executor;
  /**
   * MappedStatement
   */
  protected final MappedStatement mappedStatement;
  /**
   * 分页对象
   */
  protected final RowBounds rowBounds;

  /**
   * BoundSql
   */
  protected BoundSql boundSql;

  /**构造函数
   * @param executor            sql执行器
   * @param mappedStatement     mappedStatement
   * @param parameterObject     参数
   * @param rowBounds           分页对象
   * @param resultHandler       结果处理器
   * @param boundSql            BoundSql
   */
  protected BaseStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
    //设置属性值
    this.configuration = mappedStatement.getConfiguration();
    this.executor = executor;
    this.mappedStatement = mappedStatement;
    this.rowBounds = rowBounds;

    this.typeHandlerRegistry = configuration.getTypeHandlerRegistry();
    this.objectFactory = configuration.getObjectFactory();

    if (boundSql == null) { // issue #435, get the key before calculating the statement
      //如果boundSql为空,先处理主键生成
      generateKeys(parameterObject);
      //从mappedStatement中获取boundSql
      boundSql = mappedStatement.getBoundSql(parameterObject);
    }

    this.boundSql = boundSql;

    this.parameterHandler = configuration.newParameterHandler(mappedStatement, parameterObject, boundSql);
    this.resultSetHandler = configuration.newResultSetHandler(executor, mappedStatement, rowBounds, parameterHandler, resultHandler, boundSql);
  }

  /**获取BoundSql对象
   * @return                    返回BoundSql
   */
  @Override
  public BoundSql getBoundSql() {
    return boundSql;
  }

  /**获取参数处理器
   * @return                    参数处理器对象
   */
  @Override
  public ParameterHandler getParameterHandler() {
    return parameterHandler;
  }

  /**预处理Statement
   * @param connection            连接对象
   * @param transactionTimeout    超时时间
   * @return                      Statement
   * @throws SQLException         异常
   */
  @Override
  public Statement prepare(Connection connection, Integer transactionTimeout) throws SQLException {
    ErrorContext.instance().sql(boundSql.getSql());
    Statement statement = null;
    try {
      //实例化statement
      statement = instantiateStatement(connection);
      //设置超时时间
      setStatementTimeout(statement, transactionTimeout);
      //设置拉取数据的条数
      setFetchSize(statement);
      return statement;
    } catch (SQLException e) {
      //关闭statement
      closeStatement(statement);
      throw e;
    } catch (Exception e) {
      //关闭statement
      closeStatement(statement);
      throw new ExecutorException("Error preparing statement.  Cause: " + e, e);
    }
  }

  /**实例化一个statement
   * @param connection        数据库连接
   * @return                  statement
   * @throws SQLException     异常
   */
  protected abstract Statement instantiateStatement(Connection connection) throws SQLException;

  /**设置超时时间
   * @param stmt                stmt
   * @param transactionTimeout  超时时间
   * @throws SQLException       异常
   */
  protected void setStatementTimeout(Statement stmt, Integer transactionTimeout) throws SQLException {
    Integer queryTimeout = null;
    if (mappedStatement.getTimeout() != null) {
      //获取mappedStatement的超时时间
      queryTimeout = mappedStatement.getTimeout();
    } else if (configuration.getDefaultStatementTimeout() != null) {
      //获取默认的Statement的超时时间
      queryTimeout = configuration.getDefaultStatementTimeout();
    }
    if (queryTimeout != null) {
      //设置查询超时时间
      stmt.setQueryTimeout(queryTimeout);
    }
    //比较transactionTimeout和queryTimeout,设置queryTimeout
    StatementUtil.applyTransactionTimeout(stmt, queryTimeout, transactionTimeout);
  }

  /**设置拉取条数
   * @param stmt           Statement
   * @throws SQLException  异常
   */
  protected void setFetchSize(Statement stmt) throws SQLException {
    //获取mappedStatement中配置的fetchSize
    Integer fetchSize = mappedStatement.getFetchSize();
    if (fetchSize != null) {
      //不为空时设置fetchSize
      stmt.setFetchSize(fetchSize);
      return;
    }
    //获取默认的拉取条数
    Integer defaultFetchSize = configuration.getDefaultFetchSize();
    if (defaultFetchSize != null) {
      //不为空时设置fetchSzie
      stmt.setFetchSize(defaultFetchSize);
    }
  }

  /**关闭statement
   * @param statement    statement
   */
  protected void closeStatement(Statement statement) {
    try {
      if (statement != null) {
        statement.close();
      }
    } catch (SQLException e) {
      //ignore
    }
  }

  /**处理主键生成
   * @param parameter       sql参数
   */
  protected void generateKeys(Object parameter) {
    KeyGenerator keyGenerator = mappedStatement.getKeyGenerator();
    ErrorContext.instance().store();
    //处理主键返回问题
    keyGenerator.processBefore(executor, mappedStatement, null, parameter);
    ErrorContext.instance().recall();
  }

}
