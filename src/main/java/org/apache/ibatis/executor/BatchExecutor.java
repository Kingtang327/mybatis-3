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
package org.apache.ibatis.executor;

import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.transaction.Transaction;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Jeff Butler
 */
public class BatchExecutor extends BaseExecutor {

  public static final int BATCH_UPDATE_RETURN_VALUE = Integer.MIN_VALUE + 1002;

  /**
   * Statement列表
   */
  private final List<Statement> statementList = new ArrayList<>();
  /**
   * 批处理结果列表
   */
  private final List<BatchResult> batchResultList = new ArrayList<>();
  /**
   * 当前sql
   */
  private String currentSql;
  /**
   * 当前的MappedStatement
   */
  private MappedStatement currentStatement;

  /**构造函数
   * @param configuration       sql执行器
   * @param transaction         事务
   */
  public BatchExecutor(Configuration configuration, Transaction transaction) {
    //调用父类的构造方法
    super(configuration, transaction);
  }

  /**执行更新
   * @param ms              MappedStatement
   * @param parameterObject 更新参数
   * @return                更新的条数
   * @throws SQLException   异常
   */
  @Override
  public int doUpdate(MappedStatement ms, Object parameterObject) throws SQLException {
    //获取配置对象
    final Configuration configuration = ms.getConfiguration();
    //构建一个RoutingStatementHandler
    final StatementHandler handler = configuration.newStatementHandler(this, ms, parameterObject, RowBounds.DEFAULT, null, null);
    //获取sql语句
    final BoundSql boundSql = handler.getBoundSql();
    final String sql = boundSql.getSql();
    final Statement stmt;
    if (sql.equals(currentSql) && ms.equals(currentStatement)) {
      //如果当前sql和当前sql,并且MappedStatement是当前MappedStatement
      int last = statementList.size() - 1;
      //获取最后一个statement
      stmt = statementList.get(last);
      //设置超时时间
      applyTransactionTimeout(stmt);
      //处理参数
      handler.parameterize(stmt);// fix Issues 322
      //从批处理结果列表中取出批处理结果对象
      BatchResult batchResult = batchResultList.get(last);
      //添加参数到批处理结果
      batchResult.addParameterObject(parameterObject);
    } else {
      //获取一个日志增强的connection
      Connection connection = getConnection(ms.getStatementLog());
      //connection的增强类ConnectionLogger会在执行prepareStatement,prepareCall,createStatement
      //方法时进行增强,并返回一个日志增强型Statement
      //准备一个Statement
      stmt = handler.prepare(connection, transaction.getTimeout());
      //参数处理
      handler.parameterize(stmt);    // fix Issues 322
      //设置当前sql
      currentSql = sql;
      //设置当前MappedStatement
      currentStatement = ms;
      //添加到statementList
      statementList.add(stmt);
      //添加批处理结果对象到batchResultList
      batchResultList.add(new BatchResult(ms, sql, parameterObject));
    }
    //调用handler进行批量
    handler.batch(stmt);
    return BATCH_UPDATE_RETURN_VALUE;
  }

  /**执行query
   * @param ms                  MappedStatement
   * @param parameterObject     参数
   * @param rowBounds           分页对象
   * @param resultHandler       结果处理器
   * @param boundSql            boundSql
   * @param <E>                 结果对象泛型
   * @return                    结果
   * @throws SQLException       异常
   */
  @Override
  public <E> List<E> doQuery(MappedStatement ms, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql)
      throws SQLException {
    Statement stmt = null;
    try {
      //执行批处理
      flushStatements();
      //获取配置对象
      Configuration configuration = ms.getConfiguration();
      //构建一个RoutingStatementHandler
      StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameterObject, rowBounds, resultHandler, boundSql);
      //获取一个日志增强的connection
      Connection connection = getConnection(ms.getStatementLog());
      //connection的增强类ConnectionLogger会在执行prepareStatement,prepareCall,createStatement
      //方法时进行增强,并返回一个日志增强型Statement
      //准备一个Statement
      stmt = handler.prepare(connection, transaction.getTimeout());
      //参数处理
      handler.parameterize(stmt);
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
    //执行批处理
    flushStatements();
    //获取配置对象
    Configuration configuration = ms.getConfiguration();
    //构建一个RoutingStatementHandler
    StatementHandler handler = configuration.newStatementHandler(wrapper, ms, parameter, rowBounds, null, boundSql);
    //获取一个日志增强的connection
    Connection connection = getConnection(ms.getStatementLog());
    //connection的增强类ConnectionLogger会在执行prepareStatement,prepareCall,createStatement
    //方法时进行增强,并返回一个日志增强型Statement
    //准备一个Statement
    Statement stmt = handler.prepare(connection, transaction.getTimeout());
    //参数处理
    handler.parameterize(stmt);
    //查询游标
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
  public List<BatchResult> doFlushStatements(boolean isRollback) throws SQLException {
    try {
      List<BatchResult> results = new ArrayList<>();
      if (isRollback) {
        //如果需要回滚,直接返回空集合
        return Collections.emptyList();
      }
      for (int i = 0, n = statementList.size(); i < n; i++) {
        //遍历Statement列表
        Statement stmt = statementList.get(i);
        //设置超时时间
        applyTransactionTimeout(stmt);
        //获取配出来结果对象
        BatchResult batchResult = batchResultList.get(i);
        try {
          //设置影响的行数
          batchResult.setUpdateCounts(stmt.executeBatch());
          MappedStatement ms = batchResult.getMappedStatement();
          List<Object> parameterObjects = batchResult.getParameterObjects();
          KeyGenerator keyGenerator = ms.getKeyGenerator();
          if (Jdbc3KeyGenerator.class.equals(keyGenerator.getClass())) {
            //如果是Jdbc3KeyGenerator类型的keyGenerator
            Jdbc3KeyGenerator jdbc3KeyGenerator = (Jdbc3KeyGenerator) keyGenerator;
            //调用processBatch进行批处理中key的处理
            jdbc3KeyGenerator.processBatch(ms, stmt, parameterObjects);
          } else if (!NoKeyGenerator.class.equals(keyGenerator.getClass())) { //issue #141
            //如果不是NoKeyGenerator
            for (Object parameter : parameterObjects) {
              //调用processAfter进行key的处理
              keyGenerator.processAfter(this, ms, stmt, parameter);
            }
          }
          // Close statement to close cursor #1109
          //关闭statement
          closeStatement(stmt);
        } catch (BatchUpdateException e) {
          StringBuilder message = new StringBuilder();
          message.append(batchResult.getMappedStatement().getId())
              .append(" (batch index #")
              .append(i + 1)
              .append(")")
              .append(" failed.");
          if (i > 0) {
            message.append(" ")
                .append(i)
                .append(" prior sub executor(s) completed successfully, but will be rolled back.");
          }
          throw new BatchExecutorException(message.toString(), e, results, batchResult);
        }
        //处理后的结果对象加入到结果集合
        results.add(batchResult);
      }
      //返回结果集合
      return results;
    } finally {
      for (Statement stmt : statementList) {
        //关闭statement
        closeStatement(stmt);
      }
      currentSql = null;
      //清空statement集合
      statementList.clear();
      //清空批处理结果集合
      batchResultList.clear();
    }
  }
}
