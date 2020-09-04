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

import org.apache.ibatis.builder.xml.XMLConfigBuilder;
import org.apache.ibatis.exceptions.ExceptionFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

/**
 * Builds {@link SqlSession} instances.
 *
 * @author Clinton Begin
 */
public class SqlSessionFactoryBuilder {

  /**根据输入流构建SqlSessionFactory
   * @param reader        输入流
   * @return              SqlSessionFactory
   */
  public SqlSessionFactory build(Reader reader) {
    //调用内部方法
    return build(reader, null, null);
  }

  /**根据输入流和指定的环境参数构建SqlSessionFactory
   * @param reader        输入流
   * @param environment   环境参数
   * @return              SqlSessionFactory
   */
  public SqlSessionFactory build(Reader reader, String environment) {
    //调用内部方法
    return build(reader, environment, null);
  }

  /**根据输入流和Properties构建SqlSessionFactory
   * @param reader        输入流
   * @param properties    properties
   * @return              SqlSessionFactory
   */
  public SqlSessionFactory build(Reader reader, Properties properties) {
    //调用内部方法
    return build(reader, null, properties);
  }

  /**根据输入流和Properties和指定的环境参数构建SqlSessionFactory
   * @param reader        输入流
   * @param environment   环境参数
   * @param properties    properties
   * @return
   */
  public SqlSessionFactory build(Reader reader, String environment, Properties properties) {
    try {
      //构建一个XMLConfigBuilder, 用于解析mybatis核心配置文件mybatis-config.xml
      XMLConfigBuilder parser = new XMLConfigBuilder(reader, environment, properties);
      //解析核心配置文件
      //构造Configuration对象
      //调用内部方法返回SqlSessionFactory
      return build(parser.parse());
    } catch (Exception e) {
      //把异常包装成为PersistenceException类型异常
      throw ExceptionFactory.wrapException("Error building SqlSession.", e);
    } finally {
      ErrorContext.instance().reset();
      try {
        //关闭输入流
        reader.close();
      } catch (IOException e) {
        // Intentionally ignore. Prefer previous error.
      }
    }
  }

  /**根据输入字节流构建SqlSessionFactory
   * @param inputStream   字节流
   * @return              SqlSessionFactory
   */
  public SqlSessionFactory build(InputStream inputStream) {
    //调用内部方法
    return build(inputStream, null, null);
  }

  /**根据输入字节流和环境参数构建SqlSessionFactory
   * @param inputStream   字节流
   * @param environment   环境参数
   * @return              SqlSessionFactory
   */
  public SqlSessionFactory build(InputStream inputStream, String environment) {
    //调用内部方法
    return build(inputStream, environment, null);
  }

  /**根据输入字节流和Properties构建SqlSessionFactory
   * @param inputStream   字节流
   * @param properties    properties
   * @return              SqlSessionFactory
   */
  public SqlSessionFactory build(InputStream inputStream, Properties properties) {
    //调用内部方法
    return build(inputStream, null, properties);
  }

  /**根据输入字节流,环境参数和Properties构建SqlSessionFactory
   * @param inputStream   字节流
   * @param environment   环境参数
   * @param properties    properties
   * @return              SqlSessionFactory
   */
  public SqlSessionFactory build(InputStream inputStream, String environment, Properties properties) {
    try {
      //构建一个XMLConfigBuilder, 用于解析mybatis核心配置文件mybatis-config.xml
      XMLConfigBuilder parser = new XMLConfigBuilder(inputStream, environment, properties);
      //解析核心配置文件
      //构造Configuration对象
      //调用内部方法返回SqlSessionFactory
      return build(parser.parse());
    } catch (Exception e) {
      //把异常包装成为PersistenceException类型异常
      throw ExceptionFactory.wrapException("Error building SqlSession.", e);
    } finally {
      ErrorContext.instance().reset();
      try {
        //关闭输入流
        inputStream.close();
      } catch (IOException e) {
        // Intentionally ignore. Prefer previous error.
      }
    }
  }

  /**构建SqlSessionFactory的最终实现,根据Configuration构建
   * @param config        Configuration
   * @return              SqlSessionFactory
   */
  public SqlSessionFactory build(Configuration config) {
    //构建一个DefaultSqlSessionFactory
    return new DefaultSqlSessionFactory(config);
  }

}
