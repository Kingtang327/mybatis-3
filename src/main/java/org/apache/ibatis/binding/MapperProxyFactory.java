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
package org.apache.ibatis.binding;

import org.apache.ibatis.binding.MapperProxy.MapperMethodInvoker;
import org.apache.ibatis.session.SqlSession;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Lasse Voss
 */
public class MapperProxyFactory<T> {

  /**
   * Mapper接口类
   */
  private final Class<T> mapperInterface;
  /**
   * Mapper方法和调用器的缓存
   */
  private final Map<Method, MapperMethodInvoker> methodCache = new ConcurrentHashMap<>();

  /**构造函数
   * @param mapperInterface      Mapper接口类
   */
  public MapperProxyFactory(Class<T> mapperInterface) {
    this.mapperInterface = mapperInterface;
  }

  /**获取接口
   * @return            Mapper接口类
   */
  public Class<T> getMapperInterface() {
    return mapperInterface;
  }

  /**Mapper方法和调用器的缓存
   * @return            Mapper方法和调用器的缓存
   */
  public Map<Method, MapperMethodInvoker> getMethodCache() {
    return methodCache;
  }

  /**创建一个Mapper的代理对象
   * @param mapperProxy         MapperProxy
   * @return                    Mapper的代理对象
   */
  @SuppressWarnings("unchecked")
  protected T newInstance(MapperProxy<T> mapperProxy) {
    //调用Proxy创建代理对象
    return (T) Proxy.newProxyInstance(mapperInterface.getClassLoader(), new Class[] { mapperInterface }, mapperProxy);
  }

  /**创建一个Mapper的代理对象
   * @param sqlSession          SqlSession
   * @return                    Mapper的代理对象
   */
  public T newInstance(SqlSession sqlSession) {
    final MapperProxy<T> mapperProxy = new MapperProxy<>(sqlSession, mapperInterface, methodCache);
    return newInstance(mapperProxy);
  }

}
