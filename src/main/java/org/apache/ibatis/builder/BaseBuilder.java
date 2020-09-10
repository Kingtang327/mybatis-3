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
package org.apache.ibatis.builder;

import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeAliasRegistry;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**抽象的基础解析器
 * @author Clinton Begin
 */
public abstract class BaseBuilder {
  /**
   * 核心配置对象
   */
  protected final Configuration configuration;
  /**
   * 类别名注册器
   */
  protected final TypeAliasRegistry typeAliasRegistry;
  /**
   * 类型处理器的注册器, 维护jdbcType和对应的处理器的关系
   */
  protected final TypeHandlerRegistry typeHandlerRegistry;

  /**构造函数
   * @param configuration     核心配置对象
   */
  public BaseBuilder(Configuration configuration) {
    this.configuration = configuration;
    this.typeAliasRegistry = this.configuration.getTypeAliasRegistry();
    this.typeHandlerRegistry = this.configuration.getTypeHandlerRegistry();
  }

  /**获取核心配置对象
   * @return
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**解析正则表达式
   * @param regex             正则表达式
   * @param defaultValue      默认值
   * @return                  解析后的Pattern
   */
  protected Pattern parseExpression(String regex, String defaultValue) {
    return Pattern.compile(regex == null ? defaultValue : regex);
  }

  /**获取布尔类型的值
   * @param value             原始值
   * @param defaultValue      默认值
   * @return                  解析后的Boolean变量
   */
  protected Boolean booleanValueOf(String value, Boolean defaultValue) {
    return value == null ? defaultValue : Boolean.valueOf(value);
  }

  /**获取Integer类型的值
   * @param value             原始值
   * @param defaultValue      默认值
   * @return                  解析后的Integer变量
   */
  protected Integer integerValueOf(String value, Integer defaultValue) {
    return value == null ? defaultValue : Integer.valueOf(value);
  }

  /**解析Set集合数据
   * @param value             原始值
   * @param defaultValue      默认值
   * @return                  解析后的Set集合
   */
  protected Set<String> stringSetValueOf(String value, String defaultValue) {
    value = value == null ? defaultValue : value;
    return new HashSet<>(Arrays.asList(value.split(",")));
  }

  /**根据名称解析Jdbc类型
   * @param alias             jdbc名称
   * @return                  jdbc类型
   */
  protected JdbcType resolveJdbcType(String alias) {
    if (alias == null) {
      return null;
    }
    try {
      return JdbcType.valueOf(alias);
    } catch (IllegalArgumentException e) {
      throw new BuilderException("Error resolving JdbcType. Cause: " + e, e);
    }
  }

  /**解析结果集类型
   * @param alias             结果集类型别名
   * @return                  结果集类型
   */
  protected ResultSetType resolveResultSetType(String alias) {
    if (alias == null) {
      return null;
    }
    try {
      return ResultSetType.valueOf(alias);
    } catch (IllegalArgumentException e) {
      throw new BuilderException("Error resolving ResultSetType. Cause: " + e, e);
    }
  }

  /**解析参数模式              IN, OUT, INOUT
   * @param alias             别名
   * @return                  参数类型对象
   */
  protected ParameterMode resolveParameterMode(String alias) {
    if (alias == null) {
      return null;
    }
    try {
      return ParameterMode.valueOf(alias);
    } catch (IllegalArgumentException e) {
      throw new BuilderException("Error resolving ParameterMode. Cause: " + e, e);
    }
  }

  /**根据别名创建对象
   * @param alias             类别名
   * @return                  对象
   */
  protected Object createInstance(String alias) {
    //根据别名解析对象类
    Class<?> clazz = resolveClass(alias);
    if (clazz == null) {
      return null;
    }
    try {
      //调用无参构造函数初始化对象
      return clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new BuilderException("Error creating instance. Cause: " + e, e);
    }
  }

  /**根据别名解析对象类
   * @param alias             类别名
   * @param <T>               泛型
   * @return                  类对象
   */
  protected <T> Class<? extends T> resolveClass(String alias) {
    if (alias == null) {
      return null;
    }
    try {
      //根据别名解析对象类
      return resolveAlias(alias);
    } catch (Exception e) {
      throw new BuilderException("Error resolving class. Cause: " + e, e);
    }
  }

  /**解析类型处理器
   * @param javaType          java类型
   * @param typeHandlerAlias  处理器别名
   * @return                  类型处理器
   */
  protected TypeHandler<?> resolveTypeHandler(Class<?> javaType, String typeHandlerAlias) {
    if (typeHandlerAlias == null) {
      return null;
    }
    //根据别名解析类对象
    Class<?> type = resolveClass(typeHandlerAlias);
    //判断是否是TypeHandler的实现
    if (type != null && !TypeHandler.class.isAssignableFrom(type)) {
      throw new BuilderException("Type " + type.getName() + " is not a valid TypeHandler because it does not implement TypeHandler interface");
    }
    @SuppressWarnings("unchecked") // already verified it is a TypeHandler
    Class<? extends TypeHandler<?>> typeHandlerType = (Class<? extends TypeHandler<?>>) type;
    //解析类型处理器
    return resolveTypeHandler(javaType, typeHandlerType);
  }

  /**解析类型处理器
   * @param javaType
   * @param typeHandlerType
   * @return
   */
  protected TypeHandler<?> resolveTypeHandler(Class<?> javaType, Class<? extends TypeHandler<?>> typeHandlerType) {
    if (typeHandlerType == null) {
      return null;
    }
    // javaType ignored for injected handlers see issue #746 for full detail
    //从类型注册器中获取类型处理器
    TypeHandler<?> handler = typeHandlerRegistry.getMappingTypeHandler(typeHandlerType);
    if (handler == null) {
      // not in registry, create a new one
      //未找到类型处理器,使用反射创建一个并返回
      handler = typeHandlerRegistry.getInstance(javaType, typeHandlerType);
    }
    return handler;
  }

  /**解析类别名
   * @param alias       类别名
   * @param <T>         泛型
   * @return            对应的java类对象
   */
  protected <T> Class<? extends T> resolveAlias(String alias) {
    return typeAliasRegistry.resolveAlias(alias);
  }
}
