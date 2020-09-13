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
package org.apache.ibatis.binding;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.annotations.MapKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.reflection.TypeParameterResolver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

/**
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Lasse Voss
 * @author Kazuki Shimizu
 */
public class MapperMethod {

  /**
   * sql命令
   */
  private final SqlCommand command;
  /**
   * 方法签名
   */
  private final MethodSignature method;

  /**构造函数
   * @param mapperInterface         mapper接口类
   * @param method                  方法对象
   * @param config                  核心配置文件
   */
  public MapperMethod(Class<?> mapperInterface, Method method, Configuration config) {
    this.command = new SqlCommand(config, mapperInterface, method);
    this.method = new MethodSignature(config, mapperInterface, method);
  }

  /**执行方法
   * @param sqlSession                SqlSession
   * @param args                      参数
   * @return                          返回对象
   */
  public Object execute(SqlSession sqlSession, Object[] args) {
    Object result;
    switch (command.getType()) {
      case INSERT: {
        //插入类型语句
        //转换参数
        Object param = method.convertArgsToSqlCommandParam(args);
        //调用sqlSession的插入方法
        //处理返回sql影响行数
        result = rowCountResult(sqlSession.insert(command.getName(), param));
        break;
      }
      case UPDATE: {
        //更新类型语句
        //转换参数
        Object param = method.convertArgsToSqlCommandParam(args);
        //调用sqlSession的更新方法
        //处理返回sql影响行数
        result = rowCountResult(sqlSession.update(command.getName(), param));
        break;
      }
      case DELETE: {
        //删除类型语句
        //转换参数
        Object param = method.convertArgsToSqlCommandParam(args);
        //调用sqlSession的删除方法
        //处理返回sql影响行数
        result = rowCountResult(sqlSession.delete(command.getName(), param));
        break;
      }
      case SELECT:
        //插入类型语句

        if (method.returnsVoid() && method.hasResultHandler()) {
          //如果被调用方法的返回值为空,并且方法中的参数指定了ResultHandler
          //使用结果处理器的方式调用
          executeWithResultHandler(sqlSession, args);
          result = null;
        } else if (method.returnsMany()) {
          //如果被调用方法的返回值是集合或者是数组类型
          //调用查询多条
          result = executeForMany(sqlSession, args);
        } else if (method.returnsMap()) {
          //如果被调用方法的返回值是Map
          //查询map的方式调用
          result = executeForMap(sqlSession, args);
        } else if (method.returnsCursor()) {
          //如果被调用方法的返回值是cursor
          //查询cursor的方式调用
          result = executeForCursor(sqlSession, args);
        } else {
          //否则调用sqlSession的查询单条方法
          //转换参数
          Object param = method.convertArgsToSqlCommandParam(args);
          result = sqlSession.selectOne(command.getName(), param);
          if (method.returnsOptional()
              && (result == null || !method.getReturnType().equals(result.getClass()))) {
            //返回值为Optional,并且返回值类型和实际结果类型不一致,或者结果为空
            //使用Optional包装结果
            result = Optional.ofNullable(result);
          }
        }
        break;
      case FLUSH:
        //FLUSH类型
        //调用sqlSession的flushStatements
        result = sqlSession.flushStatements();
        break;
      default:
        throw new BindingException("Unknown execution method for: " + command.getName());
    }
    if (result == null && method.getReturnType().isPrimitive() && !method.returnsVoid()) {
      throw new BindingException("Mapper method '" + command.getName()
          + " attempted to return null from a method with a primitive return type (" + method.getReturnType() + ").");
    }
    return result;
  }

  /**处理返回值结果
   * @param rowCount                  结果行数
   * @return                          返回结果
   */
  private Object rowCountResult(int rowCount) {
    final Object result;
    if (method.returnsVoid()) {
      //没有返回值时,设置结果为null
      result = null;
    } else if (Integer.class.equals(method.getReturnType()) || Integer.TYPE.equals(method.getReturnType())) {
      //如果返回值为Integer或者int,直接返回当前行数
      result = rowCount;
    } else if (Long.class.equals(method.getReturnType()) || Long.TYPE.equals(method.getReturnType())) {
      //如果返回值为Long或者long,强转为long
      result = (long) rowCount;
    } else if (Boolean.class.equals(method.getReturnType()) || Boolean.TYPE.equals(method.getReturnType())) {
      //返回值为Boolean, 返回行数是否大于0
      result = rowCount > 0;
    } else {
      throw new BindingException("Mapper method '" + command.getName() + "' has an unsupported return type: " + method.getReturnType());
    }
    return result;
  }

  /**使用结果处理器的方式执行
   * @param sqlSession                sqlSession
   * @param args                      参数
   */
  private void executeWithResultHandler(SqlSession sqlSession, Object[] args) {
    //获取MappedStatement
    MappedStatement ms = sqlSession.getConfiguration().getMappedStatement(command.getName());
    if (!StatementType.CALLABLE.equals(ms.getStatementType())
        && void.class.equals(ms.getResultMaps().get(0).getType())) {
      throw new BindingException("method " + command.getName()
          + " needs either a @ResultMap annotation, a @ResultType annotation,"
          + " or a resultType attribute in XML so a ResultHandler can be used as a parameter.");
    }
    //转换参数
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      //如果有分页参数,转换分页参数
      RowBounds rowBounds = method.extractRowBounds(args);
      //调用sqlSession的select
      sqlSession.select(command.getName(), param, rowBounds, method.extractResultHandler(args));
    } else {
      //调用sqlSession的select
      sqlSession.select(command.getName(), param, method.extractResultHandler(args));
    }
  }

  /**查询返回多个结果
   * @param sqlSession                sqlSession
   * @param args                      参数
   * @param <E>                       泛型
   * @return                          结果结合
   */
  private <E> Object executeForMany(SqlSession sqlSession, Object[] args) {
    List<E> result;
    //转换参数
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      //有分页参数
      RowBounds rowBounds = method.extractRowBounds(args);
      //调用sqlSession的selectList
      result = sqlSession.selectList(command.getName(), param, rowBounds);
    } else {
      //调用sqlSession的selectList
      result = sqlSession.selectList(command.getName(), param);
    }
    // issue #510 Collections & arrays support
    if (!method.getReturnType().isAssignableFrom(result.getClass())) {
      //返回类型不为List时
      if (method.getReturnType().isArray()) {
        //转换成数组
        return convertToArray(result);
      } else {
        //转换成定义的集合类型
        return convertToDeclaredCollection(sqlSession.getConfiguration(), result);
      }
    }
    return result;
  }

  /**查询返回游标
   * @param sqlSession                sqlSession
   * @param args                      参数
   * @param <T>                       泛型
   * @return                          游标
   */
  private <T> Cursor<T> executeForCursor(SqlSession sqlSession, Object[] args) {
    Cursor<T> result;
    //转换参数
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      //如果有分页参数,转换分页参数
      RowBounds rowBounds = method.extractRowBounds(args);
      //调用查询游标
      result = sqlSession.selectCursor(command.getName(), param, rowBounds);
    } else {
      //调用查询游标
      result = sqlSession.selectCursor(command.getName(), param);
    }
    return result;
  }

  /**转为结果为指定的集合类型
   * @param config                    核心配置对象
   * @param list                      结果列表
   * @param <E>                       结果泛型
   * @return                          指定类型的集合
   */
  private <E> Object convertToDeclaredCollection(Configuration config, List<E> list) {
    //用对象工厂创建一个返回值类型的集合对象
    Object collection = config.getObjectFactory().create(method.getReturnType());
    //为集合对象创建一个元数据对象
    MetaObject metaObject = config.newMetaObject(collection);
    //把结果结合加入到指定集合中
    metaObject.addAll(list);
    //返回指定集合
    return collection;
  }

  /**将List转换为数组
   * @param list                      List集合
   * @param <E>                       泛型
   * @return                          数组
   */
  @SuppressWarnings("unchecked")
  private <E> Object convertToArray(List<E> list) {
    //获取结合的元素类型
    Class<?> arrayComponentType = method.getReturnType().getComponentType();
    //根据元素类型和,集合size创建一个数组
    Object array = Array.newInstance(arrayComponentType, list.size());
    if (arrayComponentType.isPrimitive()) {
      //如果是原始类型
      for (int i = 0; i < list.size(); i++) {
        //设置集合元素值
        Array.set(array, i, list.get(i));
      }
      return array;
    } else {
      //使用集合的toArray方法转换成数组
      return list.toArray((E[]) array);
    }
  }

  /**以查询的Map的方式执行
   * @param sqlSession                sqlSession
   * @param args                      参数
   * @param <K>                       Key
   * @param <V>                       Value
   * @return                          map
   */
  private <K, V> Map<K, V> executeForMap(SqlSession sqlSession, Object[] args) {
    Map<K, V> result;
    //转换参数
    Object param = method.convertArgsToSqlCommandParam(args);
    if (method.hasRowBounds()) {
      //如果有分页参数,转换分页参数
      RowBounds rowBounds = method.extractRowBounds(args);
      //调用selectMap
      result = sqlSession.selectMap(command.getName(), param, method.getMapKey(), rowBounds);
    } else {
      //调用selectMap
      result = sqlSession.selectMap(command.getName(), param, method.getMapKey());
    }
    return result;
  }

  public static class ParamMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -2212268410512043556L;

    @Override
    public V get(Object key) {
      if (!super.containsKey(key)) {
        throw new BindingException("Parameter '" + key + "' not found. Available parameters are " + keySet());
      }
      return super.get(key);
    }

  }

  public static class SqlCommand {

    private final String name;
    private final SqlCommandType type;

    public SqlCommand(Configuration configuration, Class<?> mapperInterface, Method method) {
      final String methodName = method.getName();
      final Class<?> declaringClass = method.getDeclaringClass();
      MappedStatement ms = resolveMappedStatement(mapperInterface, methodName, declaringClass,
          configuration);
      if (ms == null) {
        if (method.getAnnotation(Flush.class) != null) {
          name = null;
          type = SqlCommandType.FLUSH;
        } else {
          throw new BindingException("Invalid bound statement (not found): "
              + mapperInterface.getName() + "." + methodName);
        }
      } else {
        name = ms.getId();
        type = ms.getSqlCommandType();
        if (type == SqlCommandType.UNKNOWN) {
          throw new BindingException("Unknown execution method for: " + name);
        }
      }
    }

    public String getName() {
      return name;
    }

    public SqlCommandType getType() {
      return type;
    }

    private MappedStatement resolveMappedStatement(Class<?> mapperInterface, String methodName,
        Class<?> declaringClass, Configuration configuration) {
      String statementId = mapperInterface.getName() + "." + methodName;
      if (configuration.hasStatement(statementId)) {
        return configuration.getMappedStatement(statementId);
      } else if (mapperInterface.equals(declaringClass)) {
        return null;
      }
      for (Class<?> superInterface : mapperInterface.getInterfaces()) {
        if (declaringClass.isAssignableFrom(superInterface)) {
          MappedStatement ms = resolveMappedStatement(superInterface, methodName,
              declaringClass, configuration);
          if (ms != null) {
            return ms;
          }
        }
      }
      return null;
    }
  }

  public static class MethodSignature {

    private final boolean returnsMany;
    private final boolean returnsMap;
    private final boolean returnsVoid;
    private final boolean returnsCursor;
    private final boolean returnsOptional;
    private final Class<?> returnType;
    private final String mapKey;
    private final Integer resultHandlerIndex;
    private final Integer rowBoundsIndex;
    private final ParamNameResolver paramNameResolver;

    public MethodSignature(Configuration configuration, Class<?> mapperInterface, Method method) {
      Type resolvedReturnType = TypeParameterResolver.resolveReturnType(method, mapperInterface);
      if (resolvedReturnType instanceof Class<?>) {
        this.returnType = (Class<?>) resolvedReturnType;
      } else if (resolvedReturnType instanceof ParameterizedType) {
        this.returnType = (Class<?>) ((ParameterizedType) resolvedReturnType).getRawType();
      } else {
        this.returnType = method.getReturnType();
      }
      this.returnsVoid = void.class.equals(this.returnType);
      this.returnsMany = configuration.getObjectFactory().isCollection(this.returnType) || this.returnType.isArray();
      this.returnsCursor = Cursor.class.equals(this.returnType);
      this.returnsOptional = Optional.class.equals(this.returnType);
      this.mapKey = getMapKey(method);
      this.returnsMap = this.mapKey != null;
      this.rowBoundsIndex = getUniqueParamIndex(method, RowBounds.class);
      this.resultHandlerIndex = getUniqueParamIndex(method, ResultHandler.class);
      this.paramNameResolver = new ParamNameResolver(configuration, method);
    }

    public Object convertArgsToSqlCommandParam(Object[] args) {
      return paramNameResolver.getNamedParams(args);
    }

    public boolean hasRowBounds() {
      return rowBoundsIndex != null;
    }

    public RowBounds extractRowBounds(Object[] args) {
      return hasRowBounds() ? (RowBounds) args[rowBoundsIndex] : null;
    }

    public boolean hasResultHandler() {
      return resultHandlerIndex != null;
    }

    public ResultHandler extractResultHandler(Object[] args) {
      return hasResultHandler() ? (ResultHandler) args[resultHandlerIndex] : null;
    }

    public Class<?> getReturnType() {
      return returnType;
    }

    public boolean returnsMany() {
      return returnsMany;
    }

    public boolean returnsMap() {
      return returnsMap;
    }

    public boolean returnsVoid() {
      return returnsVoid;
    }

    public boolean returnsCursor() {
      return returnsCursor;
    }

    /**
     * return whether return type is {@code java.util.Optional}.
     *
     * @return return {@code true}, if return type is {@code java.util.Optional}
     * @since 3.5.0
     */
    public boolean returnsOptional() {
      return returnsOptional;
    }

    private Integer getUniqueParamIndex(Method method, Class<?> paramType) {
      Integer index = null;
      final Class<?>[] argTypes = method.getParameterTypes();
      for (int i = 0; i < argTypes.length; i++) {
        if (paramType.isAssignableFrom(argTypes[i])) {
          if (index == null) {
            index = i;
          } else {
            throw new BindingException(method.getName() + " cannot have multiple " + paramType.getSimpleName() + " parameters");
          }
        }
      }
      return index;
    }

    public String getMapKey() {
      return mapKey;
    }

    private String getMapKey(Method method) {
      String mapKey = null;
      if (Map.class.isAssignableFrom(method.getReturnType())) {
        final MapKey mapKeyAnnotation = method.getAnnotation(MapKey.class);
        if (mapKeyAnnotation != null) {
          mapKey = mapKeyAnnotation.value();
        }
      }
      return mapKey;
    }
  }

}
