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
package org.apache.ibatis.builder.xml;

import org.apache.ibatis.builder.*;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import java.io.InputStream;
import java.io.Reader;
import java.util.*;

/**mapper.xml解析器
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLMapperBuilder extends BaseBuilder {

  /**
   * xml解析器
   */
  private final XPathParser parser;
  /**
   * mapper解析助手
   */
  private final MapperBuilderAssistant builderAssistant;
  /**
   * sql片段
   */
  private final Map<String, XNode> sqlFragments;
  /**
   * 资源信息
   */
  private final String resource;

  @Deprecated
  public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments, String namespace) {
    this(reader, configuration, resource, sqlFragments);
    this.builderAssistant.setCurrentNamespace(namespace);
  }

  @Deprecated
  public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    this(new XPathParser(reader, true, configuration.getVariables(), new XMLMapperEntityResolver()),
        configuration, resource, sqlFragments);
  }

  /**构造函数
   * @param inputStream         输入流
   * @param configuration       核心配置对象
   * @param resource            资源路径
   * @param sqlFragments        sql片段
   * @param namespace           名称空间
   */
  public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource, Map<String, XNode> sqlFragments, String namespace) {
    this(inputStream, configuration, resource, sqlFragments);
    //设置名称空间
    this.builderAssistant.setCurrentNamespace(namespace);
  }

  /**构造函数
   * @param inputStream         输入流
   * @param configuration       核心配置对象
   * @param resource            资源路径
   * @param sqlFragments        sql片段缓存
   */
  public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    this(new XPathParser(inputStream, true, configuration.getVariables(), new XMLMapperEntityResolver()),
        configuration, resource, sqlFragments);
  }

  /**私有构造函数
   * @param parser              解析器
   * @param configuration       核心配置对象
   * @param resource            资源路径
   * @param sqlFragments        sql片段缓存
   */
  private XMLMapperBuilder(XPathParser parser, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
    super(configuration);
    //构建助手对象
    this.builderAssistant = new MapperBuilderAssistant(configuration, resource);
    this.parser = parser;
    this.sqlFragments = sqlFragments;
    this.resource = resource;
  }

  /**
   * 解析mapper.xml
   */
  public void parse() {
    if (!configuration.isResourceLoaded(resource)) {
      //改资源未加载过
      //解析当前mapper节点下的配置
      configurationElement(parser.evalNode("/mapper"));
      //加入到已解析资源结合
      configuration.addLoadedResource(resource);
      //设置Mapper.class和名称空间的对应关系
      bindMapperForNamespace();
    }

    //解析待解析的resultMap
    parsePendingResultMaps();
    //解析待解析的缓存引用
    parsePendingCacheRefs();
    //解析待解析的sql
    parsePendingStatements();
  }

  /**根据id获取对应的sql片段对象
   * @param refid               sql片段id
   * @return                    sql片段
   */
  public XNode getSqlFragment(String refid) {
    return sqlFragments.get(refid);
  }

  /**解析mapper.xml
   * @param context             xml节点
   */
  private void configurationElement(XNode context) {
    try {
      //解析名称空间
      String namespace = context.getStringAttribute("namespace");
      if (namespace == null || namespace.isEmpty()) {
        throw new BuilderException("Mapper's namespace cannot be empty");
      }
      //设置名称空间
      builderAssistant.setCurrentNamespace(namespace);
      //解析缓存引用
      cacheRefElement(context.evalNode("cache-ref"));
      //解析二级缓存
      cacheElement(context.evalNode("cache"));
      //解析parameterMap
      parameterMapElement(context.evalNodes("/mapper/parameterMap"));
      //解析resultMap
      resultMapElements(context.evalNodes("/mapper/resultMap"));
      //解析sql片段
      sqlElement(context.evalNodes("/mapper/sql"));
      //解析sql语句
      buildStatementFromContext(context.evalNodes("select|insert|update|delete"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing Mapper XML. The XML location is '" + resource + "'. Cause: " + e, e);
    }
  }

  /**解析sql语句
   * @param list                sql语句xml列表
   */
  private void buildStatementFromContext(List<XNode> list) {
    if (configuration.getDatabaseId() != null) {
      buildStatementFromContext(list, configuration.getDatabaseId());
    }
    buildStatementFromContext(list, null);
  }

  /**解析sql语句
   * @param list                sql语句xml列表
   * @param requiredDatabaseId  数据库厂商id
   */
  private void buildStatementFromContext(List<XNode> list, String requiredDatabaseId) {
    for (XNode context : list) {
      //构建一个sql语句解析器
      final XMLStatementBuilder statementParser = new XMLStatementBuilder(configuration, builderAssistant, context, requiredDatabaseId);
      try {
        //解析sql
        statementParser.parseStatementNode();
      } catch (IncompleteElementException e) {
        //解析失败,加入到未完成的sql集合中
        configuration.addIncompleteStatement(statementParser);
      }
    }
  }

  /**
   * 解析未完成的resultMap
   */
  private void parsePendingResultMaps() {
    //获取未完成解析的resultMap集合, LinkedList
    //此处使用LinkedList的原因应是因为会涉及到移除元素操作,使用链表效率高
    Collection<ResultMapResolver> incompleteResultMaps = configuration.getIncompleteResultMaps();
    synchronized (incompleteResultMaps) {
      //此处得到的迭代器实际是一个ListIterator,所以在遍历时remove不会报错
      Iterator<ResultMapResolver> iter = incompleteResultMaps.iterator();
      while (iter.hasNext()) {
        try {
          //遍历解析
          iter.next().resolve();
          //解析成功后从集合移除
          iter.remove();
        } catch (IncompleteElementException e) {
          // ResultMap is still missing a resource...
        }
      }
    }
  }

  /**
   * 解析未完成的二级缓存引用配置
   */
  private void parsePendingCacheRefs() {
    //获取未完成的二级缓存引用配置, LinkedList
    //此处使用LinkedList的原因应是因为会涉及到移除元素操作,使用链表效率高
    Collection<CacheRefResolver> incompleteCacheRefs = configuration.getIncompleteCacheRefs();
    synchronized (incompleteCacheRefs) {
      Iterator<CacheRefResolver> iter = incompleteCacheRefs.iterator();
      while (iter.hasNext()) {
        try {
          //遍历解析
          iter.next().resolveCacheRef();
          //解析成功后从集合移除
          iter.remove();
        } catch (IncompleteElementException e) {
          // Cache ref is still missing a resource...
        }
      }
    }
  }

  /**
   * 解析未完成的sql语句
   */
  private void parsePendingStatements() {
    //获取未完成解析的sql语句集合, LinkedList
    ////此处使用LinkedList的原因应是因为会涉及到移除元素操作,使用链表效率高
    Collection<XMLStatementBuilder> incompleteStatements = configuration.getIncompleteStatements();
    synchronized (incompleteStatements) {
      Iterator<XMLStatementBuilder> iter = incompleteStatements.iterator();
      while (iter.hasNext()) {
        try {
          //遍历解析
          iter.next().parseStatementNode();
          //解析成功后从集合移除
          iter.remove();
        } catch (IncompleteElementException e) {
          // Statement is still missing a resource...
        }
      }
    }
  }

  /**解析cache-ref标签
   * @param context             xml节点
   */
  private void cacheRefElement(XNode context) {
    if (context != null) {
      //将当前名称空间和被引用的名称空间放入map中
      configuration.addCacheRef(builderAssistant.getCurrentNamespace(), context.getStringAttribute("namespace"));
      //构建一个cache-ref解析器
      CacheRefResolver cacheRefResolver = new CacheRefResolver(builderAssistant, context.getStringAttribute("namespace"));
      try {
        //解析
        cacheRefResolver.resolveCacheRef();
      } catch (IncompleteElementException e) {
        //解析失败,加入到未完成的cache-ref的列表中
        configuration.addIncompleteCacheRef(cacheRefResolver);
      }
    }
  }

  /**解析二级缓存标签cache
   * @param context             xml节点
   */
  private void cacheElement(XNode context) {
    if (context != null) {
      //获取cache标签中配置的各个属性
      String type = context.getStringAttribute("type", "PERPETUAL");
      Class<? extends Cache> typeClass = typeAliasRegistry.resolveAlias(type);
      String eviction = context.getStringAttribute("eviction", "LRU");
      Class<? extends Cache> evictionClass = typeAliasRegistry.resolveAlias(eviction);
      Long flushInterval = context.getLongAttribute("flushInterval");
      Integer size = context.getIntAttribute("size");
      boolean readWrite = !context.getBooleanAttribute("readOnly", false);
      boolean blocking = context.getBooleanAttribute("blocking", false);
      Properties props = context.getChildrenAsProperties();
      //使用助手,构建cache对象,将缓存对象加入到核心配置对象中
      builderAssistant.useNewCache(typeClass, evictionClass, flushInterval, size, readWrite, blocking, props);
    }
  }

  /**解析parameterMap标签
   * @param list                xml节点列表
   */
  private void parameterMapElement(List<XNode> list) {
    for (XNode parameterMapNode : list) {
      //遍历xml节点
      String id = parameterMapNode.getStringAttribute("id");
      String type = parameterMapNode.getStringAttribute("type");
      //根据类型解析类对象
      Class<?> parameterClass = resolveClass(type);
      List<XNode> parameterNodes = parameterMapNode.evalNodes("parameter");
      List<ParameterMapping> parameterMappings = new ArrayList<>();
      for (XNode parameterNode : parameterNodes) {
        //遍历parameter节点,并解析
        String property = parameterNode.getStringAttribute("property");
        String javaType = parameterNode.getStringAttribute("javaType");
        String jdbcType = parameterNode.getStringAttribute("jdbcType");
        String resultMap = parameterNode.getStringAttribute("resultMap");
        String mode = parameterNode.getStringAttribute("mode");
        String typeHandler = parameterNode.getStringAttribute("typeHandler");
        Integer numericScale = parameterNode.getIntAttribute("numericScale");
        ParameterMode modeEnum = resolveParameterMode(mode);
        Class<?> javaTypeClass = resolveClass(javaType);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        //使用助手构建ParameterMapping对象
        ParameterMapping parameterMapping = builderAssistant.buildParameterMapping(parameterClass, property, javaTypeClass, jdbcTypeEnum, resultMap, modeEnum, typeHandlerClass, numericScale);
        //解析后的节点加入到列表中
        parameterMappings.add(parameterMapping);
      }
      //将解析完成的一个parameterMap标签构建成ParameterMap对象
      //将对象加入到核心配置对象中
      builderAssistant.addParameterMap(id, parameterClass, parameterMappings);
    }
  }

  /**解析resultMap标签
   * @param list                xml节点列表
   */
  private void resultMapElements(List<XNode> list) {
    for (XNode resultMapNode : list) {
      try {
        resultMapElement(resultMapNode);
      } catch (IncompleteElementException e) {
        // ignore, it will be retried
      }
    }
  }

  /**解析resultMap标签
   * @param resultMapNode       xml节点
   * @return                    ResultMap
   */
  private ResultMap resultMapElement(XNode resultMapNode) {
    return resultMapElement(resultMapNode, Collections.emptyList(), null);
  }

  /**解析resultMap标签
   * @param resultMapNode             xml节点
   * @param additionalResultMappings  附加的ResultMapping列表
   * @param enclosingType             类对象
   * @return                          ResultMap
   */
  private ResultMap resultMapElement(XNode resultMapNode, List<ResultMapping> additionalResultMappings, Class<?> enclosingType) {
    ErrorContext.instance().activity("processing " + resultMapNode.getValueBasedIdentifier());
    //解析type
    String type = resultMapNode.getStringAttribute("type",
        resultMapNode.getStringAttribute("ofType",
            resultMapNode.getStringAttribute("resultType",
                resultMapNode.getStringAttribute("javaType"))));
    //根据type构建类对象
    Class<?> typeClass = resolveClass(type);
    if (typeClass == null) {
      //如果类对象为空, 继承指定的类对象
      typeClass = inheritEnclosingType(resultMapNode, enclosingType);
    }
    Discriminator discriminator = null;
    List<ResultMapping> resultMappings = new ArrayList<>(additionalResultMappings);
    List<XNode> resultChildren = resultMapNode.getChildren();
    for (XNode resultChild : resultChildren) {
      if ("constructor".equals(resultChild.getName())) {
        //解析constructor标签
        processConstructorElement(resultChild, typeClass, resultMappings);
      } else if ("discriminator".equals(resultChild.getName())) {
        //解析鉴别器
        discriminator = processDiscriminatorElement(resultChild, typeClass, resultMappings);
      } else {
        //解析id和result,association,collection等
        List<ResultFlag> flags = new ArrayList<>();
        if ("id".equals(resultChild.getName())) {
          flags.add(ResultFlag.ID);
        }
        resultMappings.add(buildResultMappingFromContext(resultChild, typeClass, flags));
      }
    }
    String id = resultMapNode.getStringAttribute("id",
            resultMapNode.getValueBasedIdentifier());
    String extend = resultMapNode.getStringAttribute("extends");
    Boolean autoMapping = resultMapNode.getBooleanAttribute("autoMapping");
    //构建解析器
    ResultMapResolver resultMapResolver = new ResultMapResolver(builderAssistant, id, typeClass, extend, discriminator, resultMappings, autoMapping);
    try {
      //解析
      return resultMapResolver.resolve();
    } catch (IncompleteElementException e) {
      //加入到未完成解析的集合中
      configuration.addIncompleteResultMap(resultMapResolver);
      throw e;
    }
  }

  /**继承指定类型
   * @param resultMapNode             resultMap节点
   * @param enclosingType             指定的类型
   * @return                          类对象
   */
  protected Class<?> inheritEnclosingType(XNode resultMapNode, Class<?> enclosingType) {
    if ("association".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
      //是association标签
      //获取属性字段
      String property = resultMapNode.getStringAttribute("property");
      if (property != null && enclosingType != null) {
        //创建指定类的类元数据对象
        MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
        //找到该属性的setter方法的参数类型并返回
        return metaResultType.getSetterType(property);
      }
    } else if ("case".equals(resultMapNode.getName()) && resultMapNode.getStringAttribute("resultMap") == null) {
      //鉴别器中的case节点, 直接返回类对象
      return enclosingType;
    }
    return null;
  }

  /**处理constructor标签
   * @param resultChild               xml节点
   * @param resultType                类对象
   * @param resultMappings            ResultMapping列表
   */
  private void processConstructorElement(XNode resultChild, Class<?> resultType, List<ResultMapping> resultMappings) {
    List<XNode> argChildren = resultChild.getChildren();
    for (XNode argChild : argChildren) {
      List<ResultFlag> flags = new ArrayList<>();
      flags.add(ResultFlag.CONSTRUCTOR);
      if ("idArg".equals(argChild.getName())) {
        flags.add(ResultFlag.ID);
      }
      resultMappings.add(buildResultMappingFromContext(argChild, resultType, flags));
    }
  }

  /**解析鉴别器
   * @param context                   xml节点
   * @param resultType                类对象
   * @param resultMappings            ResultMapping列表
   * @return                          鉴别器
   */
  private Discriminator processDiscriminatorElement(XNode context, Class<?> resultType, List<ResultMapping> resultMappings) {
    String column = context.getStringAttribute("column");
    String javaType = context.getStringAttribute("javaType");
    String jdbcType = context.getStringAttribute("jdbcType");
    String typeHandler = context.getStringAttribute("typeHandler");
    Class<?> javaTypeClass = resolveClass(javaType);
    Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
    JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
    Map<String, String> discriminatorMap = new HashMap<>();
    for (XNode caseChild : context.getChildren()) {
      String value = caseChild.getStringAttribute("value");
      String resultMap = caseChild.getStringAttribute("resultMap", processNestedResultMappings(caseChild, resultMappings, resultType));
      discriminatorMap.put(value, resultMap);
    }
    return builderAssistant.buildDiscriminator(resultType, column, javaTypeClass, jdbcTypeEnum, typeHandlerClass, discriminatorMap);
  }

  /**处理sql片段
   * @param list                      xml节点列表
   */
  private void sqlElement(List<XNode> list) {
    if (configuration.getDatabaseId() != null) {
      sqlElement(list, configuration.getDatabaseId());
    }
    sqlElement(list, null);
  }

  /**解析sql片段,放入缓存对象
   * @param list                      xml节点列表
   * @param requiredDatabaseId        数据库厂商id
   */
  private void sqlElement(List<XNode> list, String requiredDatabaseId) {
    for (XNode context : list) {
      String databaseId = context.getStringAttribute("databaseId");
      String id = context.getStringAttribute("id");
      id = builderAssistant.applyCurrentNamespace(id, false);
      if (databaseIdMatchesCurrent(id, databaseId, requiredDatabaseId)) {
        sqlFragments.put(id, context);
      }
    }
  }

  /**判断数据库厂商id是否和当前匹配
   * @param id                      sql片段id
   * @param databaseId              数据库厂商id
   * @param requiredDatabaseId      需要的数据库厂商id
   * @return                        是否和当前匹配
   */
  private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
    if (requiredDatabaseId != null) {
      return requiredDatabaseId.equals(databaseId);
    }
    if (databaseId != null) {
      return false;
    }
    if (!this.sqlFragments.containsKey(id)) {
      return true;
    }
    // skip this fragment if there is a previous one with a not null databaseId
    XNode context = this.sqlFragments.get(id);
    return context.getStringAttribute("databaseId") == null;
  }

  /**构造resultMapping对象
   * @param context                 xml节点
   * @param resultType              类型
   * @param flags                   结果的标记集合
   * @return
   */
  private ResultMapping buildResultMappingFromContext(XNode context, Class<?> resultType, List<ResultFlag> flags) {
    String property;
    if (flags.contains(ResultFlag.CONSTRUCTOR)) {
      property = context.getStringAttribute("name");
    } else {
      property = context.getStringAttribute("property");
    }
    String column = context.getStringAttribute("column");
    String javaType = context.getStringAttribute("javaType");
    String jdbcType = context.getStringAttribute("jdbcType");
    String nestedSelect = context.getStringAttribute("select");
    String nestedResultMap = context.getStringAttribute("resultMap", () ->
        processNestedResultMappings(context, Collections.emptyList(), resultType));
    String notNullColumn = context.getStringAttribute("notNullColumn");
    String columnPrefix = context.getStringAttribute("columnPrefix");
    String typeHandler = context.getStringAttribute("typeHandler");
    String resultSet = context.getStringAttribute("resultSet");
    String foreignColumn = context.getStringAttribute("foreignColumn");
    boolean lazy = "lazy".equals(context.getStringAttribute("fetchType", configuration.isLazyLoadingEnabled() ? "lazy" : "eager"));
    Class<?> javaTypeClass = resolveClass(javaType);
    Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
    JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
    return builderAssistant.buildResultMapping(resultType, property, column, javaTypeClass, jdbcTypeEnum, nestedSelect, nestedResultMap, notNullColumn, columnPrefix, typeHandlerClass, flags, resultSet, foreignColumn, lazy);
  }

  /**处理嵌套层级的resultMapping
   * @param context
   * @param resultMappings
   * @param enclosingType
   * @return
   */
  private String processNestedResultMappings(XNode context, List<ResultMapping> resultMappings, Class<?> enclosingType) {
    if (Arrays.asList("association", "collection", "case").contains(context.getName())
        && context.getStringAttribute("select") == null) {
      //如果是association, collection, case标签,并且不是嵌套查询的
      //验证collection标签
      validateCollection(context, enclosingType);
      //解析resultmapping
      ResultMap resultMap = resultMapElement(context, resultMappings, enclosingType);
      return resultMap.getId();
    }
    return null;
  }

  /**延迟collection标签
   * @param context
   * @param enclosingType
   */
  protected void validateCollection(XNode context, Class<?> enclosingType) {
    if ("collection".equals(context.getName()) && context.getStringAttribute("resultMap") == null
        && context.getStringAttribute("javaType") == null) {
      MetaClass metaResultType = MetaClass.forClass(enclosingType, configuration.getReflectorFactory());
      String property = context.getStringAttribute("property");
      if (!metaResultType.hasSetter(property)) {
        throw new BuilderException(
            "Ambiguous collection type for property '" + property + "'. You must specify 'javaType' or 'resultMap'.");
      }
    }
  }

  /**
   * 为mapper类绑定命名空间
   */
  private void bindMapperForNamespace() {
    //获取名称空间
    String namespace = builderAssistant.getCurrentNamespace();
    if (namespace != null) {
      Class<?> boundType = null;
      try {
        //根据名称空间反射获取类对象
        boundType = Resources.classForName(namespace);
      } catch (ClassNotFoundException e) {
        // ignore, bound type is not required
      }
      if (boundType != null && !configuration.hasMapper(boundType)) {
        // Spring may not know the real resource name so we set a flag
        // to prevent loading again this resource from the mapper interface
        // look at MapperAnnotationBuilder#loadXmlResource
        configuration.addLoadedResource("namespace:" + namespace);
        configuration.addMapper(boundType);
      }
    }
  }

}
