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

import org.apache.ibatis.binding.MapperRegistry;
import org.apache.ibatis.builder.CacheRefResolver;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.ResultMapResolver;
import org.apache.ibatis.builder.annotation.MethodResolver;
import org.apache.ibatis.builder.xml.XMLStatementBuilder;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.decorators.FifoCache;
import org.apache.ibatis.cache.decorators.LruCache;
import org.apache.ibatis.cache.decorators.SoftCache;
import org.apache.ibatis.cache.decorators.WeakCache;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.datasource.jndi.JndiDataSourceFactory;
import org.apache.ibatis.datasource.pooled.PooledDataSourceFactory;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSourceFactory;
import org.apache.ibatis.executor.*;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.executor.loader.cglib.CglibProxyFactory;
import org.apache.ibatis.executor.loader.javassist.JavassistProxyFactory;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.resultset.DefaultResultSetHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.logging.commons.JakartaCommonsLoggingImpl;
import org.apache.ibatis.logging.jdk14.Jdk14LoggingImpl;
import org.apache.ibatis.logging.log4j.Log4jImpl;
import org.apache.ibatis.logging.log4j2.Log4j2Impl;
import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.logging.slf4j.Slf4jImpl;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.InterceptorChain;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.scripting.LanguageDriverRegistry;
import org.apache.ibatis.scripting.defaults.RawLanguageDriver;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeAliasRegistry;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.util.*;
import java.util.function.BiFunction;

/**
 * @author Clinton Begin
 */
public class Configuration {

  /**
   * 环境对象
   */
  protected Environment environment;

  /**
   * 是否启动分页的安全检查
   * 主要是startIndex和size的检查
   */
  protected boolean safeRowBoundsEnabled;
  /**
   * 是否启动结果处理器的安全检查
   */
  protected boolean safeResultHandlerEnabled = true;
  /**
   * 是否开启下划线转驼峰
   */
  protected boolean mapUnderscoreToCamelCase;
  /**
   * 控制具有懒加载特性的对象的属性的加载, 配合懒加载开关使用
   * true   表示如果对具有懒加载特性的对象的任意调用会导致这个对象的完整加载
   * false  表示每种属性按照需要加载
   */
  protected boolean aggressiveLazyLoading;
  /**
   * 是否支持多结果集
   */
  protected boolean multipleResultSetsEnabled = true;
  /**
   * 是否使用主键生成
   */
  protected boolean useGeneratedKeys;
  /**
   * 是否使用列标签
   */
  protected boolean useColumnLabel = true;
  /**
   * 是否开启二级缓存,  ps:一级缓存session级别,默认有,而且关不掉
   */
  protected boolean cacheEnabled = true;
  /**
   * 是否对null值进行值设置
   */
  protected boolean callSettersOnNulls;
  /**
   * 是否使用参数的实际名称作为查询参数
   */
  protected boolean useActualParamName = true;
  /**
   * 查询结果为空时,是否返回空对象
   */
  protected boolean returnInstanceForEmptyRow;
  /**
   * 是否跳过sql语句中的空格
   */
  protected boolean shrinkWhitespacesInSql;

  /**
   * 日志前缀
   */
  protected String logPrefix;
  /**
   * LOG实现类
   */
  protected Class<? extends Log> logImpl;
  /**
   * 虚拟文件系统实现类
   */
  protected Class<? extends VFS> vfsImpl;
  /**
   * //todo
   */
  protected Class<?> defaultSqlProviderType;
  /**
   * 本地缓存级别, SESSION,STATEMENT
   */
  protected LocalCacheScope localCacheScope = LocalCacheScope.SESSION;
  /**
   * 处理null类型的jdbcType
   */
  protected JdbcType jdbcTypeForNull = JdbcType.OTHER;
  /**
   * 触发懒加载的方法集合
   */
  protected Set<String> lazyLoadTriggerMethods = new HashSet<>(Arrays.asList("equals", "clone", "hashCode", "toString"));
  /**
   * 查询语句的默认超时时间
   */
  protected Integer defaultStatementTimeout;
  /**
   * 默认查询的size
   */
  protected Integer defaultFetchSize;
  /**
   * 默认的结果集类型
   */
  protected ResultSetType defaultResultSetType;
  /**
   * 默认的sql执行器类型 Simple
   */
  protected ExecutorType defaultExecutorType = ExecutorType.SIMPLE;
  /**
   * 自动匹配的行为 ,部分匹配, 不匹配resultmap的嵌套
   */
  protected AutoMappingBehavior autoMappingBehavior = AutoMappingBehavior.PARTIAL;
  /**
   * 自动处理未知列的方式, NONE 不处理
   */
  protected AutoMappingUnknownColumnBehavior autoMappingUnknownColumnBehavior = AutoMappingUnknownColumnBehavior.NONE;

  /**
   * 内部维护的配置对象, 用于保存xml中读取的属性键值对
   */
  protected Properties variables = new Properties();
  /**
   * 反射工厂
   */
  protected ReflectorFactory reflectorFactory = new DefaultReflectorFactory();
  /**
   * 对象工厂
   */
  protected ObjectFactory objectFactory = new DefaultObjectFactory();
  /**
   * 对象包装工厂
   */
  protected ObjectWrapperFactory objectWrapperFactory = new DefaultObjectWrapperFactory();

  /**
   * 是否开启懒加载
   */
  protected boolean lazyLoadingEnabled = false;
  /**
   * 代理工厂
   */
  protected ProxyFactory proxyFactory = new JavassistProxyFactory(); // #224 Using internal Javassist instead of OGNL

  /**
   * 数据库厂商id
   */
  protected String databaseId;
  /**配置工厂类
   * Configuration factory class.
   * 用于创建加载反序列化的不可读的属性的Configuration对象
   * Used to create Configuration for loading deserialized unread properties.
   *
   * @see <a href='https://code.google.com/p/mybatis/issues/detail?id=300'>Issue 300 (google code)</a>
   */
  protected Class<?> configurationFactory;

  /**
   * Mapper注册器,内部维护了Mapper类和Mapper代理工厂的HashMap
   */
  protected final MapperRegistry mapperRegistry = new MapperRegistry(this);
  /**
   * 拦截器链, 内部维护了一个拦截器列表
   */
  protected final InterceptorChain interceptorChain = new InterceptorChain();
  /**
   * 类型处理器的注册器, 维护jdbcType和对应的处理器的关系
   */
  protected final TypeHandlerRegistry typeHandlerRegistry = new TypeHandlerRegistry(this);
  /**
   * 类别名注册器, 维护类和类别名的关系
   */
  protected final TypeAliasRegistry typeAliasRegistry = new TypeAliasRegistry();
  /**
   * 语言注册器, 维护语言驱动类和语言驱动对象的关系
   */
  protected final LanguageDriverRegistry languageRegistry = new LanguageDriverRegistry();

  /**
   * 维护MapperStatement对象 id和对象本身的map
   * 一个MappedStatement 对应的就是mapper文件中的一个CRUD的语句
   */
  protected final Map<String, MappedStatement> mappedStatements = new StrictMap<MappedStatement>("Mapped Statements collection")
      .conflictMessageProducer((savedValue, targetValue) ->
          ". please check " + savedValue.getResource() + " and " + targetValue.getResource());
  /**
   * 维护Cache对象 id和对象本身的map
   * 如果在mapper.xml中开启缓存, 会以namespace为id创建一个二级缓存cache对象
   */
  protected final Map<String, Cache> caches = new StrictMap<>("Caches collection");
  /**
   * 维护resultmap对象 id和对象本身的map
   * ResultMap对象就是mapper.xml中定义的<resultmap></resultmap>
   */
  protected final Map<String, ResultMap> resultMaps = new StrictMap<>("Result Maps collection");
  /**
   * 维护ParameterMap对象 id和对象本身的map
   * ParameterMap就是mapper.xml中定义的<parameterMap></parameterMap>
   * 一般不用
   */
  protected final Map<String, ParameterMap> parameterMaps = new StrictMap<>("Parameter Maps collection");
  /**
   * 维护KeyGenerator对象 id和对象本身的map
   * KeyGenerator是定义在 <selectKey></selectKey>
   */
  protected final Map<String, KeyGenerator> keyGenerators = new StrictMap<>("Key Generators collection");

  /**
   * 维护已加载的资源名称
   * 就是mapper.xml的名称或者是mapper.class的类名
   */
  protected final Set<String> loadedResources = new HashSet<>();
  /**
   * 维护sql代码段和sql节点id
   * <sql></sql>
   */
  protected final Map<String, XNode> sqlFragments = new StrictMap<>("XML fragments parsed from previous mappers");

  /**
   * 未完成处理的XMLStatement集合
   * 解析出错的XMLStatement会放入其中
   * 此处使用LinkedList的原因应是因为会涉及到移除元素操作,使用链表效率高
   */
  protected final Collection<XMLStatementBuilder> incompleteStatements = new LinkedList<>();
  /**
   * 未完成处理的缓存引用集合
   * 解析<cache-ref></cache-ref>标签出错时会放入其中
   * 或者是CacheNamespaceRef注解
   * 此处使用LinkedList的原因应是因为会涉及到移除元素操作,使用链表效率高
   */
  protected final Collection<CacheRefResolver> incompleteCacheRefs = new LinkedList<>();
  /**
   * 未完成处理的ResultMap集合
   * 此处使用LinkedList的原因应是因为会涉及到移除元素操作,使用链表效率高
   */
  protected final Collection<ResultMapResolver> incompleteResultMaps = new LinkedList<>();
  /**
   * 未完成处理的方法集合
   * 为Mapper.class中定义的方法
   * 此处使用LinkedList的原因应是因为会涉及到移除元素操作,使用链表效率高
   */
  protected final Collection<MethodResolver> incompleteMethods = new LinkedList<>();

  /**
   * 二级缓存的引用  namespace ==> referencedNamespace
   * A map holds cache-ref relationship. The key is the namespace that
   * references a cache bound to another namespace and the value is the
   * namespace which the actual cache is bound to.
   */
  protected final Map<String, String> cacheRefMap = new HashMap<>();

  /**
   * 构造函数
   * @param environment       环境对象
   */
  public Configuration(Environment environment) {
    //调用无参构造函数,初始化各种注册器
    this();
    this.environment = environment;
  }

  /**
   * 无参构造函数
   */
  public Configuration() {

    //初始化类别名注册器

    //事务工厂类别名注册
    typeAliasRegistry.registerAlias("JDBC", JdbcTransactionFactory.class);
    typeAliasRegistry.registerAlias("MANAGED", ManagedTransactionFactory.class);

    //数据源类别名注册
    typeAliasRegistry.registerAlias("JNDI", JndiDataSourceFactory.class);
    typeAliasRegistry.registerAlias("POOLED", PooledDataSourceFactory.class);
    typeAliasRegistry.registerAlias("UNPOOLED", UnpooledDataSourceFactory.class);

    //缓存类别名注册
    typeAliasRegistry.registerAlias("PERPETUAL", PerpetualCache.class);
    typeAliasRegistry.registerAlias("FIFO", FifoCache.class);
    typeAliasRegistry.registerAlias("LRU", LruCache.class);
    typeAliasRegistry.registerAlias("SOFT", SoftCache.class);
    typeAliasRegistry.registerAlias("WEAK", WeakCache.class);

    //数据库厂商类别名注册
    typeAliasRegistry.registerAlias("DB_VENDOR", VendorDatabaseIdProvider.class);

    //语言处理类别名注册
    typeAliasRegistry.registerAlias("XML", XMLLanguageDriver.class);
    typeAliasRegistry.registerAlias("RAW", RawLanguageDriver.class);

    //日志实现类别名注册
    typeAliasRegistry.registerAlias("SLF4J", Slf4jImpl.class);
    typeAliasRegistry.registerAlias("COMMONS_LOGGING", JakartaCommonsLoggingImpl.class);
    typeAliasRegistry.registerAlias("LOG4J", Log4jImpl.class);
    typeAliasRegistry.registerAlias("LOG4J2", Log4j2Impl.class);
    typeAliasRegistry.registerAlias("JDK_LOGGING", Jdk14LoggingImpl.class);
    typeAliasRegistry.registerAlias("STDOUT_LOGGING", StdOutImpl.class);
    typeAliasRegistry.registerAlias("NO_LOGGING", NoLoggingImpl.class);

    //代理工厂类别名注册
    typeAliasRegistry.registerAlias("CGLIB", CglibProxyFactory.class);
    typeAliasRegistry.registerAlias("JAVASSIST", JavassistProxyFactory.class);

    //语言注册器初始化
    languageRegistry.setDefaultDriverClass(XMLLanguageDriver.class);
    languageRegistry.register(RawLanguageDriver.class);
  }

  public String getLogPrefix() {
    return logPrefix;
  }

  public void setLogPrefix(String logPrefix) {
    this.logPrefix = logPrefix;
  }

  public Class<? extends Log> getLogImpl() {
    return logImpl;
  }

  public void setLogImpl(Class<? extends Log> logImpl) {
    if (logImpl != null) {
      this.logImpl = logImpl;
      LogFactory.useCustomLogging(this.logImpl);
    }
  }

  public Class<? extends VFS> getVfsImpl() {
    return this.vfsImpl;
  }

  public void setVfsImpl(Class<? extends VFS> vfsImpl) {
    if (vfsImpl != null) {
      this.vfsImpl = vfsImpl;
      VFS.addImplClass(this.vfsImpl);
    }
  }

  /**
   * Gets an applying type when omit a type on sql provider annotation(e.g. {@link org.apache.ibatis.annotations.SelectProvider}).
   *
   * @return the default type for sql provider annotation
   * @since 3.5.6
   */
  public Class<?> getDefaultSqlProviderType() {
    return defaultSqlProviderType;
  }

  /**
   * Sets an applying type when omit a type on sql provider annotation(e.g. {@link org.apache.ibatis.annotations.SelectProvider}).
   *
   * @param defaultSqlProviderType
   *          the default type for sql provider annotation
   * @since 3.5.6
   */
  public void setDefaultSqlProviderType(Class<?> defaultSqlProviderType) {
    this.defaultSqlProviderType = defaultSqlProviderType;
  }

  public boolean isCallSettersOnNulls() {
    return callSettersOnNulls;
  }

  public void setCallSettersOnNulls(boolean callSettersOnNulls) {
    this.callSettersOnNulls = callSettersOnNulls;
  }

  public boolean isUseActualParamName() {
    return useActualParamName;
  }

  public void setUseActualParamName(boolean useActualParamName) {
    this.useActualParamName = useActualParamName;
  }

  public boolean isReturnInstanceForEmptyRow() {
    return returnInstanceForEmptyRow;
  }

  public void setReturnInstanceForEmptyRow(boolean returnEmptyInstance) {
    this.returnInstanceForEmptyRow = returnEmptyInstance;
  }

  public boolean isShrinkWhitespacesInSql() {
    return shrinkWhitespacesInSql;
  }

  public void setShrinkWhitespacesInSql(boolean shrinkWhitespacesInSql) {
    this.shrinkWhitespacesInSql = shrinkWhitespacesInSql;
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(String databaseId) {
    this.databaseId = databaseId;
  }

  public Class<?> getConfigurationFactory() {
    return configurationFactory;
  }

  public void setConfigurationFactory(Class<?> configurationFactory) {
    this.configurationFactory = configurationFactory;
  }

  public boolean isSafeResultHandlerEnabled() {
    return safeResultHandlerEnabled;
  }

  public void setSafeResultHandlerEnabled(boolean safeResultHandlerEnabled) {
    this.safeResultHandlerEnabled = safeResultHandlerEnabled;
  }

  public boolean isSafeRowBoundsEnabled() {
    return safeRowBoundsEnabled;
  }

  public void setSafeRowBoundsEnabled(boolean safeRowBoundsEnabled) {
    this.safeRowBoundsEnabled = safeRowBoundsEnabled;
  }

  public boolean isMapUnderscoreToCamelCase() {
    return mapUnderscoreToCamelCase;
  }

  public void setMapUnderscoreToCamelCase(boolean mapUnderscoreToCamelCase) {
    this.mapUnderscoreToCamelCase = mapUnderscoreToCamelCase;
  }

  public void addLoadedResource(String resource) {
    loadedResources.add(resource);
  }

  public boolean isResourceLoaded(String resource) {
    return loadedResources.contains(resource);
  }

  public Environment getEnvironment() {
    return environment;
  }

  public void setEnvironment(Environment environment) {
    this.environment = environment;
  }

  public AutoMappingBehavior getAutoMappingBehavior() {
    return autoMappingBehavior;
  }

  public void setAutoMappingBehavior(AutoMappingBehavior autoMappingBehavior) {
    this.autoMappingBehavior = autoMappingBehavior;
  }

  /**
   * Gets the auto mapping unknown column behavior.
   *
   * @return the auto mapping unknown column behavior
   * @since 3.4.0
   */
  public AutoMappingUnknownColumnBehavior getAutoMappingUnknownColumnBehavior() {
    return autoMappingUnknownColumnBehavior;
  }

  /**
   * Sets the auto mapping unknown column behavior.
   *
   * @param autoMappingUnknownColumnBehavior
   *          the new auto mapping unknown column behavior
   * @since 3.4.0
   */
  public void setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior autoMappingUnknownColumnBehavior) {
    this.autoMappingUnknownColumnBehavior = autoMappingUnknownColumnBehavior;
  }

  public boolean isLazyLoadingEnabled() {
    return lazyLoadingEnabled;
  }

  public void setLazyLoadingEnabled(boolean lazyLoadingEnabled) {
    this.lazyLoadingEnabled = lazyLoadingEnabled;
  }

  public ProxyFactory getProxyFactory() {
    return proxyFactory;
  }

  public void setProxyFactory(ProxyFactory proxyFactory) {
    if (proxyFactory == null) {
      proxyFactory = new JavassistProxyFactory();
    }
    this.proxyFactory = proxyFactory;
  }

  public boolean isAggressiveLazyLoading() {
    return aggressiveLazyLoading;
  }

  public void setAggressiveLazyLoading(boolean aggressiveLazyLoading) {
    this.aggressiveLazyLoading = aggressiveLazyLoading;
  }

  public boolean isMultipleResultSetsEnabled() {
    return multipleResultSetsEnabled;
  }

  public void setMultipleResultSetsEnabled(boolean multipleResultSetsEnabled) {
    this.multipleResultSetsEnabled = multipleResultSetsEnabled;
  }

  public Set<String> getLazyLoadTriggerMethods() {
    return lazyLoadTriggerMethods;
  }

  public void setLazyLoadTriggerMethods(Set<String> lazyLoadTriggerMethods) {
    this.lazyLoadTriggerMethods = lazyLoadTriggerMethods;
  }

  public boolean isUseGeneratedKeys() {
    return useGeneratedKeys;
  }

  public void setUseGeneratedKeys(boolean useGeneratedKeys) {
    this.useGeneratedKeys = useGeneratedKeys;
  }

  public ExecutorType getDefaultExecutorType() {
    return defaultExecutorType;
  }

  public void setDefaultExecutorType(ExecutorType defaultExecutorType) {
    this.defaultExecutorType = defaultExecutorType;
  }

  public boolean isCacheEnabled() {
    return cacheEnabled;
  }

  public void setCacheEnabled(boolean cacheEnabled) {
    this.cacheEnabled = cacheEnabled;
  }

  public Integer getDefaultStatementTimeout() {
    return defaultStatementTimeout;
  }

  public void setDefaultStatementTimeout(Integer defaultStatementTimeout) {
    this.defaultStatementTimeout = defaultStatementTimeout;
  }

  /**
   * Gets the default fetch size.
   *
   * @return the default fetch size
   * @since 3.3.0
   */
  public Integer getDefaultFetchSize() {
    return defaultFetchSize;
  }

  /**
   * Sets the default fetch size.
   *
   * @param defaultFetchSize
   *          the new default fetch size
   * @since 3.3.0
   */
  public void setDefaultFetchSize(Integer defaultFetchSize) {
    this.defaultFetchSize = defaultFetchSize;
  }

  /**
   * Gets the default result set type.
   *
   * @return the default result set type
   * @since 3.5.2
   */
  public ResultSetType getDefaultResultSetType() {
    return defaultResultSetType;
  }

  /**
   * Sets the default result set type.
   *
   * @param defaultResultSetType
   *          the new default result set type
   * @since 3.5.2
   */
  public void setDefaultResultSetType(ResultSetType defaultResultSetType) {
    this.defaultResultSetType = defaultResultSetType;
  }

  public boolean isUseColumnLabel() {
    return useColumnLabel;
  }

  public void setUseColumnLabel(boolean useColumnLabel) {
    this.useColumnLabel = useColumnLabel;
  }

  public LocalCacheScope getLocalCacheScope() {
    return localCacheScope;
  }

  public void setLocalCacheScope(LocalCacheScope localCacheScope) {
    this.localCacheScope = localCacheScope;
  }

  public JdbcType getJdbcTypeForNull() {
    return jdbcTypeForNull;
  }

  public void setJdbcTypeForNull(JdbcType jdbcTypeForNull) {
    this.jdbcTypeForNull = jdbcTypeForNull;
  }

  public Properties getVariables() {
    return variables;
  }

  public void setVariables(Properties variables) {
    this.variables = variables;
  }

  public TypeHandlerRegistry getTypeHandlerRegistry() {
    return typeHandlerRegistry;
  }

  /**
   * Set a default {@link TypeHandler} class for {@link Enum}.
   * A default {@link TypeHandler} is {@link org.apache.ibatis.type.EnumTypeHandler}.
   * @param typeHandler a type handler class for {@link Enum}
   * @since 3.4.5
   */
  public void setDefaultEnumTypeHandler(Class<? extends TypeHandler> typeHandler) {
    if (typeHandler != null) {
      getTypeHandlerRegistry().setDefaultEnumTypeHandler(typeHandler);
    }
  }

  public TypeAliasRegistry getTypeAliasRegistry() {
    return typeAliasRegistry;
  }

  /**
   * Gets the mapper registry.
   *
   * @return the mapper registry
   * @since 3.2.2
   */
  public MapperRegistry getMapperRegistry() {
    return mapperRegistry;
  }

  public ReflectorFactory getReflectorFactory() {
    return reflectorFactory;
  }

  public void setReflectorFactory(ReflectorFactory reflectorFactory) {
    this.reflectorFactory = reflectorFactory;
  }

  public ObjectFactory getObjectFactory() {
    return objectFactory;
  }

  public void setObjectFactory(ObjectFactory objectFactory) {
    this.objectFactory = objectFactory;
  }

  public ObjectWrapperFactory getObjectWrapperFactory() {
    return objectWrapperFactory;
  }

  public void setObjectWrapperFactory(ObjectWrapperFactory objectWrapperFactory) {
    this.objectWrapperFactory = objectWrapperFactory;
  }

  /**
   * Gets the interceptors.
   *
   * @return the interceptors
   * @since 3.2.2
   */
  public List<Interceptor> getInterceptors() {
    return interceptorChain.getInterceptors();
  }

  public LanguageDriverRegistry getLanguageRegistry() {
    return languageRegistry;
  }

  public void setDefaultScriptingLanguage(Class<? extends LanguageDriver> driver) {
    if (driver == null) {
      driver = XMLLanguageDriver.class;
    }
    getLanguageRegistry().setDefaultDriverClass(driver);
  }

  public LanguageDriver getDefaultScriptingLanguageInstance() {
    return languageRegistry.getDefaultDriver();
  }

  /**
   * Gets the language driver.
   *
   * @param langClass
   *          the lang class
   * @return the language driver
   * @since 3.5.1
   */
  public LanguageDriver getLanguageDriver(Class<? extends LanguageDriver> langClass) {
    if (langClass == null) {
      return languageRegistry.getDefaultDriver();
    }
    languageRegistry.register(langClass);
    return languageRegistry.getDriver(langClass);
  }

  /**
   * Gets the default scripting lanuage instance.
   *
   * @return the default scripting lanuage instance
   * @deprecated Use {@link #getDefaultScriptingLanguageInstance()}
   */
  @Deprecated
  public LanguageDriver getDefaultScriptingLanuageInstance() {
    return getDefaultScriptingLanguageInstance();
  }

  /**通过一个给定的对象构建一个MetaObject
   * @param object            给定的对象
   * @return                  包含对象各种信息的元对象, 包括属性,getter/setter方法等
   */
  public MetaObject newMetaObject(Object object) {
    return MetaObject.forObject(object, objectFactory, objectWrapperFactory, reflectorFactory);
  }

  /**构建一个参数处理器对象       mybatis对外提供的Interceptor接口,允许对ParameterHandler的方法就行拦截处理
   * @param mappedStatement   mappedStatement
   * @param parameterObject   参数对象
   * @param boundSql          boundSql
   * @return                  参数处理器对象ParameterHandler
   */
  public ParameterHandler newParameterHandler(MappedStatement mappedStatement, Object parameterObject, BoundSql boundSql) {
    //根据语言驱动构建一个参数处理器
    ParameterHandler parameterHandler = mappedStatement.getLang().createParameterHandler(mappedStatement, parameterObject, boundSql);
    //将参数处理器注册到每个Interceptor中
    parameterHandler = (ParameterHandler) interceptorChain.pluginAll(parameterHandler);
    return parameterHandler;
  }

  /**构建一个结果集处理器         mybatis对外提供的Interceptor接口,允许对ResultSetHandler的方法就行拦截处理
   * @param executor          sql执行器
   * @param mappedStatement   mappedStatement
   * @param rowBounds         mybatis自带的分页对象
   * @param parameterHandler  参数处理器
   * @param resultHandler     结果处理器
   * @param boundSql          boundSql
   * @return                  结果集处理器
   */
  public ResultSetHandler newResultSetHandler(Executor executor, MappedStatement mappedStatement, RowBounds rowBounds, ParameterHandler parameterHandler,
      ResultHandler resultHandler, BoundSql boundSql) {
    //创建一个默认的结果集处理器DefaultResultSetHandler
    ResultSetHandler resultSetHandler = new DefaultResultSetHandler(executor, mappedStatement, parameterHandler, resultHandler, boundSql, rowBounds);
    //将结果集处理器注册到每个Interceptor中
    resultSetHandler = (ResultSetHandler) interceptorChain.pluginAll(resultSetHandler);
    return resultSetHandler;
  }

  /**构建一个Statement处理器     mybatis对外提供的Interceptor接口,允许对StatementHandler的方法就行拦截处理
   * @param executor          sql执行器
   * @param mappedStatement   mappedStatement
   * @param parameterObject   参数对象
   * @param rowBounds         mybatis自带的分页对象
   * @param resultHandler     结果处理器
   * @param boundSql          boundSql
   * @return                  Statement处理器
   */
  public StatementHandler newStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
    //返回一个RoutingStatementHandler,通过StatementType进行后续的路由
    StatementHandler statementHandler = new RoutingStatementHandler(executor, mappedStatement, parameterObject, rowBounds, resultHandler, boundSql);
    //将StatementHandler注册到每个Interceptor中
    statementHandler = (StatementHandler) interceptorChain.pluginAll(statementHandler);
    return statementHandler;
  }

  /**构建一个sql执行器
   * @param transaction       事务对象
   * @return                  sql执行器
   */
  public Executor newExecutor(Transaction transaction) {
    //调用内部方法, 返回一个默认处理器类型的sql执行器
    return newExecutor(transaction, defaultExecutorType);
  }

  /**构建一个sql执行器           mybatis对外提供的Executor接口,允许对StatementHandler的方法就行拦截处理
   * @param transaction       事务对象
   * @param executorType      执行器类型, SIMPLE, REUSE, BATCH
   * @return                  sql执行器
   */
  public Executor newExecutor(Transaction transaction, ExecutorType executorType) {
    //给定的执行器类型为空时,返回默认的执行器,否则返回给定的执行器类型
    executorType = executorType == null ? defaultExecutorType : executorType;
    //执行器类型还为null时,返回SIMPLE类型的处理器,否则返回给定的执行器
    executorType = executorType == null ? ExecutorType.SIMPLE : executorType;
    Executor executor;
    if (ExecutorType.BATCH == executorType) {
      //返回批处理类型的执行器
      executor = new BatchExecutor(this, transaction);
    } else if (ExecutorType.REUSE == executorType) {
      //返回可重用的执行器
      executor = new ReuseExecutor(this, transaction);
    } else {
      //返回一个简单类型的执行器
      executor = new SimpleExecutor(this, transaction);
    }
    if (cacheEnabled) {
      //若开启了二级缓存,使用缓存执行器进行包装
      executor = new CachingExecutor(executor);
    }
    //将sql执行器注册到每个Interceptor中
    executor = (Executor) interceptorChain.pluginAll(executor);
    return executor;
  }

  public void addKeyGenerator(String id, KeyGenerator keyGenerator) {
    keyGenerators.put(id, keyGenerator);
  }

  public Collection<String> getKeyGeneratorNames() {
    return keyGenerators.keySet();
  }

  public Collection<KeyGenerator> getKeyGenerators() {
    return keyGenerators.values();
  }

  public KeyGenerator getKeyGenerator(String id) {
    return keyGenerators.get(id);
  }

  public boolean hasKeyGenerator(String id) {
    return keyGenerators.containsKey(id);
  }

  public void addCache(Cache cache) {
    caches.put(cache.getId(), cache);
  }

  public Collection<String> getCacheNames() {
    return caches.keySet();
  }

  public Collection<Cache> getCaches() {
    return caches.values();
  }

  public Cache getCache(String id) {
    return caches.get(id);
  }

  public boolean hasCache(String id) {
    return caches.containsKey(id);
  }

  public void addResultMap(ResultMap rm) {
    resultMaps.put(rm.getId(), rm);
    checkLocallyForDiscriminatedNestedResultMaps(rm);
    checkGloballyForDiscriminatedNestedResultMaps(rm);
  }

  public Collection<String> getResultMapNames() {
    return resultMaps.keySet();
  }

  public Collection<ResultMap> getResultMaps() {
    return resultMaps.values();
  }

  public ResultMap getResultMap(String id) {
    return resultMaps.get(id);
  }

  public boolean hasResultMap(String id) {
    return resultMaps.containsKey(id);
  }

  public void addParameterMap(ParameterMap pm) {
    parameterMaps.put(pm.getId(), pm);
  }

  public Collection<String> getParameterMapNames() {
    return parameterMaps.keySet();
  }

  public Collection<ParameterMap> getParameterMaps() {
    return parameterMaps.values();
  }

  public ParameterMap getParameterMap(String id) {
    return parameterMaps.get(id);
  }

  public boolean hasParameterMap(String id) {
    return parameterMaps.containsKey(id);
  }

  public void addMappedStatement(MappedStatement ms) {
    mappedStatements.put(ms.getId(), ms);
  }

  public Collection<String> getMappedStatementNames() {
    buildAllStatements();
    return mappedStatements.keySet();
  }

  public Collection<MappedStatement> getMappedStatements() {
    buildAllStatements();
    return mappedStatements.values();
  }

  public Collection<XMLStatementBuilder> getIncompleteStatements() {
    return incompleteStatements;
  }

  public void addIncompleteStatement(XMLStatementBuilder incompleteStatement) {
    incompleteStatements.add(incompleteStatement);
  }

  public Collection<CacheRefResolver> getIncompleteCacheRefs() {
    return incompleteCacheRefs;
  }

  public void addIncompleteCacheRef(CacheRefResolver incompleteCacheRef) {
    incompleteCacheRefs.add(incompleteCacheRef);
  }

  public Collection<ResultMapResolver> getIncompleteResultMaps() {
    return incompleteResultMaps;
  }

  public void addIncompleteResultMap(ResultMapResolver resultMapResolver) {
    incompleteResultMaps.add(resultMapResolver);
  }

  public void addIncompleteMethod(MethodResolver builder) {
    incompleteMethods.add(builder);
  }

  public Collection<MethodResolver> getIncompleteMethods() {
    return incompleteMethods;
  }

  public MappedStatement getMappedStatement(String id) {
    return this.getMappedStatement(id, true);
  }

  public MappedStatement getMappedStatement(String id, boolean validateIncompleteStatements) {
    if (validateIncompleteStatements) {
      buildAllStatements();
    }
    return mappedStatements.get(id);
  }

  public Map<String, XNode> getSqlFragments() {
    return sqlFragments;
  }

  public void addInterceptor(Interceptor interceptor) {
    interceptorChain.addInterceptor(interceptor);
  }

  public void addMappers(String packageName, Class<?> superType) {
    mapperRegistry.addMappers(packageName, superType);
  }

  public void addMappers(String packageName) {
    mapperRegistry.addMappers(packageName);
  }

  public <T> void addMapper(Class<T> type) {
    mapperRegistry.addMapper(type);
  }

  public <T> T getMapper(Class<T> type, SqlSession sqlSession) {
    return mapperRegistry.getMapper(type, sqlSession);
  }

  public boolean hasMapper(Class<?> type) {
    return mapperRegistry.hasMapper(type);
  }

  public boolean hasStatement(String statementName) {
    return hasStatement(statementName, true);
  }

  public boolean hasStatement(String statementName, boolean validateIncompleteStatements) {
    if (validateIncompleteStatements) {
      buildAllStatements();
    }
    return mappedStatements.containsKey(statementName);
  }

  public void addCacheRef(String namespace, String referencedNamespace) {
    cacheRefMap.put(namespace, referencedNamespace);
  }

  /* 处理所有未完成处理的statement节点
   * Parses all the unprocessed statement nodes in the cache. It is recommended
   * to call this method once all the mappers are added as it provides fail-fast
   * statement validation.
   */
  protected void buildAllStatements() {
    //解析未完成的resultmap
    parsePendingResultMaps();
    if (!incompleteCacheRefs.isEmpty()) {
      //处理未完成的<cache-ref>和注解
      synchronized (incompleteCacheRefs) {
        //解析成功之后从未完成集合中移除
        incompleteCacheRefs.removeIf(x -> x.resolveCacheRef() != null);
      }
    }
    if (!incompleteStatements.isEmpty()) {
      //处理未完成的statement
      synchronized (incompleteStatements) {
        //解析成功后从未完成集合中移除
        incompleteStatements.removeIf(x -> {
          x.parseStatementNode();
          return true;
        });
      }
    }
    if (!incompleteMethods.isEmpty()) {
      //处理未完成的方法
      synchronized (incompleteMethods) {
        //解析成功后从未完成集合中移除
        incompleteMethods.removeIf(x -> {
          x.resolve();
          return true;
        });
      }
    }
  }

  /**
   * 解析所有未完成的resultmap
   */
  private void parsePendingResultMaps() {
    if (incompleteResultMaps.isEmpty()) {
      return;
    }
    synchronized (incompleteResultMaps) {
      boolean resolved;
      IncompleteElementException ex = null;
      do {
        resolved = false;
        Iterator<ResultMapResolver> iterator = incompleteResultMaps.iterator();
        while (iterator.hasNext()) {
          try {
            //遍历处理未完成的resultmap
            iterator.next().resolve();
            //完成后从集合中移除
            iterator.remove();
            resolved = true;
          } catch (IncompleteElementException e) {
            ex = e;
          }
        }
      } while (resolved);
      if (!incompleteResultMaps.isEmpty() && ex != null) {
        // At least one result map is unresolvable.
        throw ex;
      }
    }
  }

  /**
   * Extracts namespace from fully qualified statement id.
   *
   * @param statementId
   *          the statement id
   * @return namespace or null when id does not contain period.
   */
  protected String extractNamespace(String statementId) {
    int lastPeriod = statementId.lastIndexOf('.');
    return lastPeriod > 0 ? statementId.substring(0, lastPeriod) : null;
  }

  /**检查嵌套层级的resultap的鉴别器
   * @param rm          ResultMap
   */
  // Slow but a one time cost. A better solution is welcome.
  protected void checkGloballyForDiscriminatedNestedResultMaps(ResultMap rm) {
    //如果rm是有嵌套的resultmap
    if (rm.hasNestedResultMaps()) {
      //遍历所有已处理的resultmap集合
      for (Map.Entry<String, ResultMap> entry : resultMaps.entrySet()) {
        Object value = entry.getValue();
        if (value instanceof ResultMap) {
          ResultMap entryResultMap = (ResultMap) value;
          if (!entryResultMap.hasNestedResultMaps() && entryResultMap.getDiscriminator() != null) {
            //如果当前resultmap不是层级resultmap并且指定了鉴别器
            Collection<String> discriminatedResultMapNames = entryResultMap.getDiscriminator().getDiscriminatorMap().values();
            if (discriminatedResultMapNames.contains(rm.getId())) {
              //如果鉴别器的resultmap中包含当前rm, 给当前遍历的resultmap指定为层级resultmap
              entryResultMap.forceNestedResultMaps();
            }
          }
        }
      }
    }
  }

  /**根据resultmap中的鉴别的嵌套resultmap来设定当前resultmap的是否为嵌套resultmap属性
   * @param rm            当前resultmap
   */
  // Slow but a one time cost. A better solution is welcome.
  protected void checkLocallyForDiscriminatedNestedResultMaps(ResultMap rm) {
    if (!rm.hasNestedResultMaps() && rm.getDiscriminator() != null) {
      //如果当前resultmap不是嵌套的resultmap并且指定了鉴别器
      for (Map.Entry<String, String> entry : rm.getDiscriminator().getDiscriminatorMap().entrySet()) {
        String discriminatedResultMapName = entry.getValue();
        if (hasResultMap(discriminatedResultMapName)) {
          ResultMap discriminatedResultMap = resultMaps.get(discriminatedResultMapName);
          if (discriminatedResultMap.hasNestedResultMaps()) {
            //如果当期鉴别器的resultmap是嵌套层级的,设置rm的嵌套层级属性为true
            rm.forceNestedResultMaps();
            break;
          }
        }
      }
    }
  }

  protected static class StrictMap<V> extends HashMap<String, V> {

    private static final long serialVersionUID = -4950446264854982944L;
    private final String name;
    private BiFunction<V, V, String> conflictMessageProducer;

    public StrictMap(String name, int initialCapacity, float loadFactor) {
      super(initialCapacity, loadFactor);
      this.name = name;
    }

    public StrictMap(String name, int initialCapacity) {
      super(initialCapacity);
      this.name = name;
    }

    public StrictMap(String name) {
      super();
      this.name = name;
    }

    public StrictMap(String name, Map<String, ? extends V> m) {
      super(m);
      this.name = name;
    }

    /**
     * Assign a function for producing a conflict error message when contains value with the same key.
     * <p>
     * function arguments are 1st is saved value and 2nd is target value.
     * @param conflictMessageProducer A function for producing a conflict error message
     * @return a conflict error message
     * @since 3.5.0
     */
    public StrictMap<V> conflictMessageProducer(BiFunction<V, V, String> conflictMessageProducer) {
      this.conflictMessageProducer = conflictMessageProducer;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V put(String key, V value) {
      if (containsKey(key)) {
        throw new IllegalArgumentException(name + " already contains value for " + key
            + (conflictMessageProducer == null ? "" : conflictMessageProducer.apply(super.get(key), value)));
      }
      if (key.contains(".")) {
        final String shortKey = getShortName(key);
        if (super.get(shortKey) == null) {
          super.put(shortKey, value);
        } else {
          super.put(shortKey, (V) new Ambiguity(shortKey));
        }
      }
      return super.put(key, value);
    }

    @Override
    public V get(Object key) {
      V value = super.get(key);
      if (value == null) {
        throw new IllegalArgumentException(name + " does not contain value for " + key);
      }
      if (value instanceof Ambiguity) {
        throw new IllegalArgumentException(((Ambiguity) value).getSubject() + " is ambiguous in " + name
            + " (try using the full name including the namespace, or rename one of the entries)");
      }
      return value;
    }

    protected static class Ambiguity {
      private final String subject;

      public Ambiguity(String subject) {
        this.subject = subject;
      }

      public String getSubject() {
        return subject;
      }
    }

    private String getShortName(String key) {
      final String[] keyParts = key.split("\\.");
      return keyParts[keyParts.length - 1];
    }
  }

}
