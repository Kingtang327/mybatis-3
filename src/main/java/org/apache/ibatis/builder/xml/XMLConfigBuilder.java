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

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.*;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

/**核心配置文件mybatis-config.xml解析器
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

  /**
   * 是否解析已经解析过配置文件
   */
  private boolean parsed;
  /**
   * 解析器
   */
  private final XPathParser parser;
  /**
   * 环境参数
   */
  private String environment;
  /**
   * 反射器工厂
   */
  private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

  /**构造函数
   * @param reader        输入字符流
   */
  public XMLConfigBuilder(Reader reader) {
    this(reader, null, null);
  }

  /**构造函数
   * @param reader        输入字符流
   * @param environment   环境参数
   */
  public XMLConfigBuilder(Reader reader, String environment) {
    this(reader, environment, null);
  }

  /**构造函数
   * @param reader        输入字符流
   * @param environment   环境参数
   * @param props         配置信息
   */
  public XMLConfigBuilder(Reader reader, String environment, Properties props) {
    this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  /**构造函数
   * @param inputStream   输入流
   */
  public XMLConfigBuilder(InputStream inputStream) {
    this(inputStream, null, null);
  }

  /**构造函数
   * @param inputStream   输入流
   * @param environment   环境参数
   */
  public XMLConfigBuilder(InputStream inputStream, String environment) {
    this(inputStream, environment, null);
  }

  /**构造函数
   * @param inputStream   输入流
   * @param environment   环境参数
   * @param props         配置信息
   */
  public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
    this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  /**构造函数
   * @param parser        xml解析器
   * @param environment   环境参数
   * @param props         配置信息
   */
  private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
    //调用父类构造函数
    super(new Configuration());
    ErrorContext.instance().resource("SQL Mapper Configuration");
    //设置类属性
    this.configuration.setVariables(props);
    this.parsed = false;
    this.environment = environment;
    this.parser = parser;
  }

  /**解析配置configuration节点
   * @return              核心配置对象
   */
  public Configuration parse() {
    if (parsed) {
      //不允许重复解析
      throw new BuilderException("Each XMLConfigBuilder can only be used once.");
    }
    //设置解析标志
    parsed = true;
    //解析configuration节点
    parseConfiguration(parser.evalNode("/configuration"));
    return configuration;
  }

  /**解析configuration节点
   * @param root          xml的节点
   */
  private void parseConfiguration(XNode root) {
    try {
      // issue #117 read properties first
      //解析properties
      propertiesElement(root.evalNode("properties"));
      //解析setting
      Properties settings = settingsAsProperties(root.evalNode("settings"));
      //VFS含义是虚拟文件系统；主要是通过程序能够方便读取本地文件系统、FTP文件系统等系统中的文件资源
      loadCustomVfs(settings);
      //自定义log实现
      loadCustomLogImpl(settings);
      //处理别名
      typeAliasesElement(root.evalNode("typeAliases"));
      //处理插件
      pluginElement(root.evalNode("plugins"));
      //处理自定义对象工厂
      objectFactoryElement(root.evalNode("objectFactory"));
      //处理自定义对象包装工厂
      objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
      //处理反射工厂
      reflectorFactoryElement(root.evalNode("reflectorFactory"));
      //解析自定义setting,并设置到Configuration
      settingsElement(settings);
      // read it after objectFactory and objectWrapperFactory issue #631
      //解析环境配置
      environmentsElement(root.evalNode("environments"));
      //解析数据库提供厂商
      databaseIdProviderElement(root.evalNode("databaseIdProvider"));
      //解析类型处理器
      typeHandlerElement(root.evalNode("typeHandlers"));
      //解析mappers节点
      mapperElement(root.evalNode("mappers"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
    }
  }

  /**解析settings配置
   * @param context         settings节点
   * @return                Properties
   */
  private Properties settingsAsProperties(XNode context) {
    if (context == null) {
      return new Properties();
    }
    //读取节点的到Properties
    Properties props = context.getChildrenAsProperties();
    // Check that all settings are known to the configuration class
    //使用反射器工厂创建一个Configuration类的元数据类对象
    MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
    for (Object key : props.keySet()) {
      //遍历Properties的key
      if (!metaConfig.hasSetter(String.valueOf(key))) {
        //如果Configuration中没有找到该key的setter方法,抛出异常
        throw new BuilderException("The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
      }
    }
    return props;
  }

  /**设置虚拟文件系统的实现类型
   * @param props                     配置信息
   * @throws ClassNotFoundException   异常
   */
  private void loadCustomVfs(Properties props) throws ClassNotFoundException {
    //获取配置
    String value = props.getProperty("vfsImpl");
    if (value != null) {
      String[] clazzes = value.split(",");
      for (String clazz : clazzes) {
        if (!clazz.isEmpty()) {
          @SuppressWarnings("unchecked")
          //反射创建对象
          Class<? extends VFS> vfsImpl = (Class<? extends VFS>)Resources.classForName(clazz);
          configuration.setVfsImpl(vfsImpl);
        }
      }
    }
  }

  /**解析日志实现类
   * @param props          配置信息
   */
  private void loadCustomLogImpl(Properties props) {
    //解析log的实现类对象
    Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
    configuration.setLogImpl(logImpl);
  }

  /**处理别名
   * @param parent            xml的节点
   */
  private void typeAliasesElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          String typeAliasPackage = child.getStringAttribute("name");
          //根据包名解析类别名
          configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
        } else {
          //解析别名和类型的配置
          String alias = child.getStringAttribute("alias");
          String type = child.getStringAttribute("type");
          try {
            //将别名和类对象注册到别名注册器中
            Class<?> clazz = Resources.classForName(type);
            if (alias == null) {
              typeAliasRegistry.registerAlias(clazz);
            } else {
              typeAliasRegistry.registerAlias(alias, clazz);
            }
          } catch (ClassNotFoundException e) {
            throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
          }
        }
      }
    }
  }

  /**解析自定义插件
   * @param parent            xml的节点
   * @throws Exception        异常
   */
  private void pluginElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        //解析插件节点
        String interceptor = child.getStringAttribute("interceptor");
        //解析插件的配置
        Properties properties = child.getChildrenAsProperties();
        //使用构造函数初始对象
        Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).getDeclaredConstructor().newInstance();
        //设置interceptor的属性值
        interceptorInstance.setProperties(properties);
        //将interceptor加入到拦截器链
        configuration.addInterceptor(interceptorInstance);
      }
    }
  }

  /**处理自定义对象工厂
   * @param context           xml的节点
   * @throws Exception        异常
   */
  private void objectFactoryElement(XNode context) throws Exception {
    if (context != null) {
      //解析对象工厂类型
      String type = context.getStringAttribute("type");
      //获取属性
      Properties properties = context.getChildrenAsProperties();
      //使用构造函数初始对象
      ObjectFactory factory = (ObjectFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      //设置属性
      factory.setProperties(properties);
      //设置对象工厂
      configuration.setObjectFactory(factory);
    }
  }

  /**处理自定义对象包装工厂
   * @param context           xml的节点
   * @throws Exception        异常
   */
  private void objectWrapperFactoryElement(XNode context) throws Exception {
    if (context != null) {
      //解析对象包装工厂类型
      String type = context.getStringAttribute("type");
      //使用构造函数初始对象
      ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      //设置对象包装工厂
      configuration.setObjectWrapperFactory(factory);
    }
  }

  /**处理反射工厂
   * @param context           xml的节点
   * @throws Exception        异常
   */
  private void reflectorFactoryElement(XNode context) throws Exception {
    if (context != null) {
      //解析反射工厂类型
      String type = context.getStringAttribute("type");
      //使用构造函数初始对象
      ReflectorFactory factory = (ReflectorFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      //设置反射工厂
      configuration.setReflectorFactory(factory);
    }
  }

  /**解析properties
   * @param context           xml的节点
   * @throws Exception        异常
   */
  private void propertiesElement(XNode context) throws Exception {
    if (context != null) {
      //读取所有子节点构建一个Properties, 遍历读取name和value属性,然后进行赋值
      Properties defaults = context.getChildrenAsProperties();
      //读取resource
      String resource = context.getStringAttribute("resource");
      //读取url
      String url = context.getStringAttribute("url");
      if (resource != null && url != null) {
        throw new BuilderException("The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
      }
      if (resource != null) {
        //从resource中解析Properties,并放入默认的Properties
        defaults.putAll(Resources.getResourceAsProperties(resource));
      } else if (url != null) {
        //从url中解析Properties,并放入默认的Properties
        defaults.putAll(Resources.getUrlAsProperties(url));
      }
      Properties vars = configuration.getVariables();
      if (vars != null) {
        defaults.putAll(vars);
      }
      //合并configuration中已存在的配置和默认配置,并设置到configuration中
      parser.setVariables(defaults);
      configuration.setVariables(defaults);
    }
  }

  /**根据配置信息设置configuration的属性
   * @param props             配置信息
   */
  private void settingsElement(Properties props) {
    configuration.setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
    configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
    configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
    configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
    configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
    configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
    configuration.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
    configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
    configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
    configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
    configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
    configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
    configuration.setDefaultResultSetType(resolveResultSetType(props.getProperty("defaultResultSetType")));
    configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
    configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
    configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
    configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
    configuration.setLazyLoadTriggerMethods(stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
    configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
    configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
    configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
    configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
    configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
    configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
    configuration.setLogPrefix(props.getProperty("logPrefix"));
    configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
    configuration.setShrinkWhitespacesInSql(booleanValueOf(props.getProperty("shrinkWhitespacesInSql"), false));
    configuration.setDefaultSqlProviderType(resolveClass(props.getProperty("defaultSqlProviderType")));
  }

  /**解析环境配置
   * @param context           xml的节点
   * @throws Exception        异常
   */
  private void environmentsElement(XNode context) throws Exception {
    if (context != null) {
      if (environment == null) {
        environment = context.getStringAttribute("default");
      }
      for (XNode child : context.getChildren()) {
        String id = child.getStringAttribute("id");
        if (isSpecifiedEnvironment(id)) {
          //解析事务管理器
          TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
          //解析数据源配置
          DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
          //获取数据源
          DataSource dataSource = dsFactory.getDataSource();
          Environment.Builder environmentBuilder = new Environment.Builder(id)
              .transactionFactory(txFactory)
              .dataSource(dataSource);
          //设置环境信息
          configuration.setEnvironment(environmentBuilder.build());
        }
      }
    }
  }

  /**解析数据库提供厂商
   * @param context           xml的节点
   * @throws Exception        异常
   */
  private void databaseIdProviderElement(XNode context) throws Exception {
    DatabaseIdProvider databaseIdProvider = null;
    if (context != null) {
      String type = context.getStringAttribute("type");
      // awful patch to keep backward compatibility
      if ("VENDOR".equals(type)) {
        type = "DB_VENDOR";
      }
      Properties properties = context.getChildrenAsProperties();
      databaseIdProvider = (DatabaseIdProvider) resolveClass(type).getDeclaredConstructor().newInstance();
      databaseIdProvider.setProperties(properties);
    }
    Environment environment = configuration.getEnvironment();
    if (environment != null && databaseIdProvider != null) {
      String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
      configuration.setDatabaseId(databaseId);
    }
  }

  /**解析事务管理器
   * @param context           xml的节点
   * @return                  事务管理器
   * @throws Exception        异常
   */
  private TransactionFactory transactionManagerElement(XNode context) throws Exception {
    if (context != null) {
      //解析事务管理器类型
      String type = context.getStringAttribute("type");
      //获取配置信息
      Properties props = context.getChildrenAsProperties();
      //使用构造函数初始对象
      TransactionFactory factory = (TransactionFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a TransactionFactory.");
  }

  /**解析数据源配置
   * @param context           xml的节点
   * @return                  数据源工厂
   * @throws Exception        异常
   */
  private DataSourceFactory dataSourceElement(XNode context) throws Exception {
    if (context != null) {
      //解析数据源类型
      String type = context.getStringAttribute("type");
      //获取配置信息
      Properties props = context.getChildrenAsProperties();
      //使用构造函数初始对象
      DataSourceFactory factory = (DataSourceFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a DataSourceFactory.");
  }

  /**解析类型处理器
   * @param parent           xml的节点
   */
  private void typeHandlerElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          //指定包名方式配置
          String typeHandlerPackage = child.getStringAttribute("name");
          //指定包下进行扫描,找到所有为TypeHandler.class的class,然后进行注册
          //会排除接口,匿名内部类,还有抽象类
          typeHandlerRegistry.register(typeHandlerPackage);
        } else {
          //指定单个的处理器
          String javaTypeName = child.getStringAttribute("javaType");
          String jdbcTypeName = child.getStringAttribute("jdbcType");
          String handlerTypeName = child.getStringAttribute("handler");
          Class<?> javaTypeClass = resolveClass(javaTypeName);
          JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
          Class<?> typeHandlerClass = resolveClass(handlerTypeName);
          if (javaTypeClass != null) {
            if (jdbcType == null) {
              typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
            } else {
              typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
            }
          } else {
            typeHandlerRegistry.register(typeHandlerClass);
          }
        }
      }
    }
  }

  /**解析mappers节点
   * @param parent          xml的节点
   * @throws Exception      异常
   */
  private void mapperElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          String mapperPackage = child.getStringAttribute("name");
          //通过报名解析,所有mapper类
          //并且注册到mapperRegistry中
          configuration.addMappers(mapperPackage);
        } else {
          String resource = child.getStringAttribute("resource");
          String url = child.getStringAttribute("url");
          String mapperClass = child.getStringAttribute("class");
          if (resource != null && url == null && mapperClass == null) {
            //优先从resource进行解析
            ErrorContext.instance().resource(resource);
            InputStream inputStream = Resources.getResourceAsStream(resource);
            //使用XMLMapperBuilder解析mapper.xml文件
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
            mapperParser.parse();
          } else if (resource == null && url != null && mapperClass == null) {
            //从url中进行解析
            ErrorContext.instance().resource(url);
            InputStream inputStream = Resources.getUrlAsStream(url);
            //使用XMLMapperBuilder解析mapper.xml文件
            XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url, configuration.getSqlFragments());
            mapperParser.parse();
          } else if (resource == null && url == null && mapperClass != null) {
            //指定了mapper的类
            Class<?> mapperInterface = Resources.classForName(mapperClass);
            //通过类注册
            configuration.addMapper(mapperInterface);
          } else {
            throw new BuilderException("A mapper element may only specify a url, resource or class, but not more than one.");
          }
        }
      }
    }
  }

  /**是否指定环境id
   * @param id            环境id
   * @return              是否指定环境id
   */
  private boolean isSpecifiedEnvironment(String id) {
    if (environment == null) {
      throw new BuilderException("No environment specified.");
    } else if (id == null) {
      throw new BuilderException("Environment requires an id attribute.");
    } else if (environment.equals(id)) {
      return true;
    }
    return false;
  }

}
