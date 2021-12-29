# Hive笔记

## 1 Hive引言

### 1.1 Hive简介

Hive是facebook开源，捐献给apache组织，作为apache组织的顶级项目(hive.apache.org)。 hive是一个基于大数据技术的数据仓库(DataWareHouse)技术，主要是通过将用户书写的SQL语句翻译成MapReduce代码，然后发布任务给MR框架执行，完成SQL 到 MapReduce的转换。可以将结构化的数据文件映射为一张数据库表，并提供类SQL查询功能。

**关键信息：**

- 数据仓库和数据库

  > 数据仓库和数据库一样都是用来保存数据，但是二者的定位有很大的差异。
  >
  > 数据库（DataBase）：
  >
  > - 存储复杂的表格数据，存储结构紧致，少冗余数据
  > - 存储的都是实时的业务数据（比如商品信息、订单信息），**为应用的普通使用者使用的功能（商品详情、查询订单）提供功能支持**
  > - 数据库中的数据即又要读（比如查看商品信息），也要写（比如产生订单，完成支付）
  > - **相对简单的读写操作，单次操作少量的数据。**对于单次大量数据的读是支持不足的。
  >
  > 数据仓库（DataWareHouse）：
  >
  > - 存储的是相对简单的表格数据，存储结构相对松散，多冗余数据
  > - 存储的是应用的历史数据，包含业务数据和非业务数据（某个页面的pv和uv），**为专业用户（程序员、产品经理、运营人员）分析产品运行状况、制定公司运营决策做支持**
  > - 数据仓库中的数据主要用于读
  > - **相对复杂的分析查询操作，单次操作大量的数据。**

- Hive的特点

  > 作用：
  >
  > - Hive是一个数据仓库
  > - Hive构建在HDFS上，底层使用HDFS，也就可以存储海量数据。
  > - Hive允许程序员使用SQL命令来完成数据的分析统计，底层转换为Yarn调度下的MR操作。(Hive会将SQL转化为MR操作)
  >
  > 优点：
  >
  > - 简化程序员的开发难度，写SQL即可，避免了写MR程序,减少开发人员的学习成本
  >
  > 缺点：
  >
  > - 延迟较高(MapReduce本身延迟，Hive SQL向MapReduce转化优化的成本)，适合做大数据的离线处理(TB PB级别的数据，统计结果延迟1天产出)
  >
  > 不适合场景：
  >
  > - 小数据量
  > - 实时计算

### 1.2 ETL的流程

ETL（数据仓库技术）是英文Extract-Transform-Load的缩写，用来描述将[数据](https://baike.baidu.com/item/数据/5947370)从来源端经过抽取（extract）、[转换](https://baike.baidu.com/item/转换/197560)（transform）、加载（load）至目的端的过程。**ETL**一词较常用在[数据仓库](https://baike.baidu.com/item/数据仓库)，但其对象并不限于数据仓库。我们来看一下大数据数仓所设计到技术和运行流程：

![image-20210813225636721](Hive笔记.assets/image-20210813225636721.png)

## 2 Hive的架构

![image-20200417163959470](Hive笔记.assets/image-20200417163959470.png)

相关概念：

- HDFS：用来存储hive仓库的数据文件
- Yarn：用来完成hive的HQL转化的MR程序的执行
- MetaStore：保存管理hive的元数据
- Driver：将HQL的执行，转化为MapReduce程序的执行，从而对HDFS集群中的数据文件进行统计。

## 3 Hive的安装步骤

```markdown
安装涉及的软件
1. HDFS(Hadoop2.9.2)
2. Yarn(Hadoop2.9.2)
3. MySQL(5.7.29)
4. Hive(1.2.1)
```

1. 克隆一台用户安装hive的机器（内存至少1G）

   > 修改ip
   >
   > 关闭防火墙
   >
   > 关闭selinux
   >
   > 修改主机名
   >
   > ip映射
   >
   > ssh免密登录
   >
   > 安装jdk

2. 搭建Hadoop环境（HDFS+Yarn）

   > Hive依赖HDFS+MR，需要先准备好Hadoop环境
   > 参考之前的hadoop环境搭建步骤

3. 安装MySQL

   > 参考之前的MySQL的安装文档
   > 注意：`一定要修改密码并允许root远程登录`
   
4. 安装Hive

   > 1. 上传hive安装包到linux中
   >
   > 2. 解压缩hive
   >
   >    ```powershell
   >    [root@hadoop10 opt]# tar -zxvf apache-hive-1.2.1-bin.tar.gz -C /opt/installs
   >    [root@hadoop10 opt]# mv apache-hive-1.2.1-bin hive1.2.1 #改名，方便配置环境变量
   >    ```
   >
   >    
   >
   > 3. 配置环境变量
   >
   >    ```markdown
   >    export HIVE_HOME=/opt/installs/hive1.2.1
   >    export PATH=$PATH:$HIVE_HOME/bin
   >    ```
   >
   > 4. 重新加载环境变量
   >
   >    ```powershell
   >    [root@hadoop10 installs]# source /etc/profile
   >    ```
   >
   >    

5. Hive配置初始化

   > 1. 配置 `hive/conf/hive-env.sh`
   >
   >    ```powershell
   >    #复制 hive-env.sh.template 并改名为hive-env.sh
   >    [root@hadoop10 conf]# cp hive-env.sh.template hive-env.sh
   >
   >    #配置hive-env.sh
   >    [root@hadoop10 conf]# vi hive-env.sh
   >
   >    # 配置hadoop目录
   >    HADOOP_HOME=/opt/installs/hadoop-2.9.2/
   >    # 指定hive的配置文件目录
   >    export HIVE_CONF_DIR=/opt/installs/hive1.2.1/conf/
   >    ```
   >
   > 2. 配置 `hive-site.xml` 
   >
   >    复制hive-default.xml.template 并改名hive-site.xml
   >
   >    ```xml
   >    <?xml version="1.0" encoding="UTF-8" standalone="no"?>
   >    <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
   >    <configuration>
   >        <!-- 将configuration中原有子标签全部删掉,保留configuration父标签-->
   >    
   >        <!--hive的元数据保存在mysql中，需要连接mysql，这里配置访问mysql的信息-->
   >        <!--url：这里必须用配置连接mysql的url 
   >    		固定格式：jdbc:mysql://ip:port/数据库名
   >    			hadoop10 mysql所在机器ip或者做过ip映射的主机名
   >    			3306 mysql监听端口号
   >    			hive	保存hive元数据的数据库。名字任意，需要自己手动在mysql中创建
   >    	-->
   >        <property>
   >            <name>javax.jdo.option.ConnectionURL</name>
   >            <value>jdbc:mysql://hadoop10:3306/hive?useUnicode=true&amp;characterEncoding=utf-8&amp;useSSL=false</value>
   >        </property>
   >        <!--数据库驱动类名-->
   >        <property>
   >            <name>javax.jdo.option.ConnectionDriverName</name>
   >            <value>com.mysql.jdbc.Driver</value>
   >        </property>
   >        <!--数据库用户名-->
   >        <property>
   >            <name>javax.jdo.option.ConnectionUserName</name>
   >            <value>root</value>
   >        </property>
   >        <!--数据库密码-->
   >        <property>
   >            <name>javax.jdo.option.ConnectionPassword</name>
   >            <value>123456</value>
   >        </property>
   >    </configuration>
   >    ```
   >
   > 
   >
   > 3. mysql中创建`hive`数据库
   >
   >    ```powershell
   >    #登录mysql
   >    [root@hadoop10 ~]# mysql -u root -p
   >    
   >    #创建hive数据库
   >    mysql> create database hive character set utf8mb4;
   >    ```
   >

6. 初始化Hive元数据（只第1次启动需要：本质上就是通过jdbc连接MySQL，在MySQL中创建保存Hive元数据的表）

   > 1. 复制资料中的 `mysql-connector-java-5.1.38.jar` 到 `hive/lib`文件夹里
   >
   >    ![image-20210913122440082](Hive%E7%AC%94%E8%AE%B0.assets/image-20210913122440082.png)
   >
   > 2. 执行初始化命令
   >
   >    ```powershell
   >    [root@hadoop10 conf]# schematool -dbType mysql -initSchema
   >    Metastore connection URL:        jdbc:mysql://hadoop10:3306/hive?useUnicode=true&characterEncoding=utf-8&useSSL=false
   >    Metastore Connection Driver :    com.mysql.jdbc.Driver
   >    Metastore connection User:       root
   >    Starting metastore schema initialization to 1.2.0
   >    Initialization script hive-schema-1.2.0.mysql.sql
   >    Initialization script completed
   >    schemaTool completed	#说明初始化成功，此时在mysql的hive数据库中发现新增了很多的表，这些表用来保存hive元数据
   >    ```
   >
   >    

7. 启动

   > 1. 启动hadoop
   >
   >    ```powershell
   >    [root@hadoop10 conf]# start-dfs.sh	#启动单机版的hdfs
   >    
   >    [root@hadoop10 conf]# start-yarn.sh	#启动单机版yarn
   >    
   >    [root@hadoop10 conf]# jps	#查看启动的进程
   >    47699 SecondaryNameNode
   >    47333 NameNode
   >    48087 ResourceManager
   >    47496 DataNode
   >    48217 NodeManager
   >    49167 Jps
   >    ```
   >
   > 2. 启动hive
   >
   >    ```powershell
   >    #本地启动（也是管理员模式），效果：直接在本地操作Hive，但是该模式只能在Hive主机上运行
   >    [root@hadoop10 conf]# hive
   >    
   >    Logging initialized using configuration in jar:file:/opt/installs/apache-hive-1.2.1-bin/lib/hive-common-1.2.1.jar!/hive-log4j.properties
   >    hive> 
   >    ```
   >
   > 

## 4 Hive基本使用

### 4.1 初识HQL

Hive允许使用者以类SQL的方式操作HDFS中的数据。HQL（Hive Query language）是Hive的查询语言，它的设计很大程度上深受MySQL的影响。因此，如果你熟悉MySQL，你会觉得Hive很亲切。

```powershell
# 1.查看数据库
	hive> show databases;
# 2. 创建一个数据库
	hive> create database baizhi;
# 3. 查看database 
	hive> show databases;
# 4. 切换进入数据库
	hive> use baizhi;
# 5.查看所有表
	hive> show tables;
# 6.创建一个表
	hive> create table t_user(id varchar(10),name varchar(20),age int);
# 7. 添加一条数据(转化为MR执行--不让用，仅供测试)
	hive> insert into t_user values('1001','zhangsan',20);
# 8.查看表结构
	hive> desc t_user;
# 9.查看表的schema描述信息。(表元数据，描述信息)
	hive> show create table t_user;
	# 明确看到，该表的数据存放在hdfs中。
# 10 .查看数据库结构
	hive> desc database baizhi;
# 11.查看当前库
	hive> select current_database();
# 12 其他sql
    select * from t_user;
    select count(*) from t_user; (Hive会启动MapReduce)
    select * from t_user order by id;(Hive会启动MapReduce)
```

说明：Hive从0.14版本开始支持行级*更新*和*删除*,但缺省是*不开启*的。官方也不建议在生产环境下使用这些功能。

### 4.2 Hive的使用方式

使用Hive本质上有2种方式：CLI方式和ThiftServler方式

- CLI方式：必须在安装了Hive的机器上才能使用，无法在别的机器上通过网络访问Hive
- Thrift Server方式：在Hive机器上启动一个Server端监听Client的请求，可以别的机器上通过网络访问Hive

#### 4.2.1 CLI方式

1. Hive交互Shell方式

   在安装了Hive的机器上，直接输入Hive命令

   ```shell
   [root@hadoop10 ~]# hive
   SLF4J: Class path contains multiple SLF4J bindings.
   SLF4J: Found binding in [jar:file:/opt/installs/hadoop-2.9.2/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
   SLF4J: Found binding in [jar:file:/opt/installs/tez0.9.1/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
   SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
   SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
   SLF4J: Class path contains multiple SLF4J bindings.
   SLF4J: Found binding in [jar:file:/opt/installs/hadoop-2.9.2/share/hadoop/common/lib/slf4j-log4j12-1.7.25.jar!/org/slf4j/impl/StaticLoggerBinder.class]
   SLF4J: Found binding in [jar:file:/opt/installs/tez0.9.1/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
   SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
   SLF4J: Actual binding is of type [org.slf4j.impl.Log4jLoggerFactory]
   
   Logging initialized using configuration in jar:file:/opt/installs/apache-hive-1.2.1-bin/lib/hive-common-1.2.1.jar!/hive-log4j.properties
   hive> 即可在这里输入hql语句...
   ```

   

2. Hive -e命令方式

   在安装了Hive的机器上，输入`Hive -e "hql语句"`，直接执行hql。适用于少量hql语句的执行

   ```shell
   [root@hadoop10 ~]# hive -e "show databases;use baizhi;select * from t_user;"
   ```

3. Hive -f命令方式

   在安装了Hive的机器上，输入`Hive -f 文件路径`，直接执行文件中定义的hql语句

   test.sql

   ```sql
   show databases;
   use baizhi;
   select * from t_user
   ```

   ```shell
   [root@hadoop10 ~]# hive -e test.sql
   ```

#### 4.2.2 Thrift Server方式

Hive CLI本地启动的方式只能在本机访问hive，无法在其它机器上通过网络访问Hive。这时，我们可以先在Hive机器上启动`hiveserver2`服务，然后就可以通过client访问：

1. 先启动Hive服务

   ```powershell
   # 启动hive的服务器，可以允许远程连接方式访问。
   // 前台启动（一旦关闭窗口，服务关闭）
   [root@hadoop10 ~]# hiveserver2 
   // 后台启动
   [root@hadoop10 ~]# hiveserver2 &
   ```

2. 启动beeline客户端访问Hive服务

   ```powershell
   # 启动客户端
   [root@hadoop10 ~]# beeline
   beeline> !connect jdbc:hive2://hadoop10:10000
   不用输入用户名，直接回车
   不用输入密码，直接回车
   0: jdbc:hive2://hadoop10:10000>
   ```

3. 此时，也可以在Windows上通过图形化客户端**DBeaver**访问Hive

   ```markdown
   1.解压缩DBeaver压缩包
   2.在DBeaver文件夹里新建lib文件夹，并将资料里的hive-jdbc-1.2.1-standalone.jar和hadoop-common-2.9.2.jar复制到lib里
   3.配置连接hive
   ```

   ![image-20210723150707623](Hive笔记.assets/image-20210723150707623.png)


### 4.3 Hive中的数据类型

​       HQL本质上就是一种SQL方言，Hive中建表时也需要设置列的数据类型。Hive支持原子和复杂数据类型。

> 数据类型（`primitive`，`array`，`map`，`struct` )
>
> 建表语法：
>
> ```mysql
> create table 表名(
> 	列名 数据类型,
>     列名 数据类型,
>     ...
> )
> 
> 说明：Hive中的约束支持很弱，通常不添加约束
> ```

**primitive(原始类型)：**

| hive数据类型  | 字节 | 备注                                       | 示例                  |
| ------------- | ---- | ------------------------------------------ | --------------------- |
| TINYINT       | 1    | java-byte  整型                            | age tinyint           |
| SMALLINT      | 2    | java-short 整型                            | score smallint        |
| **INT**       | 4    | java-int 整型                              | id int                |
| BIGINT        | 8    | java-long 整型                             | id bigint             |
| **BOOLEAN**   |      | 布尔                                       | status boolean        |
| FLOAT         | 4    | 浮点型                                     | salary float          |
| **DOUBLE**    | 8    | 浮点型                                     | salary double         |
| **STRING**    |      | 字符串 （理论上限2G）                      | name string           |
| VARCHAR       |      | 变长字符串，范围（1~65355）                | name varchar(20)      |
| CHAR          |      | 定长字符串，范围（1~65355）                | id_card char(21)      |
| BINARY        |      | 二进制类型                                 | music binary          |
| **TIMESTAMP** |      | 时间戳类型（自1970年开始计算的秒级时间戳） | create_time timestamp |
| **DATE**      |      | 日期类型（只包含年月日）                   | birthday date         |

**复杂类型**

> 复杂的数据类型**必须使用尖括号指明其中数据字段的类型**，且复杂类型允许任意层次的嵌套。

| 数据类型                                      | 备注                         | 示例                                           |
| --------------------------------------------- | ---------------------------- | ---------------------------------------------- |
| ARRAY< 元素类型 >                             | 数组类型，类似Java中List类型 | hobbies array< string >                        |
| MAP< key类型,value类型 >                      | Map类型，类似Java中Map类型   | score map< string,float >                      |
| STRUCT< 属性名:属性类型,属性名:属性类型,... > | 结构体，类似Java中的class    | info struct< name:string,age:int,sex:char(1) > |

**综合示例：**

```mysql
create table t_person(
	id int,
	name string,
	salary double,
	birthday date,
	sex char(1),
	hobbies array<string>,
	cards map<string,string>,
	addr struct<city:string,zipCode:string>
)

#删除表
drop table t_person;
```

## 5 Hive的数据分隔格式

Hive的数据分隔格式决定了Hive表中的字段值在HDFS文件中是如何分隔的。

### 5.1 自定义分隔符（重点）

| 分隔符               | 含义                                                         | 示例                                 |
| -------------------- | ------------------------------------------------------------ | ------------------------------------ |
| **fields**           | 用来表示每个列的值之间分隔符。                               | `fields terminated by ','`           |
| **collection items** | 用来分割array中每个元素，以及struct中的每个值，以及map中kv与kv之间。 | `collection items terminated by '-'` |
| **map keys**         | 用来分割map的k和v。                                          | `map keys terminated by '|'`         |
| **lines**            | 用来分割多条数据。注意lines分隔符其实不允许自定义，必须是换行 | `lines terminated by '\n'`           |

**示例：**

```mysql
create table t_person(
    id string,
    name string,
    salary double,
    birthday date,
    sex char(1),
    hobbies array<string>,
    cards map<string,string>,
    addr struct<city:string,zipCode:string>
) row format delimited
fields terminated by ','--列的分割
collection items terminated by '-' --数组 struct的属性 map的kv和kv之间
map keys terminated by '|' -- map的k与v的分割
lines terminated by '\n'; --行数据之间的分割
```

**测试数据**

```markdown
person.txt
1,张三,8000.0,2019-9-9,1,抽烟-喝酒-烫头,123456|中国银行-22334455|建设银行,北京-10010
2,李四,9000.0,2019-8-9,0,抽烟-喝酒-烫头,123456|中国银行-22334455|建设银行,郑州-45000
3,王五,7000.0,2019-7-9,1,喝酒-烫头,123456|中国银行-22334455|建设银行,北京-10010
4,赵6,100.0,2019-10-9,0,抽烟-烫头,123456|中国银行-22334455|建设银行,郑州-45000
5,于谦,1000.0,2019-10-9,0,抽烟-喝酒,123456|中国银行-22334455|建设银行,北京-10010
6,郭德纲,1000.0,2019-10-9,1,抽烟-烫头,123456|中国银行-22334455|建设银行,天津-20010
```

**导入数据：**

```powershell
# 在hive命令行中执行一下命令，将person.txt中的数据直接导入到t_person表中

-- local 代表本地（Hive所在的机器，是linux不是windows）路径，如果不写，表示读取文件来自于HDFS
-- overwrite 是覆盖的意思，如果t_person表有数据会被覆盖掉;如果不屑，表示不覆盖。
命令：load data [local] inpath '文件路径' [overwrite] into table 表名;

# 本质上就是将数据上传到hdfs中(让数据受hive的管理)
示例：从本地(Hive所在的linux)导入数据到hive
load data local inpath '/opt/person1.txt' into table t_person;
示例：从hdfs导入数据到hive
先上传文件到hdfs：			hdfs dfs -put /opt/person.txt /baizhi/person1.txt
再从hdfs中导入数据到hive：	load data inpath '/baizhi/person1.txt' overwrite into table t_person; 
```

### 5.2 JSON分隔符

如果原始数据是json格式，可以使用Hive内置的JSON分隔符自动转换。

**示例数据：**

```json
/opt/person.json
{"id":1,"name":"zhangsan","sex":0,"birth":"1991-02-08","hobbies":["抽烟","喝酒"],"cards":{"123456":"农行","456789":"建行"},"addr":{"city":"郑州","zipCode":"450000"}}
{"id":2,"name":"lisi","sex":1,"birth":"1991-02-08","cards":{"654321":"农行","987654":"建行"},"addr":{"city":"郑州","zipCode":"450000"}}
```

1. 添加依赖

   ```markdwon
   # 在hive的客户端执行一下命令(临时添加jar到hive的classpath，有效期本链接内)
   add jar /opt/installs/hive1.2.1/hcatalog/share/hcatalog/hive-hcatalog-core-1.2.1.jar
   
   # 补充：永久添加，Hive服务器级别有效。
   1. 将需要添加到hive的classpath的jar，拷贝到hive下的auxlib（需要手动在hive根目录下创建）目录下，
   2. 重启hiveserver2即可。
   ```

2. 建表，并指定使用json分隔符

   ```mysql
   create table t_person2(
       id string,
       name string,
       sex char(1),
       birth date,
       hobbies array<string>,
       cards map<string,string>,
       addr struct<city:string,zipCode:string>
   )row format serde 'org.apache.hive.hcatalog.data.JsonSerDe';
   ```

   

3. 导入数据到hive

   ```powershell
   #导入数据
   load data local inpath '/opt/person.json' overwrite into table t_person2;
   
   #查询数据
   select * from t_person2;
   ```

### 5.3 正则分隔符

>`数据：access.log`
>
>下边列于列之间的分割符没有完全统一，这时候可以使用正则分隔符

~~~shell
level ip log_time app service method
以下为日志内容：
INFO 192.168.1.1 2019-10-19 QQ com.baizhi.service.IUserService#login
INFO 192.168.1.1 2019-10-19 QQ com.baizhi.service.IUserService#login
ERROR 192.168.1.3 2019-10-19 QQ com.baizhi.service.IUserService#save
WARN 192.168.1.2 2019-10-19 QQ com.baizhi.service.IUserService#login
DEBUG 192.168.1.3 2019-10-19 QQ com.baizhi.service.IUserService#login
ERROR 192.168.1.1 2019-10-19 QQ com.baizhi.service.IUserService#register
~~~

1. 建表，并指定正则分隔符

   ```mysql
   create table t_access(
       level string,
       ip string,
       log_time date,
       app string,
       service string,
       method string
   )row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'--正则表达式的格式转化类
   with serdeproperties("input.regex"="(.*)\\s(.*)\\s(.*)\\s(.*)\\s(.*)#(.*)");--(.*) 表示任意字符 \\s表示空格
   ```

2. 导入数据

   ```powershell
   load data local inpath '/opt/access.log' into table t_access;
   ```

3. 查看数据

   ```powershell
   select * from t_access
   ```

   

## 6 HQL进阶

### 6.1 HQL简单查询

**SQL回顾：**

```markdown
SQL关键词执行顺序
from > where条件 > group by > having条件 > select > order by > limit

注意：sql一旦出现group by，后续的关键词能够操作字段只有(分组依据字段，组函数处理结果)
```

1. 查询所有字段

   ```mysql
   -- select * from 表名;
   select * from t_person;
   ```

   

2. 查询部分字段

   ```mysql
   -- select 字段1,字段2,... from 表名;
   select id,name,salary,birthday,sex,hobbies,cards,addr from t_person;
   ```

3. 查询复杂类型字段的元素值

   ```mysql
   -- select array类型[下标],map类型['key'],struct类型.属性名  from 表名;
   select name,salary,hobbies[1],cards['123456'],addr.city from t_person;
   ```

4. 条件查询

   ```mysql
   -- 比较运算符： = != >= <=
   select * from t_person where addr.city='郑州';
   ```

5. 区间和枚举查询

   ```mysql
   --  between and 、 in(值1,值2,...)
   
   select * from t_person where salary between 1000 and 8000;
   select * from t_person where id in('1','3','4');
   ```

   

6. 多条件查询

   ```mysql
   -- and、or
   select * from t_person where salary > 8000 and sex = '0';
   
   select * from t_person where salary > 8000 or sex = '1';
   ```

7. 数学运算

   ```mysql
   -- 数学运算 + - * / %
   select salary,salary+100,salary-100,salary*12,salary/30,salary %1000
   from t_person;
   ```

   

8. 取别名

   ```mysql
   -- as 别名
   select salary `月薪`,salary*12 as `年薪`,salary/30 as `日薪`
   from t_person  p;
   
   注意：as关键字可以省略，别名有中文需要使用反引号括起来
   ```

   

9. case when

   ```mysql
   语法：
   case
          when 条件1 then 结果1
            when 条件2 then 结果2
            when 条件3 then 结果3
              ...
            else 其他结果  
        end 
   类似于java中if（条件）{结果}else
   
   示例：
   select id,name,salary,
            case 
               when salary >= 9000 then '高薪'
               when salary >= 7000 then '中等'
               when salary >= 5000 then '一般'
               else '垃圾'
            end `薪资等级`
   from t_person;
   ```

10. 去重

    ```mysql
    -- distinct
    select distinct sex from t_person;
    select distinct addr.city from t_person;
    ```

11. 排序

    ```mysql
    -- order by 字段 desc|asc
    select * from t_person
    order by salary desc;
    ```

12. 模糊查询

    ```mysql
    -- like '匹配模式'   %匹配任意个任意字符，_匹配一个任意字符
    select * from t_person
    where name like '%张%';
    ```

13. limit子句

    ```mysql
    -- limit 条数; 和mysql不同，hive中limit后只能跟条数(也就是取前几条)
    select * from t_person limit 3
    ```

14. 查询表信息

    ```mysql
    -- describe 表名; 可以简写为desc 表名  获取表的详细信息
    describe t_person;
    desc t_person;
    
    -- show create table 表名; 获取建表信息
    show create table t_person ;
    ```

    

### 6.2 基础函数

hive内置了[数百个函数](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-LogicalOperators)，简化数据操作。可以通过 `show functions`查看hive内置的函数。

1. 单行函数：作用到表中的每一行数据，表中有多少行，就得到多少行新结果。

   ```mysql
   -- 1. array_contains(数组,值);	判断数组中是否包含指定的值
   select name,hobbies from t_person where array_contains(hobbies,'喝酒');
   
   -- 2. length(字段|值)	返回数据的长度
   select length('123123');
   select name, length(name) from t_person;
   
   -- 3. concat(字段|值,字段|值,...)	字符串拼接，将函数中的值拼接起来
   select concat('person_name: ',name) from t_person;
   
   -- 4. unix_timestamp() 获取当前时间的时间戳
   select unix_timestamp();
   
   -- 5. unix_timestamp('日期时间字符串','日期时间格式') 获取指定字符串日期的时间戳
   select unix_timestamp('1991-9-8 12:20:30','yyyy-MM-dd HH:mm:ss')
   
   -- 6. from_unixtime(时间戳,'日期时间格式')
   select from_unixtime(unix_timestamp(),'yyyy/MM/dd HH:mm:ss');
   
   -- 7. to_date('日期时间字符串')	返回日期字符串（年月日必须使用-分隔，时分秒使用:分隔）中的年月日的部分字符串
   select to_date('1999-9-9 12:20:30');
   
   -- 8. year('日期时间字符串'),month('日期时间字符串'),day('日期时间字符串'),
   --    hours('日期时间字符串'),minute('日期时间字符串'),second('日期时间字符串')
   select year('1999-09-09 23:20:30')
   select month('1999-09-09 23:20:30')
   select day('1999-09-09 23:20:30')
   select hour('1999-09-09 23:20:30')
   select minute('1999-09-09 23:20:30')
   select second('1999-09-09 23:20:30')
   
   -- 9. date_add(date,数字) 在其实日期上添加指定的天，获取新的日期
   select name,date_add(birthday,9) from t_person;
   select name,date_add(birthday,-9) from t_person;
   
   -- 10. concat_ws(分隔符,数组字段) 对数组的多个元素使用指定分隔符拼接。
   select id,name,concat_ws(',',hobbies) from t_person;
   
   -- 11. get_json_object('json字符串','$.属性名') 从json格式字符串中获取指定属性值
   select get_json_object('{"name":"xiaohei","age",10}','$.name'),
   		get_json_object('{"name":"xiaohei","age",10}','$.age')
   ```

2. 组函数：作用到一组（多行）数据，每组数据得到一个结果。一张表没有显式分组前，默认当做一个组处理。

   ```mysql
   -- 1. max、min、sum、avg、count等。  统计函数
   select max(salary),min(salary),avg(salary),sum(salary),count(*) from t_person;
   
   -- 2. collect_list(字段) 对某个字段的值进行收集汇总，不会去重。字段必须是基本类型值
   select collect_list(addr.city) from t_person;
   
   -- 3. collect_set(字段) 对某个字段的值进行收集汇总，会字段去重。字段必须是基本类型值
   select collect_set(addr.city) from t_person;
   ```

   

### 6.3 分组

**测试数据准备：**

```markdown
# 环境准备：资料中包含着2个txt：employees.txt departments.txt
将employees.txt导入到t_employee表中
# 建表 t_employee
create table t_employee(
	employee_id int,
	first_name	varchar(20),
	last_name	varchar(25),
	email	varchar(25),
	phone_number	varchar(20),
	job_id	varchar(10),
	salary	double,
	commission_pct	double,
	manager_id	int,
	department_id	int,
	hiredate	date
)row format delimited 
fields terminated by ','
collection items terminated by '-'
map keys terminated by '|'
lines terminated by '\n';

# 导入数据
load data local inpath '/opt/employees.txt' into table t_employee ;
```

分组：在一些情况下，需要根据某一列对表中的数据进行分类（分组），把该列值相同的数据分成一组，然后以组为单位进行数据处理。比如：`求取各个部门员工最高薪资`

```mysql
分组语法：
select 字段1,字段2,...
from 表名
[where 条件]
group by 分组的依据（字段）
[having 条件]
...
```

示例：

```mysql
-- 查询各部门的员工最高薪资
select department_id,max(salary)
from t_employee 
group by department_id;

-- 分组前执行判断（where）：查询60 90 100 部门的平均薪资
select department_id,avg(salary) 
from t_employee  
where department_id in(60,90,100) -- 条件
group by department_id;

-- 分组后执行判断（having）：查询平均薪资>=8000的部门
select department_id,avg(salary)
from t_employee 
group by department_id 
having avg(salary) >= 8000
```

**综合案例：**

```mysql
-- 表（电影观看日志）
create table t_visit_video (
    username string,
    video_name string,
    video_date date
)row format delimited fields terminated by ',';

-- 数据：豆瓣观影日志数据。(用户观影日志数据  按照天存放 1天一个日志文件)
张三,大唐双龙传,2020-03-21
李四,天下无贼,2020-03-21
张三,神探狄仁杰,2020-03-21
李四,霸王别姬,2020-03-21
李四,霸王别姬,2020-03-21
王五,机器人总动员,2020-03-21
王五,放牛班的春天,2020-03-21
王五,盗梦空间,2020-03-21
```

![image-20200419222414792](Hive笔记.assets/image-20200419222414792.png)

需求：获取每个用户某一天观看的所有影视剧

```mysql
1. 先根据username分组，并使用组函数将video_name收集起来
	select username,collect_set(video_name) from t_visit_video group by username;

2. 将收集的数组数据使用逗号拼接起来
	select username,concat_ws(',',collect_set(video_name)) from t_visit_video group by username;
```

### 6.4 子查询

当一个查询SQL的条件需要使用另外一个查询SQL的结果时，需要在一个SQL语句中嵌套另外一个SQL语句。Hive子查询的支持比较弱，只支持in和from子查询

**测试数据准备：**

```markdown
# 环境准备：资料中包含着2个txt：employees.txt departments.txt
将departments.txt导入到t_department表中

# 建表t_department
create table t_department(
	department_id	int,
	department_name	varchar(3),
	manager_id	int,
	location_id	int
)row format delimited 
fields terminated by ','
collection items terminated by '-'
map keys terminated by '|'
lines terminated by '\n';

# 导入数据
load data local inpath '/opt/departments.txt' into table t_department;
```

示例：

```mysql
-- 子查询：一个查询需要使用另外一个查询的结果做条件

-- 查询employee_id 150的员工所在的部门信息（in子查询）
select *
from t_department
where department_id in (select department_id
from t_employee
where employee_id = 150);

-- 对员工按薪资降序排列，统计出薪资前10名的总薪资,如果一个查询SQL返回多行多列，那么可以把该SQL当做一张表看待
select sum(salary) 
from 
(select * from t_employee 
	order by salary desc
limit 10
) e;
注意：把内部sql当作一张表看待，一定要为其起别名
```

### 6.5 表连接

表连接：当查询的数据需要从多张表中获取时，需要将多张表连接起来进行查询。例如：`查询员工及所在部门的信息，需要从员工表和部门表中同时获取数据`

1. 内连接

   ```mysql
   语法：
   select 表1.列名,... ,表2.列名,...
   from 表1 inner join 表2
   on 连接条件;
   
   -- 查询员工和部门的信息
   select e.*,d.*
   from employees e inner join departments d
   on e.department_id = d.department_id;
   ```

   特点：两张表中只要满足连接条件的数据才显示，不管是哪一张表的数据，只要不满足连接条件一定不显示。

2. 左外连接

   ```mysql
   语法：
   select 左表.列名,... ，右表.列名
   from 左表 left outer join 右表 
   on 连接条件；
   
   -- 查询员工及所属部门信息
   select e.*,d.*
   from employees e left outer join departments d
   on e.department_id = d.department_id;
   ```

   特点：左表中数据无论是否满足条件一定显示，右表中的数据满足条件显示，不满足条件不显示。

3. 右外连接

   ```mysql
   语法：
   select 左表.列名，... ,右表.列名，...
   from 左表 right outer join 右表
   on 连接条件;
   
   -- 查询员工及所在部门信息
   select e.*,d.*
   from employees e right outer join departments d
   on e.department_id = d.department_id;
   ```

   特点：右表中无论是否满足连接条件都显示，左表中数据满足连接条件才显示，不满足就不显示。

说明：

- 实战开发时，**左外连接**使用最多。
- 内连接的**inner**关键字和外连接的**outer**关键字可以省略

## 7 HQL高级

### 7.1 高级函数

1. 炸裂函数：作用于数组字段，将数组元素炸裂成多行展示

   ```mysql
   -- explode(array|map) 将数组或Map中的元素炸裂成多行展示,且不允许再select其它字段
   -- 查询所有的爱好
   select explode(hobbies) as `爱好` from t_person;
   select explode(cards) as (`卡号`,`银行`) from t_person;
   ```

2. **lateral view**：配合炸裂函数使用，将炸裂函数生成的多行数据做成一个临时表，并将临时表自动的拼接到原表后

   ```mysql
   -- lateral view：为表的拼接一个列(炸裂结果)
   -- 语法：from 原表 lateral view explode(数组字段) 临时表别名 as 临时表的字段名;
   
   -- 查看id，name，爱好，并要求：一个爱好一条信息。
   select id,name,hobby
   from t_person lateral view explode(hobbies) t_hobby as hobby
   ```

   ![image-20210724210922096](Hive笔记.assets/image-20210724210922096.png)

   更多案例：

   ```mysql
   -- 统计各个爱好的人数
   -- explod+lateral view
   select hobby,count( * )
   from t_person lateral view explode(hobbies) t_hobby as hobby
   group by hobby;
   
   -- 统计最受欢迎的爱好(Top1的爱好)
   SELECT hobby,count( * ) num
   	from t_person lateral view explode(hobbies) t_hobby as hobby
   	group by hobby 
   	order by num desc limit 1;
   ```

   

3. 开窗函数：根据某个开窗依据在原始表上开多个窗口（每个窗口可以包含原始表中任意行的数据，等同于分组后的一组数据），对每个窗口的数据执行统计操作（就是应用组函数）。

   开窗函数类似于分组统计，也是对原表数据进行分组（这里就是一个窗口）聚合统计。但2者有以下区别

   - 分组对一组数据聚合后只返回一个值，开窗函数为一个窗口的每行都返回一个值。
   - 组函数不能和非分组字段联合使用，但开窗函数可以

   ```mysql
   语法：
   	组函数 over(partition by 分组依据 [order by 排序字段])
   	
   # 添加测试表（记录用户超市消费的时间和金额）:
   create table t_business(
   	name string,
       order_date date,
       cost int
   )row format delimited 
   fields terminated by ',';
   
   # 导入到表 t_business中:
   load data local inpath '/opt/business.txt' into table t_business;
   ```
   
   案例一：`查询超市每天的销售记录和每月的销售总额`
   
   ![image-20210814185353615](Hive%E7%AC%94%E8%AE%B0.assets/image-20210814185353615.png)
   
   ```mysql
   select b.*, 
   	sum(cost) over(partition by month(order_date)) moth_sum_cost -- 开窗函数
   from t_business b 
   order by month(order_date);-- order by是为了方便观察结果;
   ```
   
   案例二：`查询超市每天的销售纪录和每月单笔最大交易金额`
   
   ![image-20210814185708532](Hive%E7%AC%94%E8%AE%B0.assets/image-20210814185708532.png)
   
   ```mysql
   select b.*, 
   	max(cost) over(partition by month(order_date)) moth_max_cost -- 开窗函数
   from t_business b 
   order by month(order_date); -- order by是为了方便观察结果
   ```
   
   案例三：`查询超市每天的销售记录，每月的销售记录按照消费额降序排列并编号`
   
   ![image-20210904231638645](Hive笔记.assets/image-20210904231638645.png)
   
   ```mysql
   -- row_number()配合over()开窗函数，安排排序依据为每个窗口的数据行从1开始编号
   select b.*, 
   	row_number() over(partition by month(order_date) order by cost desc) cost_desc_num
   from t_business b 
   
   -- rank() 和 row_number()不同之处，如果排序依据相同则排名相同
   select b.*, 
   	rank() over(partition by month(order_date) order by cost desc) cost_desc_num
   from t_business b 
   ```
   
   
   
   
   
   案例四：`查询超市每天的销售记录和每月按照日期进行累加的销售额`
   
   ![image-20210814191620263](Hive%E7%AC%94%E8%AE%B0.assets/image-20210814191620263.png)
   
   ```mysql
   -- 有order by；按照排序连续计算，输出计算过程的值；无order by，返回值最终的值
    select b.*, 
    	sum(cost) over(partition by month(order_date) order by day(order_date)) as month_sum_cost
   from t_business b
   order by order_date;-- order by是为了方便观察结果
   ```

### 7.2 HQL排序

Hive中有2种排序：全排序（order by）和局部排序（sort by）。

全排序（order by）会对查询结果执行一个全局的排序，在Hive中全局排序的所有结果只会通过一个reducer进行处理的过程。对于大数据集，这个过程需要极漫长的时间来完成。在很多情况下，并不需要结果是全局排序的。此时可以使用Hive独有的局部排序（sort by）。

#### 7.2.1 sort by

当不需要全局有序，只需要设置reduceTask并行度大于1，使用sort by就会使用多个reduce排序从而生成多个局部排序结果。

语法：

```mysql
select * from 表名
sort by 排序字段 asc|desc;

-- 设置reduceTask并行度为2 ，默认为1的时候sort by效果和order by一模一样。要演示演示sort by效果必须设置reduceTask并行度大于1
set mapreduce.job.reduces=2;

-- 按薪资降序查询员工信息
select * from t_employee
order by salary desc;

-- 查询每个部门的按照薪资降序排列的员工信息
select * from t_employee 
sort by salary desc;
```

原理：

![image-20210817192558554](Hive笔记.assets/image-20210817192558554.png)

#### 7.2.2 distribute by

sort by默认的分区规则是map输入的key%numReductTasks，其分区效果不可控制。需要控制分区的划分时，需要结合 `distribute by` 使用局部排序。

语法：

```mysql
select * from 表名
[distribute by 分区字段]
sort by 排序字段 asc|desc;

-- 不分区（省略distribute by），sort by效果和order by一模一样，底层只有1个reducer
-- 按薪资降序查询员工信息
select * from t_employee
order by salary desc;

-- 使用分区（distribute by 分区字段），对各区分别排序
-- 查询每个部门的按照薪资降序排列的员工信息
select * from t_employee 
distribute by department_id 
sort by salary desc;
```

原理：

![image-20210814195314814](Hive%E7%AC%94%E8%AE%B0.assets/image-20210814195314814.png)

全局排序的特点：

- 对全部数据进行排序，底层只有一个reducer执行
- 底层MR流程只生成一个结果文件

局部排序的特点：

- 对全部数据按照一定规则（distribute by）分区后分别排序，底层有多个reducer并行
- 底层MR流程生成多个结果文件

## 8 Hive中的表分类

Hive中的表可以分为2种：管理表（managed table）和外部表（external table）。

### 8.1 管理表

在Hive中创建表时，默认创建的就是管理表。这意味着由Hive全权管理存在MySQL中的元数据（表结构）和HDFS中的数据。当删除管理表时也就会同时将表所对应的MySQL中的元数据和HDFS中的数据删除。

```mysql
-- 语法：
create table 表名(
	...
)数据分隔方式;
```

总结：

- 管理表管理元数据（表结构）和数据
- 删除管理表会将表结构和数据都删除
- 不需要和其它应用共享数据时，使用管理表

### 8.2 外部表

使用 `external` 关键字创建外部表，这意味着由Hive只管理存在MySQL中的元数据。自然在删除外部表时，只是将MySQL中对应该表的元数据信息删除，并不会删除HDFS上的数据，因此外部表可以实现和第三方应用共享数据。

```mysql
-- 语法：
create external table 表名(
	...
)数据分隔方式;

-- 创建外部表示例：
1. 准备数据文件 persons.txt 
2. 上传hdfs中，该数据文件必须上传到一个单独的文件夹中。该文件夹中的数据将会被作为表数据
3. 创建表时，通过location指定外部表数据所在的文件夹路径

create external table t_external_person(
	id int,
    name string,
    salary double,
    birthday date,
    sex char(1),
    hobbies array<string>,
    cards map<string,string>,
    addr struct<city:string,zipCode:string>
)row format delimited 
fields terminated by ','
collection items terminated by '-'
map keys terminated by '|'
lines terminated by '\n';

4.导入数据到外部表
load data local inpath '/opt/persons.txt' overwrite into table t_external_person;
```

总结：

- 外部表管理表的元数据（表结构）
- 删除外部表只会删除表的元数据
- 需要和其它应用共享数据时，使用外部表

### 8.3 分区表

分区表：将表按照某个列的一定规则进行分区存放，减少海量数据情况下的数据检索范围，提高查询效率；分区表既可以是管理表也可以是外部表。比如，一个日志表存储日志信息，可以按照日志时间划分进行分区存储。这样当我们限定到某个特定日期的查询，它的处理可以变得非常高效。因为它只需要扫描特定日期范围分区中的文件。

![image-20210814202253224](Hive%E7%AC%94%E8%AE%B0.assets/image-20210814202253224.png)

```mysql
语法：
-- 建表
create table 表名(
	...
)
partitioned by (分区字段1 类型1,分区字段2 类型2,...)
数据分隔方式;

-- 导入数据, 每次只能向表中的某个分区导入数据
load data [local] inpath '文件路径' [overwrite] into table 表名 partition(分区字段1=值1,分区字段2=值2,...);
```

示例：

1. 准备测试数据

   ```mysql
   # 文件"bj.txt" (china bj数据)
   1001,张三,1999-1-9,1000.0
   1002,李四,1999-2-9,2000.0
   1008,孙帅,1999-9-8,50000.0
   1010,王宇希,1999-10-9,10000.0
   1009,刘春阳,1999-9-9,10.0
   # 文件“tj.txt” (china tj数据)
   1003,王五,1993-2-8,1000.0
   1004,赵六,1992-3-8,2000.0
   1006,郭德纲,1999-6-9,6000.0
   1007,胡鑫喆,1999-7-9,7000.0
   ```

2. 建表

   ```mysql
   create external table t_user_part(
   	id string,
   	name string,
   	birth date,
   	salary double
   )partitioned by(country string,city string)--指定分区列,按照国家和城市分区。
   row format delimited
   fields terminated by ',' 
   lines terminated by '\n';
   ```

3. 导入数据

   ```powershell
   # 导入china和bj的数据
   load data local inpath '/opt/bj.txt' into table t_user_part partition(country='china',city='bj');
   # 导入china和tj的数据
   load data local inpath '/opt/tj.txt' into table t_user_part partition(country='china',city='tj');
   ```

   ![image-20210725142241752](Hive笔记.assets/image-20210725142241752.png)

4. 使用分区表

   ```mysql
   -- 查看分区信息
   show partitions t_user_part;
   
   -- 使用分区查询：根据分区字段查询，只在满足条件的分区上查询，提高查询效率
   select * from t_user_part where city = 'bj'
   -- 分区表并不影响全表范围的查询
   select * from t_user_part
   ```

总结：

- 管理表：Hive除了管理MySQL中的元数据还管理HDFS上的表数据；删除管理表，元数据和数据都会删除
- 外部表：Hive只管理MySQL中的元数据；删除外部表不会删除HDFS中的数据
- 分区表：将表中数据按照不同分区保存管理。当根据分区字段查询时，Hive会自动对分区内数据检索，提高Hive的查询效率

实际开发中经常组合使用 **外部表**+ **分区表**

## 9 Hive自定义函数

Hive内置了数百个函数，极大的方便了我们进行数据分析。但是，有些时候一些复杂的需求无法通过内置函数解决，这时候需要编写**用户自定义函数（user-defined function，UDF）**。

Hive中有3种UDF：

- 普通UDF(user-defined function，用户自定义单行函数) 
-  UDAF(user-defined aggregate function，用户自定义聚合函数，也就是组函数)  
- UDTF(user-defined table-generating function，用户自定义表生成函数，也就是炸裂函数)

内置函数

```markdown
# 查看hive内置函数
show functions;
# 查看函数描述信息
desc function max;
```

### 9.1 UDF

UDF操作作用于单个数据行，且产生一个数据行作为输出。大多数函数（比如数学函数和字符串函数）都属于这一类。简单概括：处理一行，返回一行

开发步骤：

1. 导入hive依赖

   ```xml
   <!-- https://mvnrepository.com/artifact/org.apache.hive/hive-exec -->
   <dependency>
       <groupId>org.apache.hive</groupId>
       <artifactId>hive-exec</artifactId>
       <version>1.2.1</version>
   </dependency>
   ```

2. 编码：继承UDF父类

   ```java
   public class AddExactUDF extends UDF {
       /*
       方法名必须是evaluate，方法签名可以是如下案例所示：
       public int evaluate();
       public int evaluate(int a);
       public double evaluate(int a, double b);
       public String evaluate(String a, int b, Text c);
       public Text evaluate(String a);
       public String evaluate(List<Integer> a);
        */
      public long evaluate(long a,long b){
          return a+b;
      }
   }
   ```

3. 使用Maven工具打包

   ```powershell
   D:\ideaProject\hive-test> mvn package -DskipTests

4. 将上一步打的jar包，上传到Linux

   ```powershell
   [root@hadoop10 app]# ls
   hadoop-test.jar  hive-test-1.0-SNAPSHOT.jar.jar
   
   [root@hadoop10 app]# mv hive-test-1.0-SNAPSHOT.jar hive-test.jar #方便后续使用，改名为hive-test.jar

5. 导入到函数到Hive中

   ```mysql
   # 在hive命令中执行
   add jar /opt/app/hive-test.jar; # hive session级别的添加，
   delete jar /opt/app/hive-test.jar; # 如果重写，记得删除。
   
   #导入函数
   create [temporary] function 函数名（自定义） as "自定义函数全类名"; # temporary是会话级别,只在当前连接中有效。
   create [temporary] function addExact as "com.baizhi.function.AddExactUDF"; 
   # 删除导入的函数
   drop [temporary] function 函数名;
   drop [temporary] function addExact;
   ```
   
6. 使用函数

   ```mysql
   SELECT addExact(1,2);
   ```

   

解决一个环境问题：无法通过maven下载一个依赖，解决方案

![image-20210725235938599](Hive笔记.assets/image-20210725235938599.png)

```markdown
# 1. 关闭idea，并删除本地仓库 org/pentaho/pentaho-aggdesigner-algorithm 文件夹

# 2.手动下载依赖 （资料中也已经提供）
https://public.nexus.pentaho.org/repository/proxied-pentaho-public-repos-group/org/pentaho/pentaho-aggdesigner-algorithm/5.1.5-jhyde/pentaho-aggdesigner-algorithm-5.1.5-jhyde-javadoc.jar

# 3.手动安装依赖到本地仓库
D:\work\pentaho-aggdesigner-algorithm-5.1.5-jhyde-javadoc.jar
# 执行mvn安装本地依赖的命令（直接复制粘贴）
D:\work> mvn install:install-file -DgroupId=org.pentaho -DartifactId=pentaho-aggdesigner-algorithm  -Dversion=5.1.5-jhyde  -Dpackaging=jar  -Dfile=pentaho-aggdesigner-algorithm-5.1.5-jhyde-javadoc.jar

# 4.启动idea，刷新项目
```

**实战案例：** 自动编号（从1开始）

1. 编码

   ```java
   /*
       默认情况下一个函数的输入不变，则输出也不变：
           就会出现调用下面这个函数每次都返回1的情况
   
       将deterministic属性设置为false，每次都会重新计算
    */
   @UDFType(deterministic = false)
   public class NumberUDF extends UDF {
       private long index = 0;
       public long evaluate(){
           index++;
           return index;
       }
   }
   ```

2. 重新打包上传导入到Hive中

   ```powershell
   -- 在hive中执行
   delete jar /opt/app/hive-test.jar
   add jar /opt/app/hive-test.jar
   
   create temporary function get_number as "com.baizhi.function.NumberUDF"
   ```

3. 使用

   ```mysql
   select get_number() num,id,name,salary from t_person;
   ```

   

### 9.2 UDTF

UDTF操作走用于单个数据行，且产生多个数据行（即一张表）作为输出。比如 `explode`函数,我们模拟explode的功能，完成一个炸裂string的函数 `string_explode(使用分隔符连接的字符串,分隔符)`，将使用特定分隔符连接的字符串炸裂开来。

1. 导入Hive依赖

   ```xml
   <!-- https://mvnrepository.com/artifact/org.apache.hive/hive-exec -->
   <dependency>
       <groupId>org.apache.hive</groupId>
       <artifactId>hive-exec</artifactId>
       <version>1.2.1</version>
   </dependency>
   ```

2. 编码：

   ```java
   public class StringExplodeUDTF extends GenericUDTF {
       private ArrayList<String> outList = new ArrayList<String>();
       
       @Override
       public void process(Object[] args) throws HiveException {
           //1.获取第1个参数值
           String arg = args[0].toString();
           //2.获取第2个参数值，此处为分隔符
           String splitKey = args[1].toString();
           //3.将原始数据按照传入的分隔符进行切分
           String[] fields = arg.split(splitKey);
           //4.遍历切分后的结果，并写出
           for (String field : fields) {
               //集合为复用的，首先清空集合
               outList.clear();
               //将每一个单词添加至集合
               outList.add(field);
               //将集合内容写出
               forward(outList);
           }
       }
   
       @Override
       public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
           //1.定义输出数据的列名和类型
           List<String> fieldNames = new ArrayList<String>();
           List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
           //2.添加输出数据的列名和类型
           fieldNames.add("lineToWord");
           fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
           return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
       }
   
       
   
       @Override
       public void close() throws HiveException {
       }
   
   }
   ```

3. 使用Maven工具打包

   ```powershell
   D:\ideaProject\hive-test> mvn package -DskipTests
   ```

   

4. 将上一步打的jar包，上传到Linux

   ```powershell
   [root@hadoop10 app]# ls
   hadoop-test.jar  hive-test-1.0-SNAPSHOT.jar.jar
   
   [root@hadoop10 app]# mv hive-test-1.0-SNAPSHOT.jar hive-test.jar #方便后续使用，改名为hive-test.jar
   ```

5. 导入到函数库中，使用函数

   ```mysql
   # 在hive命令中执行
   add jar /opt/app/hive-test.jar; # hive session级别的添加，
   delete jar /opt/app/hive-test.jar; # 如果重写，记得删除。
   
   #导入函数
   create [temporary] function string_explode as "com.baizhi.function.StringExplodeUDTF"; # temporary是会话级别。
   # 删除导入的函数
   drop [temporary] function string_explode;
   ```
   
6. 使用函数

   ```mysql
   select string_explode("a-b-c-d","-");
   ```

   

## 10 导入数据

将数据导入到Hive中有多种方式，我们来详细学习一下：

1. 将文件数据导入到Hive中

   ```powershell
   load data [local] inpath '文件路径' [overwrite] into table 表;
   ```

2. 查询结果的同时创建新表

   ```mysql
   -- 语法：create table 新表 as select 语句  
   --	1. 执行select语句
   --  2. 创建新表，将查询结果添加到表中
   
   -- 示例：
   create table t_person3 
   as select * from t_person
   ```

   

3. 将查询结果导入到已经存在的表中

   ```mysql
   -- 语法： insert [into|overwrite] table 表名 select 语句
   -- into 追加数据到表中、overwrite覆盖原表中的数据
   
   -- 示例：
   insert overwrite table t_person3
   select * from t_person
   where id % 2 == 1;
   
   insert into table t_person3
   select * from t_person
   where id % 2 == 0;
   ```

4. 将HDFS中已经存在的文件，导入到新建的表中

   ```mysql
   -- 语法：
   create table 表名(
       ...
   )row format delimited 
   fields terminated by ','
   location 'hdfs的表数据对应的目录';
   
   -- 示例：
   HDFS中有文件/a/b/business.txt，现在创建一个表，使用/a/b作为表的文件夹
   
   create table t_business2 (
   	name string,
   	order_date date,
   	cost int
   )row format delimited
   fields terminated by ','
   location '/a/b';
   ```

5. 将查询结果导入到分区表中

   ```mysql
   -- 语法
   insert overwrite table 表名
   partition (分区字段1=值1,分区字段2=值2,...)
   select 语句
   
   -- 示例：要求导入数据的表要提前创建
   create table t_person4(
       id string,
       name string,
       salary double,
       sex char(1),
       hobbies array<string>,
       cards map<string,string>,
       addr struct<city:string,zipCode:string>
   ) 
   partitioned by(birthday date) 
   row format delimited
   fields terminated by ',';
   
   insert overwrite table t_person4
   partition(birthday = '2019-10-09')
   select id,name,salary,sex,hobbies,cards,addr
   from t_person;
   ```

   

