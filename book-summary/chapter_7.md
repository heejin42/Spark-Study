# Learning Spark
## chapter 7. 스파크 최적화 및 튜닝

스파크는 튜닝을 위한 많은 설정이 있지만 중요하고 자주 사용되는 것만 다룰 것이며 자세한 것은 공식 문서를 참조한다.

## 아파치 스파크 설정 확인 및 세팅

스파크 설정을 확인하고 설정하는 방법은 세가지가 있다.

1. 설정 파일을 통한 방법
배포한 $SPARK_HOME 디렉터리안에 
*conf/spark-defaults.conf.template,
conf/log4j.properties.template,*
*conf/spark-env.sh.template 
이 파일들 에 있는 기본값을 변경하고 .template 부분을 지우고 저장한다.
(conf/spark-defaults.conf 설정을 바꾸면 클러스터와 모든 애플리케이션에 적용)*
2. 스파크 애플리케이션 안에서 혹은 명령 행에서 —conf 옵션을 사용

명령 행에서 —conf 옵션 사용 코드 사용
    
    ```python
    spark-submit 
    	--conf spark.sql.shuffle.partitions=5 
    	--conf "spark.executor.memory=2g" 
    	--class main.scala.chapter7.SparkConfig_7_1 jars/main-scala-chapter7_2.12-1.0.jar
    ```
    
    - 스파크 애플리케이션에 코드 사용
        
        ```scala
        // In Scala
        import org.apache.spark.sql.SparkSession
        def printConfigs(session: SparkSession) = { 
        	// Get conf
        	val mconf = session.conf.getAll
        	// Print them
        	for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
        }
        
        def main(args: Array[String]) { 
        	// Create a session
        	val spark = SparkSession.builder
             .config("spark.sql.shuffle.partitions", 5)
             .config("spark.executor.memory", "2g")
             .master("local[*]")
             .appName("SparkConfig")
             .getOrCreate()
        
           printConfigs(spark)
           spark.conf.set("spark.sql.shuffle.partitions",
             spark.sparkContext.defaultParallelism)
           println(" ****** Setting Shuffle Partitions to Default Parallelism")
           printConfigs(spark)
        }
        ```
        
3. 스파크 셸에서 프로그래밍 인터페이스 사용
REPL(read-eval-print loop) 셸 환경에서 가볍게 반복 실행을 통해 테스트하는 환경인데 이것을 통해 확인 가능

    
    ```python
    # 파이스파크 3.0 이상에서는 getAll() 메서드가 spark.conf 대신 
    # spark.sparkContext.getConf().getAll()으로 변경되었습니다.
    
    print('\n'.join([f"{k} -> {v}" for k, v in spark.sparkContext.getConf().getAll()]))
    스파크 SQL 전용 설정들도 볼 수 있다.
    ```
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/7d0740b7-ac1b-44d8-a6a5-5bf0efe844b7/Untitled.png)
    
    ```python
    from pyspark.sql import SparkSession
    spark = (SparkSession
          .builder
          .appName("Chatper7")
          .getOrCreate())
    spark.sql("SET -v").select("key", "value").show(n=5, truncate=False)
    ```
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/391af160-948e-421b-97ec-aa4c23f84b89/Untitled.png)
    
4. 스파크 UI 환경 탭을 통해 확인만 가능 (변경 불가능)

spark.conf.isModifiable("*<config_name>*")을 호출하면 true or false를 리턴해 변경 가능한지를 알려준다.

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/b54dd374-79d3-4bd5-b067-906ad8b026d1/Untitled.png)

앞의 방법들 간에도 읽히는 우선순위가 있다.

1. spark-default.conf에 정의된 값이나 플래그
2. spark-submit의 명령 행 설정
3. SparkSession을 통해 설정된 값

이 모든 값들을 다 합쳐지면서 순서에 따라 중복된 설정은 초기화 된다.

적절한 설정값은 성능에 도움이 되며 커뮤니티를 통해 실무자들에 의해 도출된 내용이며 대규모 워크로드에 대한 클러스터 사용을 극대화하는것에 초점을 맞추었다.

### 대규모 워크로드를 위한 스파크 규모 확장

1. 정적/동적 자원 할당
    - spark-submit 에 명령 행 인자로 자원량을 지정하면 제한을 두게 된다. 이렇게 되면 워크로드보다 더 방대한 작업으로 로딩이 걸려도 추가적인 자원할당이 불가능하다는 뜻이다.
    - 스파크의 동적 자원 할당 설정 사용하면 더 할당하거나 줄일 수 있다. 이 설정 기능이 도움이 되는 예시로는 **스트리밍과 온디맨드 데이터 분석**이 있다.
    - 동적 할당 활성화 세팅 예시
        
        ```bash
        ## 설정 중 일부는 스파크 REPL안에서 쓸 수 없다.
        spark.dynamicAllocation.enabled true # 클러스터 매니저가 실행하도록 요청
        spark.dynamicAllocation.minExecutors 2 #최소 2개 executor
        spark.dynamicAllocation.schedulerBacklogTimeout 1m 
        # 1분이상 스케줄링되지 않는 테스크 있으면 백로깅된 태스크들이 실행될 새로운 executor 실행해도록 요청
        spark.dynamicAllocation.maxExecutors 20 
        # 백로깅된 테스크들이 실행될 executor 맥시멈 20개까지 
        spark.dynamicAllocation.executorIdleTimeout 2min # 테스크 완료후 2분이상 놀면 중지
        ```
        

1. 스파크 이그제큐터(executor)의 메모리와 셔플 서비스 설정
    - **각 executor 에서 사용가능한 메모리 양**은 spark.executor.memory에 의해 제어된다. 이때 메모리는 예비 메모리에 300mb 를 할당하고 , 나머지 부분을 **실행 메모리(60%) + 저장 메모리(40%)로 관리**한다.
        - 예비 메모리 300mb를 빼놓는 이유는 , OOM(out of memory)에러를 피하기 위함이다.
    - **실행메모리는 스파크의 셔플, 조인, 정렬, 집계** 등에 사용된다. 이 부분은 튜닝이 일반적이진 않지만 고치기는 쉽다. 반면 **저장 메모리는 사용자 데이터 구조를 캐싱하고 데이터 프레임에서 온 파티션들을 저장**하는데 주로 쓰인다.
    
    [셔플내용은 추후에 추가해두겠습니다]
    
2. 스파크 병렬성 최대화 
    - 스파크의 유용성은 여러 태스크를 동시에 대규모로 실행할 수 있는 것에서 기인한다.
        1. 어떻게 스파크가 데이터를 저장장치에서 읽어서 메모리에 넣는지
        2. 파티션이 스파크에서 어떤 역할을 하는 지 
            
            ** 파티션이란? 
            
            👉 데이터를 **관리가능하고 쉽게 읽어 들일 수** 있도록 디스크에 **연속된 위치에 조각이나 블록들의 모음으로 나눠서 저장**하는 방법
            
            ⭐ 이 나눠진 데이터 모음은 병렬적/독립적으로 읽어서 처리가 가능하다. 
            
        
    - 자원사용을 최적화하고 병렬성을 최대로 끌어올리려면, 이그제큐터(executor)에 할당된 코어 개수만큼 파티션들이 최소한 할당되야한다. 
    
    하나의 코어에서 돌아가는 하나의 스레드는 하나의 파티션을 처리할 수 있다.


디스크의 데이터는 저장장치에 따라 조각이나 연속된 파일 블록으로 존재.(64~128MB)

스파크에서 한 파티션의 크기는 spark.sql.files.maxPartitionBytes에 따라 결정됨. (기본값 128MB)

이 크기를 줄이면 ‘작은 파일 문제’를 만나게됨(작은 파티션 파일이 많아지며 디스크 I/O양 급증 등으로 성능 저하 → 분산 파일 시스템이 느려짐.)

셔플파티션 - 셔플 단계에서 만들어짐. 기본적으로 셔플 파티션의 개수는 spark.sql.shuffle.partitions에 50으로 지정됨. 데이터 크기에 따라 이 숫자를 조정하여 너무 작은 파티션들이 이그제큐터들에게 할당되지 않게 할 수 있다.

groupBy,join 같은 넓은 트랜스포메이션으로 알려진 작업중에 생성되는 셔플 파티션은 네트워크와 디스크 I/O를 모두 사용. 이런 작업중에는 셔플 중에 spark.local.directory에 지정된 이그제큐터의 로컬 디렉터리에 중간 결과를 쓰게 됨→SSD로 성능 올릴 수 있음

셔플단계의 셔플 파티션 갯수를 정하는 공식은 존재 하지 않음 . 수정-테스트 하는 방식으로 접근해야함

캐싱(cache)과 영속화(persist)의 차이 - 후자는 데이터가 저장되는 위치와 방식에 대해 좀 더 세밀한 설정 가능(메모리/디스크 여부, 직렬화 여부)

-어느쪽이든 자주 접근하는 데이터 프레임이나 테이블에 대해 더 나은 성능을 보여주는데 기여

cache()

데이터 프레임은 그 중 일부만 캐시될 수 도 있지만 그 파티션들은 개별 파티션의 일부만 저장될 수 없다. (e.g, 4.5파티션 정도만 들어갈 메모리만 있다면 4개의 파티션이 캐시됨)

하지만 모든 파티션이 캐시된 것이 아니라면 데이터에 접근을 다시 시도할 때 캐시되지 않은 파티션은 재계산되어야 하고 이는 스파크 잡을 느리게 만들 것이다.

### DataFrame.persist()

persist(StorageLevel.LEVEL)의 함수 호출 방식은 StorageLevel을 통해 데이터가 어떤 방식으로캐시될 것인지 제어

- 디스크의 데이터는 자바든 크리오든 직렬화를 항상 사용하게 된다

StorageLevel 목록

| StorageLevel | 설명 |
| --- | --- |
| MEMORY_ONLY | 데이터가 곧바로 객체 형태로 메모리에 저장됨 |
| MEMORY_ONLY_SER | 데이터가 직렬화되어 용량이 최소화된 바이트 배열 형태로 메모리에 저장됨. 사용 시에 역직렬화를 위한 비용이 소모됨 |
| MEMORY_AND_DISK | 데이터가 곧바로 객체 형태로 메모리에 저장되지만, 부족한 경우 직렬화되어 디스크에 저장됨 |
| DISK_ONLY | 데이터가 직렬화되어 디스크에 저장됨 |
| OFF_HEAP | 데이터가 오프힙(off-heap) 메모리에 저장됨. 오프힙 메모리는 스파크에서 저장 및쿼리 실행에 사용됨. 7장의 ‘스파크 이그제큐터의 메모리와 셔플 서비스 설정’ 참고 |
| MEMORY_AND_DISK_SER | MEMORY_AND_DISK와 비슷하지만 메모리에 저장되는 데이터가 직렬화됨(디스크에 저장되는 데이터를 항상 직렬화됨) |
- 각 StorageLevel은 동일한 기능을 하는(OFF_HEAP 제외) ‘레벨_이름_2’ 형태의 옵션이 존재하는데, 이는 서로 다른 스파크 이그제큐터에 복제해서 두 벌이 저장된다는 것을 의미.
    - 이 옵션을 사용하면, 캐싱에 자원을 더 소모하지만, 데이터를 두 군데에 저장하게 되므로 장애 상황 시 다른 카피본에 대해 태스크가 스케줄링될 수 있도록 해줌

앞의 예제에 persist()함수를 사용

```scala
import org.apache.spark.storage.StorageLevel

// 천만 개의 레코드를 가지는 데이터 프레임 생성
val df = spark.range(1*10000000).toDF(“id”).withColumn(“square”, $”id” * $”id”)
df.persist(StorageLevel.DISK_ONLY) // 데이터를 직렬화해서 디스크에 저장
df.count() // 캐시 수행

res2: Long = 10000000
Command took 2.08 seconds


df.count() // 이제 캐시를 사용

res3: Long = 10000000
Command took 0.38 seconds
```

캐시를 비우고 싶다면, DataFrame.unpersist()를 호출

캐시는 데이터 프레임뿐만 아니라, 데이터 프레임에서 파생된 테이블이나 뷰도 캐시할 수 있다.
```scala
df.createOrReplaceTempView(“dfTable”)
spark.sql(“CACHE TABLE dfTable”)
spark.sql(“SELECT count(*) FROM dfTable”).show()
```
캐시/영속화

- 사용해야 할 경우
    - 큰 데이터세트에 쿼리나 트랜스포메이션으로 **반복적으로 접근**해야 하는 시나리오
        - 반복적인 머신러닝 학습을 위해 계속 접근해야 하는 데이터프레임들
        - ETL(Extract, Transform, Load)이나 데이터 파이프라인 구축 시 빈도 높은 트랜스포메이션 연산으로 자주 접근해야 하는 데이터 프레임들
- 사용하면 안 되는 경우 (= 캐시가 효과가 없는 경우)
    - 데이터 프레임이 메모리에 들어가기엔 **너무 큰 경우**
    - **자주 쓰지 않는** 데이터프레임에 대해 비용이 크지 않은 트랜스포메이션을 수행할 때
    - 메모리 캐시는 직렬화/역직렬화에서 비용을 발생시킬 수 있기 때문에 주의 깊게 사용해야 함

### 스파크 조인의 종류

- **조인 연산**
    - 트랜스포메이션 연산 형태, 테이블이나 데이터프레임 형태로 된 두 종류의 데이터세트를 공통적으로 **일치하는 키를 기준으로 병합**하는 연산
    - 관계형 데이터베이스와 유사하게 스파크 데이터프레임과 데이터세트 API, 스파크 SQL은 여러 조인 트렌스포메이션(내외부 조인, 좌측/우측 조인 등)을 제공하며, **스파크 이그제큐터들 사이에 방대한 데이터 이동**을 일으킴
    - 스파크는 이그제큐터 간 데이터를 교환, 이동, 정렬, 그룹화, 병합하는 다섯 종류의 조인 전략을 가짐 (책에서는 V 표시된 경우만 다룸)
        - 브로드캐스트 해시 조인 (V)
        - 셔플 해시 조인 (V)
        - 셔플 소트 머지 조인
        - 브로드캐스트 네스티드 루프 조인
        - 셔플 복제 네스티드 루프 조인
- **브로드캐스트 해시 조인 (Broadcast hash Join, BHJ)**
    - = 맵사이드 조인 (map-side-only-join)
    - **한쪽은 작고**(드라이버와 이그제큐터 메모리에 들어갈 사이즈) **다른 쪽은 큰 두 종류의 데이터**를 사용, 특정 조건이나 칼럼 기준으로 조인
        - 더 작은 쪽 데이터가 드라이버에 의해 모든 스파크 이그제큐터에 복사되어 뿌려지고, 이어서 각 이그제큐터에 나뉘어 있는 큰 데이터와 조인됨
        - 큰 데이터 교환이 일어나지 않게 함
        
        ![스크린샷 2023-05-07 오후 4.49.34.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/d7f45abc-a208-41f6-996a-f6da2a4b979c/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-05-07_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_4.49.34.png)
        
    - 스파크는 기본적으로 작은 쪽 데이터가 10MB 이하일 때 브로드캐스트 조인을 사용 (`spark.sql.autoBroadcastJoinThreshold`)
    - 사용 예제 : 두 데이터 프레임에 공통 키들이 존재하고, 한쪽 정보가 적은데 양쪽 뷰를 병합하는 경우 (큰 데이터세트 `playersDF`와 작은 데이터세트 `clubsDF` 조인)
        
        ```scala
        // 스칼라 예제
        import org.apache.spark.sql.functions.broadcast
        val joinedDF = playersDF.join(broadcast(clubsDF), "key1 === key2")
        ```
        
    - 셔플이 일어나지 않으므로 스파크가 제공하는 가장 쉽고 빠른 조인 형태
    - 어떤 조인이 사용되었는지 확인하는 코드 (출력모드(`mode`) : `simple`, `extended`, `codegen`, `cost`, `formatted`)
        
        ```scala
        joinedDF.explain(mode)
        ```


### 셔플 소트 머지 조인 최적화

- 동등 조건 조인을 빈번하게 수행하고 싶을 때, 공통의 정렬된 키나 컬럼을 위한 파티션된 버킷을 만들면 Exchange 단계를 없앨 수 있다.
    - Exchange 단계 : 분산 데이터 처리 과정 중에서 shuffling이 일어나는 단계를 말한다.
        - shuffling : 이전 단계의 결과를 기반으로 데이터를 새로운 파티션으로 재분배하는 작업으로, 병렬 처리가 가능하다.
        - 작업 단계 : 데이터 파티션을 메모리에 로드 → 로드한 데이터를 적절한 파티션으로 재분배 → 재분배된 데이터를 디스크에 저장
- 사전 정렬 및 데이터 재구성을 시도하면 exchange 단계를 생략할 수 있고, 곧바로 WholeStageCodegen으로 넘어가기 때문에 성능을 올릴 수 있다.
    - WholeStageCodeGen : SQL 쿼리 및 DataFrame 등의 연산에 대해 실행 계획을 최적화하여 실행 속도를 높이는 방법 중 하나. (쿼리 성능 향상)

- scala → python code: usersDF, ordersDF (이전 코드)
    
    ```python
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
            .config("spark.jars", "./jar/mysql-connector-j-8.0.32.jar") \
            .master("local") \
            .appName("mysql") \
            .getOrCreate()
    
    import random
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    states = {0: "AZ", 1: "CO", 2: "CA", 3: "TX", 4: "NY", 5: "MI"}
    items = {0: "SKU-0", 1: "SKU-1", 2: "SKU-2", 3: "SKU-3", 4: "SKU-4",
    5: "SKU-5"}
    rnd = random.Random(42)
    
    usersDF = spark.createDataFrame([(id, f"user_{id}", f"user_{id}@databricks.com", states[rnd.randint(0, 4)]) for id in range(1000001)], ["uid", "login", "email", "user_state"])
    ordersDF = spark.createDataFrame([(r, r, rnd.randint(0, 9999), 10 * r* 0.2,
    states[rnd.randint(0, 4)], items[rnd.randint(0, 4)]) for r in range(1000001)], ["transaction_id", "quantity", "users_id", "amount", "state", "items"])
    
    usersOrdersDF = ordersDF.join(usersDF, ordersDF.users_id == usersDF.uid) # Show the joined results
    usersOrdersDF.show()
    ```
    
    - **실행결과**
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/863f6108-3a90-4dcf-a0cf-fe80a0df7b6f/Untitled.png)
    
    ```python
    usersOrdersDF.explain()
    ```
    
    - **실행결과**
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/f264ecdd-f4bf-4ff9-a858-55907d537727/Untitled.png)
    

- **bucketBy** : 데이터를 저장할 때, 동일한 버킷(column) 값을 가지는 row 값을 동일한 파티션에 저장하도록 지정하는 메소드이다.
    - 데이터를 분석하는 데 필요한 작업을 최소화하기 위해 데이터를 논리적으로 그룹화하고 파티션에 저장한다.
    - 데이터를 그룹화하면, 데이터 처리 시 특정 컬럼의 값을 조건으로 필터링하거나 조인할 때 더 효율적인 처리가 가능하다.

```python
from pyspark.sql.functions import *

usersDF.orderBy(asc("uid")) \
        .write.format("parquet") \
        .bucketBy(8, "uid") \ 
        .mode("overwrite") \
        .saveAsTable("UsersTbl")

ordersDF.orderBy(asc("users_id")) \
        .write.format("parquet") \
        .bucketBy(8, "users_id") \
        .mode("overwrite") \
        .saveAsTable("OrdersTbl")

spark.sql("CACHE TABLE UsersTbl")
spark.sql("CACHE TABLE OrdersTbl")

usersBucketDF = spark.table("UsersTbl")
ordersBucketDF = spark.table("OrdersTbl")

joinUsersOrdersBucketDF = ordersBucketDF .join(usersBucketDF, ordersBucketDF.users_id == usersBucketDF.uid)
joinUsersOrdersBucketDF.show()

```

- **실행결과**

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/61205342-5ac7-4a51-808b-913ca492e160/Untitled.png)

```python
joinUsersOrdersBucketDF.explain()
```

- **실행결과**

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/b964c24c-94c9-45b2-974d-96c33cd442e9/Untitled.png)

- write → parquet 파일 생성 (standalone 기준)

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/9d6ece1b-b10b-4aa2-9f5e-843f76d3da69/Untitled.png)



### Shuffle Sort **Merge Join 최적화 (이어서)**

앞의 예제에서는 공통의 정렬된 키(컬럼)에 따라 파티셔닝 된 bucket을 사용하여 조인했기 때문에 조인 결과도 정렬되어 있다. 그러므로 조인 과정에서 정렬할 필요가 없다.
exchange가 수행되지 않고 바로 WholeStageCodegen(Spark SQL의 물리적 쿼리 최적화 단계)로 넘어가는 것을 확인할 수 있다.

![스크린샷 2023-05-07 오후 9.28.05.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/f86af3ed-f93c-4f45-baf8-837bf15343a1/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-05-07_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_9.28.05.png)

### 어떤 경우에 Shuffle Sort **Merge Join을 사용하는가?**

- 두개의 큰 데이터세트의 각 key가 정렬 및 해싱되어 스파크 내 동일한 partition에 있을 때
- 동일한 정렬된 키로 두 데이터세트를 결합하는 동일 조건 조인을 수행하는 경우
- 네트워트 간에 규모가 큰 셔플을 일으키는 exchange 연산과 sort 연산을 안하고 싶을 때

### Environment탭

⇒ 스파크가 돌아가고 있는 환경에 대해 알려준다(트러블 슈팅에 유용한 많은 단서 제공)

Ex) 환경 변수, jar 파일, 스파크 특성, 시스템 특성, 런타임 환경(JVM/Java Version)

- 스파크 클러스터의 실행 설정
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/0439c6c3-97ca-4c39-af89-3546a84cde01/Untitled.png)
    

### 스파크 애플리케이션 디버깅

⇒ UI를 통해 디버깅이나 문제 해결에 쓸 수 있는 정보 제공 (모든 에러 로그 보여줌)

- IntelliJ IDEA와 같은 IDE에서 로컬호스트 스파크를 띄워 디버깅 가능

## 요약

- 스파크 애플리케이션을 튜닝하기 위한 여러 가지 최적화 기술 살펴봄
1. 기본적인 스파크 설정 ⇒ 1. 대용량 워크로드 개선 2.Excutor의 메모리 부족 현상 최소화
2. 데이터 접근 용이하게 하기 위해 ⇒ 적절한 캐시, 영속화 전략 선택
3. 복잡한 집계에 쓰이는 두 가지 조인 살펴봄
4. 정렬 키에 따라 bucketing해 비싼 셔플 연산 피할 수 있는 예제 시연 
5. 시각적 관점을 얻을 수 있는 스파크 UI