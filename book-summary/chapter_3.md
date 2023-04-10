# 3장 Apache Spark’s Structured APIs

소개글

- Apache Spark가 어떻게 생겨났고, Spark의 버전들의 특성을 간략하게 소개하고 있다.
- 새로운 API를 살펴보기 전에 RDD API 모델을 살펴보자고 한다.

## 1. Spark: What’s Underneath an RDD?

RDD는 스파크에서 가장 기초적인 추상화이고, RDD에는 3가지의 중요한 특성이 있다

1. 종속성 (Dependencies)
2. 파티션 (Partitions)
3. 계산 기능 (Compute function) => Iterator[T]
- Iterator 관련 자료
[https://onlyfor-me-blog.tistory.com/319](https://onlyfor-me-blog.tistory.com/319)

3가지 모두 간단한 RDD 프로그래밍 API 모델에 필수적이다.

- 종속성

스파크에 입력으로 RDD를 구성하는 방법을 **지시하는 종속성 목록이 필요**하다.

결과를 재현하기 위해 필요한 경우 Spark는 이러한 종속성에서 RDD를 재생성하고 RDD에 대한 작업을 복제할 수 있습니다. (이러한 특징은 RDD의 탄력성을 부여한다)

- 파티션

파티션은 Spark에 **작업을 분할**하여 실행자 간에 파티션의 계산을 **병렬화** 할 수 있는 기능을 제공한다. 스파크는 로컬리티 정보를 사용하여 데이터에 가까운 실행자에게 작업을 보낸다.

그렇게 하면 네트워크를 통해 전송되는 데이터가 줄어든다.

- 계산 기능

RDD 계산 기능은 RDD에 **데이터가 저장될 수 있도록 Iterator 기능을 제공**한다.

이런 RDD에 몇 가지 문제점이 있는데,

- 계산 기능이 불명확하다 (Spark는 오직 lambda 표현만 본다)
- Iterator[T] 데이터 타입 또한 불명확하다 (Spark는 Python에 있는 일반 개체라는 것만 안다)
- 의도를 전혀 이해하지 못한다
- Spark는 T에 있는 특정 데이터 타입의 지식이 없다 (특정 column 개체 접근 할 수 없음)

⇒ 이러한 불투명성은 Spark가 효율적인 query문을 작성하는데 방해가 된다.

## 2. Structuring Spark

- Clarity and Simplicity (명료함과 단순성)

Spark 2.x는 Spark를 구성하는 몇 개의 key 스키마로 소개되는데, 데이터 분석에서 패턴을 사용하면서 발견되는 계산은 filtering, selecting, counting.. 과같은 high level 작업으로 표현된다. 

- Specificity (구체성)

DSL 연산 세트를 통하여, Spark's에 제공된 언어(Java, Python, Spark, R, and SQL)에서 API를 사용 가능하게 된다.

이 연산자를 사용하면 Spark에 데이터로 무엇을 계산하고 싶은지 알릴 수 있으며, 결과적으로 실행을 위한 효율적인 쿼리를 구성할 수 있다.

명령과 구조의 마지막 스키마는 데이터가 SQL 테이블이나 스프레드시트 같은 형식으로 정렬될 수 있도록 해준다.

## 3. Key Merits and Benefits

- Structure는 스파크 구성 요소 전반에 걸쳐 향상된 성능과 공간 효율성을 포함한 여러 가지 이점을 제공한다 (표현성, 단순성, 구성성 및 일관성)

- 표현성과 구성성이 있는 코드 (각 이름에 대한 모든 연령을 다음 기준으로 집계)

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/8b5b969e-8bf9-4177-8b37-bd6c23d960b8/Untitled.png)

⇒  스파크에게 일련의 **람다 함수**를 사용하여 키를 집계하고 평균을 계산하는 방법이 **암호화**되어 있고 **읽기 어렵다.**

- 표현력이 뛰어나고 단순한 코드

(높은 수준의 DSL 연산자와 DataFrame API를 사용하여 동일한 쿼리를 표현한 코드)

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/982808f8-5ddc-4610-815f-2da36a2c018c/Untitled.png)

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/80495b53-10bc-4003-b547-ce27dffc8b0c/Untitled.png)

1) 사람들의 이름으로 그룹화하고

2) 나이를 집계한 다음

3) 같은 이름을 가진 모든 사람들의 평균 나이 계산

⇒ 쿼리를 구성하기 위해 이러한 연산자를 사용함

⇒ 고급 연산자를 단일 단순 쿼리로 사용하여 전체 계산을 구성


### 고수준 DSL연산자들과 데이터프레임 API를 쓴다면?

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/6bc58a61-0127-4e61-8a59-4dfbc3ffe1a9/Untitled.png)

- 스파크에게 **무엇을 할지** 알려주므로 표현력이 높으며 간단하다.
- 스파크는 이런 쿼리를 파악해 사용자의 의도를 이해할 수 있기에 연삭 최적화가 가능하다.
- 스파크 상위 수준 API는 컴포넌트들과 언어를 통틀어 일관성을 갖는다.
    - ex. 아래 스칼라 코드는 앞의 파이썬 코드와 같은 일을 하면서 형태도 비슷하다.
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/8345f6a2-d5f5-45f3-99df-cbed23faf3e9/Untitled.png)
    
- 이러한 단순성이나 표현력을 상위 수준 구조화 API 위에 구축된 스파크 SQL 엔진 덕택에 가능한 것이다.
- 추후 스파크 SQL 엔진에 대해 살펴볼 예정이다.(해당 과정에서는 자주 쓰이는 연산을 위한 API와 DSL이 분석에 어떻게 쓰이는지 살펴본다.)

### 데이터 프레임 API

- 스파크 데이터프레임
    - 구조, 포맷 등 특정 연산 등에 있어 판다스 데이터 프레임의 영향을 받았다.
    - 이름 있는 컬럼과 스키마를 가진 분산 인메모리 테이블처럼 동작한다.
    - integer, string, array, real, data, timestamp 등의 데이터 타입을 가질 수 있다.
    - ex. 데이터 프레임 표 형태 포맷
        
        ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/230be6fb-59fd-4a99-878d-74240a2c0272/Untitled.png)
        

### 스파크의 기본 데이터 타입

- 스파크는 지원 프로그래밍 언어에 맞게 기본적인 내부 데이터 타입을 지원한다.
    - ex. In 스칼라, 어떤 컬럼 이름이 String, Byte 등 타입 중 하나가 되도록 선언할 수 있다.
- 스파크에서 지원하는 기본 데이터 타입 #파이썬
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/2aa30a40-48ee-4433-812e-60bb11a9ce9a/Untitled.png)
    


스파크의 파이썬 정형화 데이터 타입

| 데이터타입 | 스칼라에서 할당되는 값 | 초기 생성 API |
| --- | --- | --- |
| BinaryType | bytearry | BinaryType() |
| TimestampType | datatime.datatime | TimestampType() |
| DataType | datatime.data | Datatype() |
| ArrayType | list,tuple,array 중 | ArrayType(datatype,[nullable]) |
| MapType | dict | MapType(keyType, valueType, [nullable]) |
| StructType | list 혹은 tuple | StructType([fileds]) |
| StructField | 해당 필드와 맞는 값의 타입 | StructField(name,dataType,[nullable]) |

스파크에서 스키마 = 데이터 프레임을 위해 칼럼이름 과 연관된 데이터 타입을 정의한 것.  외부 데이터 소스에서 구조화된 데이터를 읽어 들일 때 쓰이게 됨.

데이터 소스에서 큰 파일을 읽어야 한다면 가능한 한 스키마를 미리 정의해주는 것이 좋다.

( 왜냐하면 스파크가 데이터 타입을 추측해야 하는 책임을 덜어주고, 

스키마를 확정하기 위해 별도의 잡을 만드는 것을 방지하고(비용과 시간의 절약), 

데이터가 스키마와 맞지 않는 경우 문제를 조기에 발견 할 수 있음)

스키마를 정의하는 두 가지 방법

1. 프로그래밍 스타일

```python
schema=StructType([StructField("author",StringType(),False),
StructField("title",StringType(),False),
StructField("pages",IntegerType(),False)])
```

1. DDL

```python
schema=" 'author' STRING, 'title' STRING, 'pages' INT”
```


- JSON 파일에서 데이터 읽어오기 (python)
    - blogs.json
        
        ```python
        {"Id":1, "First": "Jules", "Last":"Damji", "Url":"https://tinyurl.1", "Published":"1/4/2016", "Hits": 4535, "Campaigns": ["twitter", "LinkedIn"]}
        {"Id":2, "First": "Brooke","Last": "Wenig","Url": "https://tinyurl.2", "Published": "5/5/2018", "Hits":8908, "Campaigns": ["twitter", "LinkedIn"]}
        {"Id": 3, "First": "Denny", "Last": "Lee", "Url": "https://tinyurl.3","Published": "6/7/2019","Hits": 7659, "Campaigns": ["web", "twitter", "FB", "LinkedIn"]}
        {"Id": 4, "First":"Tathagata", "Last": "Das","Url": "https://tinyurl.4", "Published": "5/12/2018", "Hits": 10568, "Campaigns": ["twitter", "FB"]}
        {"Id": 5, "First": "Matei","Last": "Zaharia","Url": "https://tinyurl.5", "Published": "5/14/2014", "Hits": 40578, "Campaigns": ["web", "twitter", "FB", "LinkedIn"]}
        {"Id":6,  "First": "Reynold", "Last": "Xin", "Url": "https://tinyurl.6", "Published": "3/2/2015", "Hits": 25568, "Campaigns": ["twitter", "LinkedIn"] }
        ```
        
    - 52-53p 예제 스키마 사용
        
        ```python
        blogs_df = spark.read.schema(schema).json("blogs.json")
        
        blogs_df.show()
        
        print(blogs_df.printSchema())
        
        print(blogs_df.schema)
        ```
        
        ![스크린샷 2023-04-03 오후 8.33.44.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/a2b1cfda-689a-4826-ab82-f78ae051d035/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.33.44.png)
        
        ![스크린샷 2023-04-03 오후 8.38.24.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/8286195e-4d66-494b-a8b8-501d91ae0d07/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.38.24.png)
        

### 칼럼과 표현식

**칼럼**

- 어떤 특정한 타입의 필드를 나타내는 개념
    - 개념적으로 판다스나 R에서의 데이터 프레임이나 RDBMS 테이블의 칼럼과 유사
- 사용자는 이름으로 칼럼을 나열해 볼 수도 있고, 관계형 표현이나 계산식 형태의 표현식으로 그 값들에 연산을 수행할 수 있음
- public 메소드를 가진 객체로 표현 (`칼럼` 타입)
- 논리식이나 수학 표현식도 사용 가능
    - `expr(”columnName * 5”)` (`columnName`이 `integer`, `string` 등의 타입일 경우)
    - `(expr(”columnName - 5”) > col(anothercolumnName))`
- 칼럼으로 가능한 예제 (python)
    - 52-53p에서 만든 데이터프레임 사용
    
    ```python
    print(blogs_df.columns)
    ```
    
    ![스크린샷 2023-04-03 오후 8.16.04.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/6872db1a-dcac-4097-9298-c60273759393/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.16.04.png)
    
    ```python
    # col으로 특정 칼럼에 접근하면 Column 타입을 되돌려 준다.
    print(blogs_df["Id"])
    ```
    
    ![스크린샷 2023-04-03 오후 8.20.46.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/4b4a6970-5c00-41e9-864f-862468571832/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.20.46.png)
    
    ```python
    # 값 계산을 위한 표현식 사용
    blogs_df.select(expr("Hits * 2")).show(2)
    # 혹은 col을 사용한 계산
    blogs_df.select(col("Hits") * 2).show(2)
    ```
    
    ![스크린샷 2023-04-03 오후 8.21.34.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/1240c5ad-e500-4918-b236-92cbf6e15232/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.21.34.png)
    
    ```python
    # 블로그 우수 방문자를 계산하기 위한 식 표현
    # 이 코드는 뒤의 식에 맞는 값으로 "Big Hitters"라는 이름의 새로운 칼럼을 추가한다.
    blogs_df.withColumn("Big Hitters", (expr("Hits > 10000"))).show()
    ```
    
    ![스크린샷 2023-04-03 오후 8.22.09.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/eea9e7d7-9f45-47ef-900e-bc31cd272fc8/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.22.09.png)
    
    ```python
    # 새 칼럼을 연결하여 새로운 칼럼을 만들고 그 칼럼을 보여준다.
    blogs_df.withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id")))).select(col("AuthorsId")).show(4)
    ```
    
    ![스크린샷 2023-04-03 오후 8.22.33.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/de7fd621-5042-4af3-b8f9-e6e407bfd47d/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.22.33.png)
    
    ```python
    # 이 문장들은 모두 동일한 결과를 보여주며 표현만 약간씩 다르다.
    blogs_df.select(expr("Hits")).show(2)
    blogs_df.select(col("Hits")).show(2)
    blogs_df.select("Hits").show(2)
    ```
    
    ![스크린샷 2023-04-03 오후 8.23.01.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/2dab9a3d-7721-4cbb-8649-a25ebcc4cbcd/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.23.01.png)
    
    ```python
    # "Id" 칼럼값에 따라 역순으로 정렬한다.
    blogs_df.sort(col("Id").desc()).show()
    ```
    
    ![스크린샷 2023-04-03 오후 8.23.30.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/27f5a9ae-d1e3-4e42-ada3-256b5896239b/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-04-03_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_8.23.30.png)
    
- Column 객체는 단독으로 존재할 수 없음 → 각 칼럼은 한 레코드의 로우의 일부분이며, 모든 로우가 합쳐져서 하나의 데이터 프레임을 구성



## 로우

하나의 행은 하나 이상의 동일한 타입 or 다른 타입의 칼럼을 갖고 있는 로우(row) 객체로 표현된다.

인덱스는 0부터 시작한다.

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/c98cfaeb-ccca-4bd8-bdea-a608881e9410/Untitled.png)

Row 객체들은 빠른 탐색을 위해 데이터프레임으로 만들어서 사용되기도 한다.

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/6ebb2763-6ca3-4086-a2bf-a4314377233b/Untitled.png)

데이터 프레임을 읽어들이는게 더 일반적이며 스키마를 이용하는 것이 데이터 프레임 작성에 더 빠르고 효율적이다.

## 자주 쓰이는 데이터 프레임 작업들

DataFrameReader

JSON, CSV, 파케이(Parquet), 텍스트, 에이브로, ORC 등의 포멧을 데이터 소스에서 데이터 프레임으로 가지고 온다.

DataFrameWriter

동일하게 특정 포멧의 데이터 소스에서 데이터프레임으로 써서 내보낸다.

## DataFrameReader와 DataFrameWriter 사용하기

고수준의 추상화 및 NoSQL, RDBMS, 아파치 카프카, 키네시스 등의 커뮤니티의 공헌으로 스파크에서 읽고 쓰는 작업은 쉬운편이다.

큰 데이터 프레임은 스키마를 지정하고 DataFrameReader 클래스를 통해서 읽는것이 효과적이다.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Chapter3").getOrCreate()

from pyspark.sql.types import *

# define schema for our data
fire_schema = StructType([
		StructField('CallNumber', IntegerType(), True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', IntegerType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True), 
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)
])

sf_frie_file = '/Users/hyunjun/vscode/Spark/spark-3.3.2-bin-hadoop3/data/sf-fire-calls.csv' 
fire_df = spark.read.csv(sf_frie_file, header=True, schema=fire_schema)
fire_df.show() # 11.3s
```

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/3350432a-9b5f-40a2-bd6d-0a0bccf20c09/Untitled.png)

spark.read.csv() 함수는 CSV파일을 읽어서 row객체와 스키마에 맞는 타입의 이름이 있는 칼럼들로 이루어진 데이터 프레임으로 되돌려준다.

DataFrameWriter으로 원하는 포맷으로 쓸 수 있다. 기본 포멧은 파케이고 스내피로 압축한다.

파케이로 쓰였다면 스키마는 파케이 메타데이터의 일부로 보존이 가능하며 수동으로 스키마를 적용할 필요가 없어진다.



## **데이터 프레임을 파케이 파일이나 SQL 테이블로 저장하기**

```python
parquet_path = …
fire_df.write.format(“parquet”).save(parquet_path)

# 혹은 하이브 메타스토어에 메타데이터로 등록되는 테이블로 저장
parquet_table = …
fire_df.write.format(“parquet”).saveAsTable(parquet_Table)
```

## **트랜스포메이션과 액션**

- 칼럼의 구성 확인 (타입 확인, null 값 확인 등)

## **프로젝션과 필터**

- 프로젝션: 필터를 이용해 특정 관계 상태와 매치되는 행들만 되돌려 주는 방법
- select() 메서드로 수행
- 필터: filter()나 where() 메서드로 표현

```python
few_fire_df = (fire_df
               .select(“IncidentNumber”, “AvailableDtTm”, “CallType”)
							 .where(col(“CallType”) != “Medical Indicident”))
few_fire_df.show(5, truncate = False)

 # 화제 신고로 기록된 CallType의 종류
from pyspark.sql.functions import *
(fire_df
				.select(“CallType”)
				.where(col(“CallType”).isNotNull())
				.agg(countDistinct(“CallType”).alias(“DistinctCallTypes”))
				.show())

# null이 아닌 신고 타입의 목록
(fire_df
				.select(“CallType”)
				.where($”CallType”.isNotNull())
				.distinct()
				.show(10,false))
```

![CA39BEFF-4BC3-4F64-B2BE-A3B2FE7CE899.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/10488bc1-267c-4eee-a6fb-9b1941fcd470/CA39BEFF-4BC3-4F64-B2BE-A3B2FE7CE899.png)

![05ACF05A-827A-4BCF-8A0C-D0AB85A20F35.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/07cc6854-c67f-4fae-bf5a-1cbea6b83141/05ACF05A-827A-4BCF-8A0C-D0AB85A20F35.png)

![791F80EF-F171-49CF-944D-2F541DB54EE8.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/6321b520-33f7-44b7-bc13-47eb447174f9/791F80EF-F171-49CF-944D-2F541DB54EE8.png)

## **칼럼의 이름 변경 및 추가 삭제**

1. StructField를 써서 스키마 내에서 원하는 칼럼 이름들을 지정
2. withColumnRenamed()함수 사용

```python
new_fire_df = fire_df.withColumnRenamed(“Delay”, “ResponseDelayedinMins”)
(new_fire_df
					.select(“ResponseDelayedinMins”)
					.where(col(“ “ResponseDelayedinMins”) > 5)
					.show(5, False))

```

- 데이터 프레임 변형은 변경 불가 방식으로 동작하므로, withColumnRenamed()로 칼럼 이름을 변경할 때는, 기존 칼럼 이름을 갖고 있는 원본을 유지한 채로 칼럼 이름이 변경된 새로운 데이터 프레임을 받아 옴

![9C18EB78-8EAD-43D9-B4E4-CD8FAB1618DC.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/a47160bc-a64f-4660-8525-6dcbc61d4df7/9C18EB78-8EAD-43D9-B4E4-CD8FAB1618DC.png)

데이터 자체를 가공 해야 할 경우가 있음 (지저분하거나, 타입이 적합하지 않은 경우)


- `spark.sql.functions` 패키지에는 `to_timestamp()`나 `to_date()` 같은 `to/from - date/timestamp` 이름의 함수들이 존재한다.
    - `to_timestamp()` : 기존 컬럼의 데이터 타입을 타임스탬프 타입으로 변환

```python
fire_ts_df = (new_fire_df
	.withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
	.drop("CallDate")
	.withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
	.drop("WatchDate")
	.withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
	.drop("AvailableDtTm"))

(fire_ts_df
	.select("IncidentDate", "OnWatchDate", "AvailableDtTS")
	.show(5, False))
```

- **실행결과**

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/e717dabc-45a5-4322-ac77-0bbba3ca32c1/Untitled.png)

- 타임스탬프 타입 컬럼에서 데이터 탐색을 할 때는 `spark.sql.functions`에서 `dayofmonth()`, `dayofyear()`, `dayofweek()`같은 함수들을 사용해 질의할 수 있다.

```python
(fire_ts_df
	.select(year('IncidentDate'))
	.distinct()
	.orderBy(year('IncidentDate'))
	.show()
)
```

- **실행결과**

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/2af906ec-7c7c-44ed-8e90-ebbbf4728121/Untitled.png)

### 집계연산

- `groupBy()` , `orderBy()` , `count()` 와 같이 데이터 프레임에서 쓰는 트랜스포메이션과 액션은 컬럼 이름으로 집계해서 각각 개수를 세어주는 기능 제공한다.

```python
(fire_ts_df
	.select("CallType")
	.where(col("CallType").isNotNull())
	.groupBy("CallType")
	.count()
	.orderBy("count", ascending=False)
	.show(n=10, truncate=False)
)
```

- **실행결과**

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/8e1fe20b-2d9f-4f8d-ad64-2dba29a14e8d/Untitled.png)


### 그 외 일반적인 데이터 프레임 연산들

- 앞선 연산들 외에도 데이터 프레임 API는 min(), max(), sum(), avg() 등 통계 함수들을 지원함
- ex) 경보 횟수의 합, 응답시간 평균, 모든 신고에 대한 최소/최장 응답시간 계산
    - 파이스파크 함수들을 파이썬 식으로 가져다 써도 내장 파이썬 함수들과는 충돌하지 않음!

```python
import pyspark.sql.functions as F
(fire_ts_df
.select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
.show())
```

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/c51407a9-85da-478b-8330-d2bec7426bc3/Untitled.png)

### 데이터세트 API

- 스파크 2.0에서는 개발자들이 한 종류의 API만 알면 되게 하기 위해, 데이터프레임과 데이터세트 API를 유사한 인터페이스를 갖도록 정형화 API로 일원화함
- 데이터세트는 정적타입(typed) API와 동적 타입(Untyped) API의 두 특성을 모두 가짐
    - 정적타입 API
        - Dataset[T] / 스칼라와 자바에서 사용 가능
    - 동적타입 API
        - DataFrame = Dataset[Row] / 스칼라에서 앨리어싱으로 사용
- 개념적으로 스칼라의 데이터프레임은 공용 객체의 모음인 Dataset[Row]의 다른 이름임
    - Row는 서로 다른 타입의 값을 저장할 수 있는 포괄적 JVM 객체
- 반면 데이터세트는 스칼라에서 엄격하게 타입이 정해진 JVM 객체이며, 자바에서는 클래스

### 정적 타입 객체, 동적 타입 객체, 포괄적인 Row

- 스파크가 지원하는 언어들에서 데이터세트는 자바와 스칼라에서 통용
    - 파이썬과 R에서는 데이터프레임만 사용 가능
    - 파이썬과 R이 컴파일 시 타입의 안전을 보장하는 언어가 아니기 때문
- 타입은 동적으로 추측되거나 컴파일할 때가 아닌, 실행 시에 정해짐
    - 타입은 변수와 객체에 컴파일 시점에 연결됨
    - 스칼라에서 DataFrame은 타입 제한이 없는 Dataset[Row]의 단순한 다른 이름일 뿐!
- Row는 스파크의 포괄적 객체 타입, 인덱스 사용 접근 가능, 다양한 타입의 값 가능
    - ex) Row 안에 있는 Int 타입은 스칼라나 자바와 파이썬에서 각각 적절하게 변환됨
        
        ```python
        from pyspark.sql import Row
        row = Row(350, True, "Learning Spark 2E", None)
        
        row[0]
        row[1]
        row[2]
        ```
        

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/71f98ef3-d2b8-4c62-9f68-eb5a5e582ac5/Untitled.png)



### 데이터세트 생성

- 대용량 데이터를 처리할때 스키마추론은 품이 많이 들기때문에 미리 해당 스키마를 알아야한다.
- 데이터세트 만들때 스키마를 저장하는 방법은 케이스클래스(case class)를 사용하는 것이다.

1. 스칼라’s 케이스 클래스 
    - 아래와 같이, JSON 엔트리를 특화객체(DeviceIoTData)로 만들기 위해 스칼라 케이스 클래스를 정의할 수 있다.
    
    [JSON 문자열]
    
    ```json
    {"device_id": 198164, "device_name": "sensor-pad-198164owomcJZ", "ip": "80.55.20.25 ,
    "cca2": "PL", "cca3": "POL", "cn": "Poland", "latitude": 53.080000, "longitude": 18.62000,
    "scale": "Celsius", "temp": 21, "'humidity": 65, "battery_level": 8, "c02_level": 1408, "Ic
    "red", "timestamp" :1458081226051}
    ```
    
    [케이스 클래스]
    
    ```scala
    case class DeviceIoTData (battery_level: Long, cO2_Level: Long, cca2: String, cca3: String, cn: String, device_id: Long, device_name: String, humidity: Long, ip: String, latitude:
    Double, lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)
    ```
    
    - 케이스 클래스를 정의한 후에는 파일을 읽어서 Dataset[Row] → Dataset[DeviceIoTData]로 바꾸는 데 사용 가능하다.
    
    ```scala
    //스칼라 예제
    val ds = spark.read
    	.json("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices-json")
    	.as[DeviceloTData]
    ds: org.apache.spark.sql.Dataset[DeviceIoTData] = [battery_level...]
    
    ds. show(5, false)
    ```
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/629219b1-3050-470d-925a-d48b50e083cd/Untitled.png)
    

### 데이터세트에서 가능한 작업

- 데이터 프레임에서 트랜스포메이션이나 액션들을 수행한 것처럼 데이터 세트에서도 수행할 수 있다.
- 데이터세트 사용예제

### 데이터 프레임 vs 데이터세트

- 스파크에서 **어떻게 하는지가 아니라 무엇을 해야 하는지** 말하고 싶으면 **데이터 프레임이나 데이터세트를 사용**한다.
- 컴파일 타임에 엄격한 타입 체크를 원하며 특정 Dataset[T]를 위해 여러 개의 케이스 클래스를 만들 의향이 있다면 데이터세트를 사용한다.
- SQL과 유사한 질의를 쓰는 관계형 변환이 필요하다면 데이터 프레임을 사용한다.
- 파이썬 사용자라면 데이터 프레임을 쓰되, 제어권을 좀 더 갖고 싶으면 RDD로 바꿔 사용한다.

### 언제 RDD를 사용하는가?

- 스파크 2.x나 3.x에서의 개발은 RDD보단 데이터 프레임에 집중되겠지만 추후 RDD에 대한 지원이 계속 이뤄질 것이다.
- 아래와 같은 시나리오에서는 RDD를 사용한다.
    - RDD를 사용하도록 작성된 서드파티 패키지 사용하는 경우
    - 데이터 프레임과 데이터세트에서 얻을 수 있는 코드 최적화, 효과적 공간 사용, 퍼포먼스 이득 포기 가능한 경우
    - 스파크가 **어떻게** 질의를 수행할지 정확히 지정주고 싶은 경우
- 데이터 프레임이나 데이터세트에서 RDD로 가고 싶을 경우, df.rdd만 호출하면된다. 다만 변환비용이 있으니 사용은 지양하길 바란다.


### 스파크 SQL과 하부 엔진

SQL 같은 질의를 수행하게 해주는 것 외에 스파크 SQL 엔진이 하는 일이 많다.

- 스파크 컴포넌트를 통합, 정형화 데이터 데이터 작업 단순화를 위한 추상화
- 아파치 하이브 메타스토어와 테이블에 접근
- 정형화된 파일 포맷에서 스키마와 정형화 데이터를 읽고, 쓰고 데이터를 임시 테이블로 변환
- 대화형 스파크 SQL 셸 제공
- 커넥터가 있어 표준 DB (JDBC/ODBC) 외부의 도구들과 연결
- 최적화된 질의 계획, 최적화된 코드 생성

이 외에 **카탈리스트 옵티마이저**와 **텅스텐 프로젝트**가 있는데 **텅스텐 프로젝트**는 6장에서 다룬다.

### **카탈리스트 옵티마이저**

연산 쿼리를 받아 실행 계획으로 변환한다.

sql, python, scala 등 사용한 언어에 관계 없이 작업은 동일한 과정으로 실행 계획과 실행을 위한 바이트 코드를 생성하게 된다.

2장의 m&m 예제를 활용해서 파이썬 코드가 바이트 코드로 되는 과정에 거치는 스테이지를 확인해보자.

```python
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())

    mnm_file = sys.argv[1]

    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file))
    mnm_df.show(n=5, truncate=False)

    count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .agg(count("Count")
                    .alias("Total"))
                    .orderBy("Total", ascending=False))

    count_mnm_df.explain(True)
    spark.stop()
```

위 explain(True) 함수가 각 스테이지마다 실행 계획을 출력해준다. 

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/7e7acac6-3dfb-48ac-8afc-df88fb3f2d32/Untitled.png)

**4단계 쿼리 최적화 과정을 더 자세히 살펴보자.**

1. 분석
    - 추상 문법 트리를 생성한다.
    - 데이터 프레임이나 데이터세트 이름 목록을 가져온다.
    - 미완성 논리 계획 수립한다.
    - 이를 스파크 SQL 프로그래밍 인터페이스 Catalog에 넘겨 논리 계획을 수립한다.
2. 논리적 최적화
    - 일단 최적화 접근 방식이 적용된다.
    - 여러 계획들을 수립하면서 **비용 기반 옵티마이저**를 써서 각 계획에 비용을 책정한다.
    - 이 계획들은 연산 트리들로 배열된다. (계획 예 - 조건절 하부 배치, 칼럼 걸러내기, 불리언 연산 단순화 등)
    - 이 계획은 물리 계획 수립의 입력 데이터가 된다
3. 물리 계획 수립
    - 스파크 실행 엔진에서 선택된 논리 계획을 바탕으로 대응되는 물리적 연산자를 사용해 최적화된 물리 계획을 생성한다.
4. 코드 생성
    - 각 머신에서 실행할 효율적인 자바 바이트 코드를 생성한다.
    - **포괄 코드 생성**이라는 것이 프로젝트 텅스텐 덕에 가능하게 됐다.
        - 전체 쿼리를 하나의 함수로 합치는 것, 가상 함수를 호출하거나 중간 데이터를 위한 CPU 레지스터 사용을 없애서 CPU 효율과 성능을 높였다.