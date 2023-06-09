# Learning Spark
## chapter 8. 정형화 스트리밍

### 데이터 트랜스포메이션

정형화 스트리밍에서 지원하는 데이터 트랜스포메이션을 자세히 알아보자.

앞서 스트리밍 쿼리의 내부를 살펴본 내용은 다음과 같다.

- 스파크 SQL의 카탈리스트 옵티마이저는 모든 데이터 프레임 연산을 최적화하여 논리 계획으로 변환하는 작업을 한다.
- 스파크 SQL 플래너는 연속적인 데이터 스트림 위에서 논리 계획을 어떻게 실행할지 결정한다.
여기서 상황에 따라 논리 계획을 일회성 실행 계획으로 바꾸기도 하고 연속적 실행 계획 묶음을 생성하기도 한다.
- 실행 계획이 실행되어 결과 데이터 프레임을 계속 업데이트 한다.

여기서 각 실행 계획은 하나의 마이크로 배치가 되고, 중간 결과들은 스트리밍 ‘상태’가 된다.

누적 연산을 실행하기 위해 상태 정보가 필요한 경우가 있는데, 
상태 정보가 필요한지 여부에 따라 데이터 프레임 연산을 크게 ‘무상태’와 ‘상태정보유지’ 로 구분한다.

### 무상태 트랜스포메이션

모든 프로젝션 연산(select(), map() 등)과 선택적 연산(filter(), where() 등)은 이전 행에 대한 정보가 없어도 입력 레코드를 개별적으로 처리할 수 있다.

이러한 연산들로 이루어진 스트리밍 쿼리는 무상태 트랜스포메이션에 해당하며 앞서 이미 처리된 결과 레코드는 수정할 수 없기 때문에 추가 및 업데이트 출력 모드는 지원하지만 전체 모드는 지원하지 않는다. 

### 상태 정보 유지 트랜스포메이션

groupby(), count()과 같이 이전 행 정보가 필요한 연산의 스트리밍 쿼리가 여기에 해당된다. 
모든 마이크로 배치에서 누적 계획에 새로운 값과 이전 배치의 값을 더하여 기록한다. 
각 배치의 상태 정보는 스파크 이그제큐터의 메모리에 보관되어 장애복구를 위한 체크포인트 기록을 한다.

이그제큐터는 실행 계획의 실행 결과를 싱크에 기록하는 작업 뿐만 아니라 마이크로 배치 간 상태를 유지하기 위해 중간 데이터를 생성한다. 이 상태 데이터는 파티션되어 분산 처리 되고, 이그제큐터 메모리에 캐싱된다. 

예시로 단어 세기 스트리밍 쿼리에서 상태가 어떻게 관리되는지 살펴보겠다.

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/e541b99c-2ba6-4785-9f02-5b65590eb01c/Untitled.png)

1) 각 마이크로 배치는 새로운 단어를 읽어 들인다. 이그제큐터는 각 배치마다 새로 들어온 단어들을 포함하여 셔플 및 그룹화하고, 개수를 세어서 새로운 개수를 출력한다. 또한 다음 마이크로배치를 위해 상태 정보를 이그제큐터의 메모리에 캐싱한다.

2) 다음 마이크로 배치 데이터는 직전 마이크로 배치와 동일한 작업을 하고, 새로운 개수를 상태 정보의 기존  개수에 더해서 상태 정보를 업데이트한다.

여기서 더 나아가 장애 발생 시 메모리의 정보를 잃을 위험이 있기 때문에 각 배치에 변화된 변경 내용과 오프셋 범위로 체크포인트 로그를 남긴다. 

상태 정보 유지 + 체크포인트 로그를 통해 장애 발생 시에 해당 배치 이전의 동일한 상태 정보로 동일한 입력 데이터를 재처리하며 이는 정확한 일회 실행을 보장하기 위한 핵심적인 요소가 된다.


### 상태 정보 유지 연산의 종류들

스트리밍 상태 정보의 본질은 과거 데이터의 요약 정보를 유지하는 것이며, 때론 새로운 정보를 위해 과거 데이터는 정리할 필요도 있다. 

그래서 두 가지 타입의 상태 정보 유지 연산을 구분할 수 있다.

- 관리형 상태 정보 유지 연산
어떤 연산이 만료 인지 아닌지를 판단해 자동적으로 오래된 상태 정보를 감지하고 정리
    - 스트리밍 집계 연산
    - 스트림-스트림 조인
    - ㅁ
- 비관리형 상태 정보 유지 연산
직접 자신만의 상태 정리 로직을 정의 할 수 있게 해 준다.
아래의 연산들은 원하는대로 임의의 상태 정보 유지 연산을 정의하도록 도와준다.
    - MapGroupsWithState
    - FlatMapGroupsWithState

# 상태 정보 유지 스트리밍 집계

정형화 스트리밍은 대부분의 데이터 프레임 집계 연산을 누적시키면서 실행한다.

다양한 타입의 스트리밍 집계에 대한 배경과 연산, 스트리밍에서 지원하지 않는 집계 연산 형태들도 간략히 살펴본다.

## 시간과 연관 없는 집계

- 전체 집계
스트림으로 들어오는 모든 데이터를 통틀어 집계
스트리밍 데이터 프레임은 집계 결과를 지속적으로 업데이트 하기 때문에 DataFrame.count() 같은 직접적인 연산을 사용할 수 없어 groupBy() 같은 함수들을 사용해야한 다.
    
    ```python
    # In Python
    runningCount = sensorReadings.groupBy().count()
    ```
    
- 그룹화 집계
데이터 스트림에서 나타나는 각 그룹이나 키별로 집계한다.
각 센서별로 실행 중인 평균 값을 아래와 같이 계산 할 수 있다.
    
    ```python
    # In Python
    baselineValues = sensorReadings.groupBy("sensorId").mean("value")
    ```
    

합계나 평균 외에도 스트리밍 데이터 프레임에서는 아래와 같은 집계 연산들을 지원한다.

- 모든 내장 집계 함수들
sum(), mean(), stddev(), countDistinct(), collect_set(), approx_count_dis tinct() 등..
자세한 내용은 API문서 참고
- 함께 계산된 다중 집계연산
아래와 같이 동시에 계산 될 수 있도록 적용 가능
    
    ```python
    # In Python
    from pyspark.sql.functions import * 
    multipleAggs = (sensorReadings
              .groupBy("sensorId")
              .agg(count("*"), mean("value").alias("baselineValue"),
                collect_set("errorCode").alias("allErrorCodes")))
    ```
    
- 사용자 정의 집계 함수
모든 사용자 정의 집계 함수를 사용할 수 있다. 타입 지정/비지정은 가이드를 참고한다.

스트리밍 집계 연산 실행과 관련되어 알아봤으며 이제 시간에 의존하지 않는 집계 연산에 대해 알아볼 것이다.

## 이벤트 타임 윈도우에 의한 집계

시간 범위에 따라 구분된 데이터에 대한 집계가 필요할 수 있다. 

전송 딜레이 때문에 이벤트 타임(읽기가 일어났을 때를 표시하는 레코드 타임스탬프)으로 계산해야하며 5분인 걸로 가정해보자.

```python
# In Python
from pyspark.sql.functions import * 
(sensorReadings
      .groupBy("sensorId", window("eventTime", "5 minute"))
      .count())
```

window()함수가 동적으로 그룹화해 계산하는 칼럼같은 개념이며 5분 간격으로 표현할 수 있게 해준다.

순서는 다음과 같다.

1. eventTime값을 사용하여 5분 단위로 읽어들인 값을 계산한다.
2. 센서ID를 기준으로 그룹화 한다.
3. 그룹의 합계를 갱신한다.

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/d3d62262-06af-4775-93e2-cb6f9c6ecd1a/Untitled.png)

각각의 이벤트 타임을 기반으로 몇몇 센서의 입력값이 어떻게 5분 간격으로 연속해서 들어와 각 그룹에 배치되는지 보여준다.

두개의 시간선은 언제 처리되는지와 이벤트 데이터의 시간을 보여준다.



- 그림에서 보면 이벤트 타임 12:07인 이벤트가 실제로는 12:11 이벤트 이후에 도착하고 처리되었
다.
- 사실 시간 간격의 정의에 따라 각 이벤트는 여러 그룹에 할당될 수도 있다. 예를 들어 만약 10분
단위의 시간 간격을 5분씩 움직인다고 하면 다음과 같이 할 수 있다.
    
    ```python
    (sensorReadings
    	.groupBy("sensorId", window("eventTime", "10 minute", "s minute"))
    	.count())
    ```
    

- 위 쿼리에서 모든 이벤트는 두 개의 중첩되는 시간 간격에 그림 8-8처럼 할당

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/c0ba5831-3795-430c-986a-6c3c62b01484/Untitled.png)

- 각각의 **(<할당된 시간 간격>', 센서ID)** 페어는 유일하게 존재하며 동적으로 생성되는 그룹으로 계산된 합계가 담기는 위치를 결정

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/5f3db82d-8fb8-4cbb-872d-d9e056b29b67/Untitled.png)

- 예를 들어 [eventTime = 12:07, sensorId = id1]의 이벤트는 두 개의 시간 간격에 연결되고, 다음과 같은 두 개의 그룹. (12:00-12:10, id1)과 (12:05-12:15, id1)에 할당된다.

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/34d624df-f24a-4422-b34d-4a373d8c9fe8/Untitled.png)

- 5분 간격 트리거링에 의해 입력 레코드들이 처리된다고 가정할 때, 그림 8-9의 아래쪽 테이블들은 각 마이크로 배치에서 결과 테이블의 상태를 보여준다(즉 현재 합계)
- **자원 사용량 관점**에서 보면 무한하게 상태 사이즈가 증가하는 문제를 야기하게 된다.
- 현재 시간 간격과 연계된 새로운 그룹이 만들어지더라도 오래된 그룹들은 상태 메모리를 계속 점유하고 있게 되며 늦게 오는 데이터들이 갱신하기를 기다리고 있게 된다.
- 이를 해결하기 위해 워터마크를 지정할 수 있다.

### 늦게 도착하는 데이터를 위한 워터마크 다루기

- **워터마크** watermark
처리된 데이터에서 쿼리에 의해 검색된 최대 이벤트 시간보다 뒤처지는 이벤트 시간의 동적인 임계값
- **워터마크 지연** watermark delay (뒤처지는 간격)
늦게 도착하는 데이터를 엔진이 얼마나 오래 기다릴 수 있는지를 정의
- 지정 그룹에 대해 데이터가 더 이상 들어오지 않는 시점을 알게 된다면 엔진은 자동적으로 해당 그룹에 대한 처리를 종료하고, 상태 정보에서 삭제해버릴 수 있다. 
⇒ 이런 방식으로 엔진이 쿼리의 결과를 계산하고 유지해야하는 상태 정보의 양을 줄일 수 있다.
- 예를 들어 센서 데이터가 10분 이상 지연되지 않는다는 것을 알고 있다고 하면, 워터마크를 다음과 같이 세팅할 수 있다.
    
    ```python
    (sensorReadings
    	.withWatermark("eventTime", "10 minutes")
    	.groupBy("sensorId", window("eventTime", "10 minutes", "5 minutes"))
    	.mean("value"))
    ```
    

### 워터마크의 보장의 의미

- 워터마크가 제공해주는 **“보장”**의 정확한 의미
    - 10분짜리 워터마크: 엔진이 입력 데이터에 나타난 가장 최근 이벤트 시간과 비교해,
    10분 이내로 지연된 데이터는 절대 버리지 않는다는 것을 보장 → 오직 한 방향으로만 적용
    - 10분 이상 지연된 데이터는 반드시 삭제되는 것은 아님
    - 10분 이상 지연된 입력 레코드가 집계될지 아닐지는 레코드가 언제 도착하는지에 대한
    정확한 타이밍과 마이크로 배치 처리가 언제 시작되는지에 따름

### 지원되는 출력 모드들

- 시간과 상관없는 스트리밍 집계와는 달리, 타임 윈도우가 포함되는 집계는 3가지 출력 모드 가능
    - 해당 모드별로 상태 정보 초기화와 관련한 묵시적인 사항 존재

### 갱신 모드

- 모든 마이크로 배치가 집계 갱신된 부분의 열만 출력 → 모든 타입의 집계에 사용 가능
- 특히 타임 윈도우 집계에서 워터마킹은 상태 정보가 정기적으로 초기화되도록 해줌
    - 스트리밍 집계 쿼리 실행에 가장 유용
    - But, Parquet/ORC 파일 기반 포맷 같은 추가 전용 스트리밍 싱크에 집계를 출력하는 용도로는 사용 불가 → Delta Lake를 쓰면 가능함

### 전체 모드

- 모든 마이크로 배치는 모든 갱신된 집계를 변화가 있는지 없는지, 얼마나 오래되었는지에 상관 없이 출력함
- 모든 타입 집계에 사용가능하지만, 타임 윈도우 집계에 사용하는 것은 워터마크가 지정되어 있더라도 상태 정보가 초기화되지 않음
- 모든 집계를 출력하는 것은 모든 과거 상태 정보를 필요로 하므로, 워터마크가 지정되어 있더라도 집계 데이터가 계속 보존되어야 함
    - 상태 정보 크기와 메모리 사용량이 무한하게 증가할 수 있다는 여지 → 타임 윈도우에서 조심

### 추가 모드

- **워터마크를 쓰는 이벤트 타임 윈도우 집계에 대해서만** 사용 가능
    - 이전 출력 결과 변경을 허용하지 않음
    - 워터마크가 없는 집계라면, 나중에 어떤 데이터가 와서 갱신될지 알 수 없으므로 추가 모드에서 출력될 수 없음
    - 오직 워터마크가 활성화된 이벤트 타임 윈도우 집계에서만 어느 시점에 더이상 쿼리 집계가 갱신되지 않을지 알 수 있음
- 파일 같은 추가 전용 스트리밍 싱크에 집계 내용을 쓸 수 있으나, 워터마크 시간만큼 출력도 늦음
    - 쿼리는 이어지는 워터마크가 집계가 아직 완료되지 않은 해당 키의 시간 간격을 지나기까지 기다릴 필요가 있기 때문


### 스트림-정적 데이터 조인

데이터 스트림을 정적 데이터세트와 조인하는 경우

- 예시: 노출되는 광고 정보 데이터세트 Impression(정적)과 광고를 사용자들이 클릭했을 때 발생하는 이벤트 정보 데이터세트 Click(스트림)이 있다고 가정
- 두 종류의 데이터 프레임(정적인 데이터와 스트리밍 데이터)으로 데이터 표현
    
    ```python
    # 정적 데이터 프레임 [adId: String, impressionTime: Timestamp, ...]
    # 정적 데이터 소스에서 읽어 들인다.
    impressionsStatic = spark.read. ...
    
    # 스트리밍 데이터 프레임 [adId: String, clickTime: Timestamp, ...]
    # 스트리밍 소스에서 읽어 들인다.
    clickStream = spark.readStream. ...
    ```
    
- `adId` 칼럼으로 **내부 동등 조인** 적용
    
    ```python
    matched = clickStream.join(impressionsStatic, "adId")
    ```
    
    - 두 개의 정적 데이터 프레임을 조인할 때와 동일한 코드
    - 실행 시 클릭에 대한 모든 마이크로배치는 정적 테이블과 내부 조인되어 서로 맞는 이벤트의 스트림을 출력
- 그 외에 가능한 조인 형태
    - **왼쪽이 스트리밍** 데이터 프레임일 때의 **좌측 외부 조인**
    
    ```python
    matched = clickStream.join(impressionsStatic, "adId", "leftOuter")
    ```
    
    - **오른쪽이 스트리밍** 데이터 프레임일 때의 **우측 외부 조인**
    - 그 외의 외부 조인은 점진적으로 실행하기 어려워서 지원되지 않음

스트림-정적 조인에서 알아두어야 할 사항

- 스트림-정적 조인은 **무상태 연산**이므로 **워터마킹이 필요하지 않음**
- **정적 데이터 프레임**은 스트리밍 데이터의 마이크로 배치마다 조인되면서 **반복적으로 읽힘**
    - 따라서 처리 속도를 올리고 싶으면 **캐시**를 해야 함
- 정적 데이터 프레임이 정의된 데이터 소스에서의 데이터가 변경된 경우, **변경사항**이 스트리밍 쿼리에서 보일지는 **데이터 소스에서 지정된 정책**에 달려 있음
    - 정적 데이터 프레임이 파일에 정의되어 있다면 파일에 대한 변경(예: 추가)은 스트리밍 쿼리가 재시작되기 전까지 반영되지 않을 것

### 스트림-스트림 조인

두 개의 데이터 스트림 사이에서 지속적으로 조인을 수행하는 것

- 문제점: 어느 시점에서든 한쪽 데이터세트의 상태가 불완전 → 입력값 사이에서 매칭되는 것을 찾는 것이 더 어려움
    - 두 스트림에서 매칭되는 이벤트들이 어떤 순서로 올지, 얼마나 지연될지도 알 수 없음
- 정형화 스트리밍은 양쪽 스트리밍 상태로부터 **입력 데이터를 버퍼링**해 지연을 처리하고, 지속적으로 **새로운 데이터가 도착할 때마다 매칭되는지 체크**

선택적 워터마킹을 사용한 내부 조인

- impressions 데이터 프레임을 스트리밍 데이터로 재정의했다고 가정

```python
# 스트리밍 데이터 프레임 [adId: String, impressionTime: Timestamp, ...]
impressionsStatic = spark.readStream. ...

# 스트리밍 데이터 프레임 [adId: String, clickTime: Timestamp, ...]
clickStream = spark.readStream. ...
```

- 엔진은 모든 클릭과 임프레션 정보를 상태 정보에 버퍼링
 → 버퍼링된 임프레션과 매칭되는 클릭 정보를 받거나 그 반대의 경우에 매칭되는 임프레션과 클릭 정보 생성
- 내부 조인 시각화
    
    ![스크린샷 2023-05-21 오후 11.12.02.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/2bb76ca9-b761-49e9-946e-6baaa80f7fce/%E1%84%89%E1%85%B3%E1%84%8F%E1%85%B3%E1%84%85%E1%85%B5%E1%86%AB%E1%84%89%E1%85%A3%E1%86%BA_2023-05-21_%E1%84%8B%E1%85%A9%E1%84%92%E1%85%AE_11.12.02.png)
    
    - 파란 점: 마이크로 배치(점선으로 표현)에서 받게 된 임프레션과 클릭 이벤트의 시간
    - 각 이벤트는 기록된 시간과 실제로 받은 시간이 동일하다고 가정

스트림-스트림 조인에서 **유지되는 상태 정보를 제한**하기 위해서는 사용 패턴에 대해 다음 정보들을 알아두어야 함

- 각각 데이터 소스에서 **두 이벤트가 생성되는 시간 차이**가 최대 얼마나 되는가?
- 데이터 소스에서 처리 엔진까지 하나의 이벤트는 **최대 얼마나 지연**될 수 있는가?

- 지연한 및 이벤트 타임 제한들 표현 가능(데이터 프레임 연산 안에서 워터마크/ 시간 범위 조건 등으로) ⇒ 상태 정보 깔끔하게 유지하기 위한 절차
1. 양쪽 입력 엔진에 얼마나 지연된 입력을 허용할 수 있도록 워터마크 지연 정의
2. 두 입력 간에 이벤트 타임 제한 정의 (엔진 한쪽 입력의 오래된 레코드가 다른 쪽 입력에 언제 없어질지 알게함)
a. 시간 범위에 대한 조인 조건 
⇒ (”leftTime BETWEEN rightTime AND rightTime + INTERVAL 1 HOUR”)
b. 이벤트 타임 윈도우로 조인
⇒ (”leftTimeWindow = rightTimeWindow”)

- 광고 예제 내부 조인 코드
    
    1) 파이썬 예제
    
    # 워터마크 정의
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/c26988df-5a4b-4807-af4d-415412ba0b43/Untitled.png)
    
    # 시간 범위 조건으로 내부 조인
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/7470bf6d-7485-4388-aa35-066e35d2891c/Untitled.png)
    
    2) 스칼라 예제
    # 워터마크 정의
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/c9857036-7070-45df-b6e3-47c9becc3479/Untitled.png)
    
    # 시간 범위 조건으로 내부 조인 
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/1aaea349-bb95-4d16-a042-0ba90b78128a/Untitled.png)
    

- 각 이벤트 조건을 통해 자동적으로 체크하는 항목들
    
    1) 얼마나 이벤트가 오랫동안 버퍼링되어야 하는지
    2) 언제 이벤트를 상태 정보에서 제거해야 되는지
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/fed392f3-1eb2-4d1a-a8e8-bf9337267149/Untitled.png)
    
    ⇒ 정형화 스트리밍은 자동적으로 워터마크 지연과 시간 간격 조건을 써서 상태 정보가 정리되어야 하는 시점을 계산한다
    
    - 3시간 지연된 클릭 정보 4시간 전의 Impression과 매치될 수 있으므로 이벤트 타임 기준 최대 4시간 동안 Impression은 버퍼링 되어야 함
    (3시간의 전송 지연 + Impression과 클릭 사이에 허용되는 최대 4시간 지연)
    - 반대로 2시간 지연된 Impression이 2시간 전의 클릭과 매치될 수 있으므로 이벤트 타임 기준 최대 2시간 동안 클릭 정보 버퍼링되어야 함

- 내부 조인에 대한 키포인트

1) 내부 조인에서 워터마킹, 이벤트 타임 제한 지정은 선택 사항이다
(잠재적으로 연결되지 않은 상태 정보 존재에 대한 위험 감수하더라도 제한을 선택하지 않을 수 있다)

2) 엔진이 2시간 이하로 지연되는 데이터는 잃어버리지 않는다

### 워터마킹을 이용한 외부 조인

- 앞서 살펴본 내부 조인은 클릭 정보가 존재하지 않는 광고에 대해서는 전혀 알 수 없다
- 추후 분석을 위한 용도로 클릭 정보가 있든 없든 모든 광고 Impression 정보에 대해 받아 보기를 원할 수도 있음 ⇒ **스트림ㅡ스트림 외부 조인** 으로 해결
- 외부 조건 타입으로 지정
    
    #파이썬 예제
    
    # 시간 범위 조건으로 left Outer 조인
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/8699e8d4-b7db-4f18-aeb2-beb9dd155804/Untitled.png)
    
    # 스칼라 예제
    
    # 시간 범위 조건으로 left Outer 조인
    
    ![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/816266e0-ddcc-442a-9134-67a70b48364e/Untitled.png)
    
    # 유일한 변경 : 외부 조인 타입 지정
    
    ⇒ 모든 Impression에 대해 클리 정보가 있든 없든 결과를 출력
    

- 외부 조인 알아둘 사항

1) 외부 조인에서는 워터마크 지연이나 이벤트 타임 제한이 선택이 아닌 필수이다
: NULL을 결과로 출력하기 위해서 엔진이 어느 시점부터 이벤트가 매치되지 않을지 알아야 하기 떄문

2) 외부 조인의 NULL 결과 출력을 위해서 엔진은 매치되는 정보가 확실히 없다는 것을 보장하기 위해 대기할 필요가 있다

: 이 지연은 버퍼링 시간이며, 각 이벤트마다 엔진에 의해 계산된다

## 임의의 상태 정보 유지 연산

많은 사용 패턴들 ⇒ SQL 연산자/함수들보다 복잡한 논리 흐름을 필요함

Ex) 실시간 사용자 행동 추적하여 상태 추적하고 싶다고 가정

1) 임의의 자료구조로 상태 정보에 사용자 행동 기록 추적

2) 지속적으로 사용자의 행동에 기반한 자료 구조에 복잡한 로직을 적용해야 함

: mapGroupWithState(), flatMapGroupWithState() 위의 복잡한 분석 사례를 커버하기 위해 설계 됨

### mapGroupsWithState() 써서 임의의 상태 정보 유지 연산 모델링하기

: 임의의 스키마로 상태 정보 저장, 트랜스포메이션 적용 (앞 버전의 상태값과 새로운 데이터를 입력으로 받아들이고, 갱신된 상태 정보와 계산 결과를 출력하는 사용자 정의 함수로 모델링 할 수 있다)
⇒ 스칼라에서 시그니처에 맞게 함수를 정의해야(K,V,S,U) 한다

![Untitled](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/3a6398ac-9ab3-42a9-8217-1068a4873da3/Untitled.png)


스트리밍 쿼리가 시작되면 각 마이크로 배치에서 스파크는 arbitraryStateUpdateFunction()를 데이터의 각 개별 키마다 호출

### 파라미터

- key : K

상태 정보화 입력 데이터에 정의되는 공통 키의 데이터 타입

스파크는 데이터의 각 개별 키마다 이 함수를 호출

- newDataForKey : Iterator[V]

V는 입력 데이터 세트의 데이터 타입.

스파크가 키에 대해서 이 함수를 호출할 때, 이 파라미터는 키와 관련된 모든 입력 데이터를 갖게 됨.

이터레이터에서 가져오는 입력 데이터 객체의 순서는 정해져 있지 않음

- previousStateForKey : GroupState[S]

S는 개발자가 관리하게 될 임의의 상태 정보의 데이터 타입.

GroupState[S]는 상태 정보값들에 접근하고, 관리할 수 있는 함수들을 제공하는 타입을 갖는 포장 객체.

스파크가 키에 대해 함수를 호출할때 이 객체는 이전 스파크가 호출했던 상태값 집합을 제공

- U

함수의 출력값의 데이터 타입

### 프로그래밍 관점에서 다음 단계에 따라 상태 갱신 함수를 정의

1. 데이터 타입 정의

  K,V,S,U에 대한 정확한 타입 정의

- 입력데이터(V) = case class UserAction(userId: String, action: String)
- 키(K) = String *userId
- 상태(S) = case class UserState(userId:String, active =Boolean)
- 결과(U) = UserStatus, 최신 사용자 상태 정보를 담는 클래스

2. 함수 정의

- 새로운 함수가 사용자 행동과 동시에 호출될 때 대응해야 할 두 가지의 주요 상황
- 키에 해당하는 이전 상태 정보가 존재하는지 아닌지
- 어느 쪽인지에 따라 적절하게 새로운 행동에 대한 정보와 함께 현재 상태를 갱신해주거나, 아니면 사용자 상태 정보를 초기화
- 명시적으로 상태를 새로운 집계와 함께 갱신해주고, 최종적으로는 갱신된 userId-userStatus 쌍을 되돌려줌

```scala
import org.apache.saprk.sql.streaming._

def updateUserStatus(

userId: String,

newActions: Iterator[UserAction],

state: GroupState[UserStatus]): UserStatus = {

 val userStatus = state.getOption.getOrElse {

   new UserState(userId, false)

}

 newActions.foreach { action =>

   userStatus.updateWith(Action)

}

 state.update(userStatus)

 userStatus

}
```

3. 사용자 액션에 함수를 적요. groupByKey()를 써서 입력 액션들의 데이터세트를 그룹화하고, mapGroupWithStatus()를 사용하는 updateUserStatus함수를 적용

```scala
val userActions: Dataset[UserAction] = …
val latestStatuses = userActions
     .groupByKey(userAction => userAction.userId)
     .mapGroupWithState(updateUserStatus _)
```


### 비활성화 그룹을 관리하기 위한 타임아웃 사용

- 활성 사용자 세션 추적 예제에서는 사용자가 수가 증가할수록 관리해야 할 상태 정보의 키 숫자가 늘어날 뿐만 아니라 상태 정보 유지를 위한 메모리 사용량도 늘어날 수 있다.
- 실제로 사용자들은 언제나 활성(active) 상태로 남아 있지 않기 때문에 그 정보를 계속 유지하는 것은 도움이 되지 않을 수 있다.
- 따라서 비활성화 사용자들에 대한 정보를 명시적으로 제거하는 것이 필요할 수 있지만, 사용자가 어떤 상태에서 비활성화 인지는 알 수 없다.
- 시간 기반 비활성화를 인코딩하기 위해 `mapGroupsWithState()` 는 다음처럼 정의되는 타음아웃 개념을 지원한다.
    1. 매번 함수는 하나의 키에 대해 호출되며 타임아웃은 하나의 키에 대해 시간 기반이나 임계값의 타임스탬프를 기반으로 설정할 수 있다.
    2. 만약 키에 대한 데이터가 전혀 들어오지 않고 해당 타임아웃 조건을 만족한다면 키는 ‘타임 아웃됨’으로 표시된다. 
        1. 다음 마이크로 배치는 이 키에 대한 데이터가 전혀 없더라도 이 타임아웃 키에 대해 함수를 호출할 수 있다. 
- 타임 아웃에는 처리 시간과 이벤트 타임에 따른 두 가지 종류가 있다.

### 처리 시간 타임아웃

- 처리 시간 타임아웃은 스트리밍 쿼리가 도는 머신에서의 시스템 시간 기준으로 한다.
    - 한 키에 대해 마지막으로 데이터를 시스템 시간 T에 받았으며 현재 시각이 `(T + <타임아웃 시간>)` 보다 크다면 새로운 빈 데이터 이터레이터와 함께 함수가 호출될 것이다.


**이벤트 타임 타임아웃**

이벤트 타임아웃은 데이터에 있는 이벤트 타임과 이벤트 타임에 저장된 워터마크 이용.

만약 한 키에 특정 타임아웃 타임스탬프T가 설정되어 있고, 그 키에 대한 데이터가 함수가 호출된 이후 새로운 데이터가 없는 채로 워터마크가 T를 초과 한다면, 키는 타임아웃 처리

**워터마크** 

데이터가 처리되는 동안 가장 최신 이벤트 타임 이후의 지연된 시간을 의미하는 동적인 임계값.

타임아웃 시점은 현재 워터마크보다는 큰 값으로 지정되어야 함. 

**flatMapGroupsWithState()를 사용한 일반화**

매번 mapGroupWithState()가 호출될 때 오직 하나의 레코드를 되돌려주어야 함.

mapGroupWithState()로는 불투명한 사앹 정보 업데이트 함수에 대한 정보 부재로 엔진은 생성된 레코드들이 갱신된 키/값 데이터 쌍이라고 가정. 그에따라 다운스트림 연산에 대해 판단하고 어떤 것을 허용할지, 하지 않을지 결정.

→flatMapGroupsWithStates는 조금 더 문법이 복잡하지만 위의 제한들을 극복함. mapGroupsWithstate와 두가지 차이점이 있다.

1. 리턴타입이 단일 객체가 아니라 이터레이터. 함수가 레코드 개수와 관계없이 리턴할 수 있게 하며, 심지어 아무것도 리턴하지 않는 것도 가능.
2. 연산자 출력 모드라고 불리는 또 다른 파라미터를 받아들이는데, 이는 출력 레코드가 추가될 수 있는 새로운 레코드인지 갱신된 키/값 레코드 인지를 정의한다.


### 성능튜닝

정형화 스트리밍은 Spark SQL 엔진을 사용하므로 5,7장에서와 동일한 파라미터들로 튜닝할 수 있다. 그러니 “배치 잡”과 달리 적은 데이터양을 처리하는 “마이크로 배치 잡”이므로 몇 가지 고민해봐야할 포인트들이 있다.

1. **클러스터 자원 배치**
    1. 24/7로 클러스터들이 돌아가기 때문에 적절한 자원배치가 중요함. 자원이 부족하게 배치되면 쿼리지연이 일어날 수 있고, 과하게 배치하면 쓸모없는 비용이 발생한다. 
    2. 스트리밍 쿼리의 특성에 맞는 자원배치해야한다. **무상태 쿼리(이전 쿼리 결과에 의존하지 않는 쿼리)에는 더 많은 코어**가 필요하고, **상태 정보 유지 쿼리(이전 쿼리 결과에 의존하는 쿼리)는 메모리를 더 필요**로 한다.
    
    ** 24/7 로 클러스터가 돌아간다는 의미는 하루 24시간, 일주일 7일 즉, 연중무휴로 운영된다는 의미
    
    - 스트리밍 쿼리에서 자원 배치하는 순서
        1. **쿼리의 유형을 식별**합니다. 쿼리가 무상태인지 상태 정보 유지인지 확인합니다.
        2. **쿼리의 특성을 식별**합니다. 쿼리에 어떤 데이터가 필요하고 얼마나 많은 데이터가 필요합니까? 쿼리는 얼마나 자주 실행됩니까?
        3. 쿼리에 **필요한 리소스를 결정**합니다. 쿼리에 얼마나 많은 코어와 메모리가 필요합니까?
        4. **리소스를 쿼리에 할당**합니다. 스파크 클러스터에 리소스를 할당합니다.
        5. 쿼리를 실행하고 **성능을 모니터링**합니다. 쿼리의 성능을 모니터링하고 필요에 따라 리소스를 조정합니다.
2. **셔플을 위한 파티션 숫자**
    1. 정형화 스트리밍 쿼리에선 대개 배치 쿼리보다는 셔플 파티션 갯수를 적게한다. 그러나 너무 잘개 쪼개면 오히려 오버헤드를 일으키고 처리량을 감소 시킬 수 있다.  (상태 정보 유지 연산에 의한 셔플은 체크포인팅때문에 더 큰 오버헤드 발생)
    2. 만약 **상태 정보 유지 연산 + second/minute 단위 트리거 간격의 스트리밍 쿼리**라면 셔플 파티션 숫자를 **기본(200) 대비 2-3배로 수정**하는 걸 권한다.
3. **안정성을 위한 소스의 처리량 제한**
    1. 자원 할당과 설정이 쿼리에 최적화된 후라도, 갑작스레 데이터 처리량 증가로 클러스터가 불안정해질 수 있다. 이때, 소스 전송률 제한으로 불안정성에 대한 방어가 가능하다. (자원 과다 할당도 하나의 방법이지만 비용이 많이 듦)
    2. 다만 너무 제한을 낮게 설정하면 쿼리가 할당 자원을 제대로 쓰지 못할 수 있고, 입력 비율이 계속해서 올라갈 경우 효과적으로 처리하지 못할 수 있다. 소스의 안정성이 유지되더라도, 처리되지 않은 데이터는 소스에서 무한하게 증가하여 응답속도를 느리게할 수 있다. 그러므로, **소스 전송률 제한이 소스에서 생성되는 데이터 속도를 초과하지 않도록 설정해야함. (데이터 생성 속도 제한 or 더 작은 chunk로 분할)**
4. **동일 스파크 애플리케이션에서 다중 스트리밍 쿼리 실행**
    1. 동일한 SparkContext , SparkSession 내에서 여러개의 스트리밍 쿼리를 실행하는 것은 자원을 세분화하여 공유할 수 있다. 다만 아래 2가지를 참고해야한다.
        1. 각 쿼리를 지속적으로 실행하는 것은 스파크 드라이버 자원(JVM 돌아가는 곳)을 사용하게 되는데, 이는 동시 실행 쿼리수를 제한한다. 만약 초과한다면 **테스크 스케줄링이 병목(executor  활용 x)되거나 메모리 제한을 넘게될 수** 있다.
        2. 각 쿼리 별로 별도의 스케줄러 풀에서 돌도록 설정하여 동일한 컨텍스트 내에서 쿼리 사이에 안정적 자원 할당이 이뤄지도록 한다. 
            - SparkContext의 스레드 로컬 속성인 spark.scheduler.pool에 서로 다른 문자열 값으로 각 스트림마다 지정해준다.