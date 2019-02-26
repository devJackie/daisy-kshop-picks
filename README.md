# daisy-kshop-picks
1. model의 추첨 점수가 1 이상인 경우에 대한 설명
    + https://stackoverflow.com/questions/46904078/spark-als-recommendation-system-have-value-prediction-greater-than-1
    1 이상인 경우 사용자가 좋아할 것이다, 관심있을 것이다라고 생각할 수 있음 
    
2. setCheckpointDir 를 사용해야 하는 이유 ()java StackOverflowError 시 체크 포인트 설정해야한다)
```spark.sparkContext.setCheckpointDir("hdfs://daisydp/tmp/p_yymmdd=" + p_yymmdd)```
    + 해결책은 체크 포인트를 추가하여 코드베이스에서 사용 된 재귀가 오버플로를 만드는 것을 방지하는 것    
    + https://stackoverflow.com/questions/31484460/spark-gives-a-stackoverflowerror-when-training-using-als
    + Optional, but may help avoid errors due to long lineage
    + https://issues.apache.org/jira/browse/SPARK-1006
        + ALS는 너무 긴 계보 체인 (집행자의 deserialization 원인 stackoverflow)으로 StackOverflow 발생, 
        우리는 계보 체인을 깰 수 없기 때문에 DAGScheduler를 재귀적으로 변경하여 이 문제를 해결할수 확실하지 않음. 하지만 그것은 잠재적인 stackoverflow를 드라이버에서 피할 것이다. 
        (이 예외는 반복적으로 단계를 만들 때 발생했다)