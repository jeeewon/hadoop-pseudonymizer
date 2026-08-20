# BigProtector

Hadoop MapReduce 기반 대용량 정형 데이터 개인정보 가명처리 및 결합(Join) 시스템입니다.

## Features

- **가명처리 8종**: Delete, Masking, PartDelete, Round, Encryption(SHA-256), TopBottom, MicroAggregation, Random
- **전처리**: 가명처리에 필요한 통계값(평균/표준편차/최댓값/최솟값) 사전 계산
- **Join**: 두 데이터셋을 지정된 키로 결합(Inner Join)

## Requirements

- Java 7+
- Maven
- Hadoop 3.3.x 클러스터

## Build

```bash
mvn clean package
```

## Usage

```bash
hadoop jar demo3-1.0-SNAPSHOT.jar com.example.App <preprocessor|pseudonymize> <input> <config.json> <output>
hadoop jar demo3-1.0-SNAPSHOT.jar com.example.App join <inputA> <inputB> <config.json> <output>
```

## Config

컬럼별로 적용할 기능과 옵션을 JSON으로 지정합니다.

```json
{
  "주민등록번호": ["Masking", "1:0-:1"],
  "혈액형": ["Encryption"]
}
```

## Project Structure

```
App.java          커맨드 라우터 (preprocessor / pseudonymize / join)
Pseudonymize.java 전처리 및 가명처리 로직
Join.java         결합(Join) 로직
```

## Notes

- 2TB급 데이터, 다중 노드 Hadoop 클러스터 환경에서 시험 완료
