## BattleGround Open API활용 웹페이지 개발 

🔗 https://github.com/nbac406/battleGround

### 프로젝트 개요
- - - 
- 프로젝트 주제 : 배틀그라운드 게임 Open API활용 유저 정보제공 및 전적 정보 제공 기능 홈페이지 제작
- 프로젝트 기간 : 2023.07~2023.08
- 기획의도
  - 유저 전적 정보 및 플레이 스타일 정보 제공
  - 유저의 실제 플레이를 바탕으로 한 무기관련 정보 제공
  - 유저의 착륙지점 및 이동경로 정보 제공

### 기술스택 및 아키텍처 구상도
- - -
![image](https://github.com/nbac406/battleGround/assets/125121623/cb9db150-7b87-4e82-91bd-4d86607a7d1b)
<div align="center">
<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=Python&logoColor=white">
<img src="https://img.shields.io/badge/Django-092E20?style=for-the-badge&logo=django&logoColor=white">
<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"> </br>
<img src="https://img.shields.io/badge/GCP-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white">
<img src="https://img.shields.io/badge/MySQL-4479A1?style=for-the-badge&logo=MySQL&logoColor=white">
<img src="https://img.shields.io/badge/ubuntu-E95420?style=for-the-badge&logo=ubuntu&logoColor=white">  </br>
<img src="https://img.shields.io/badge/apachehadoop-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=white">
<img src="https://img.shields.io/badge/apachespark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"> 
<img src="https://img.shields.io/badge/nginx-2C2255?style=for-the-badge&logo=nginx&logoColor=white"> 
<img src="https://img.shields.io/badge/gunicorn-499848?style=for-the-badge&logo=gunicorn&logoColor=white"> 
</div>


### ERD
![image](https://github.com/nbac406/battleGround/assets/125121623/420abaf5-2b25-43e2-880a-bc52c758615d)


### 웹 주요 기능 구상도
- - -
![image](https://github.com/nbac406/battleGround/assets/125121623/bb68a581-e1ea-4edc-87d4-61a08f9fad0c)


### 웹 주요 기능 
- - -
<details>
<summary>메인페이지 및 유저 검색 기능</summary>
  
- 유저 검색 기능
- 서버 변경 기능 (KAKAO & STEAM)
- 무기 분석/맵 분석 페이지 이동
- 커뮤니티 워드 클라우드
![image](https://github.com/nbac406/battleGround/assets/125121623/77fbbf80-a021-444f-9f7a-06ed0ab77296)
</details>

<details>
<summary>플레이어 전적 조회</summary>
  
- 검색 유저 닉네임, 선택서버
- 전적 갱신 (Open API 호출)
- 솔로/듀오/스쿼드별 경기 요약
- 유저 무기레벨 top3표시
- 유저 플레이스타일 표시(Kmeans 클러스터링)
- 최근 30일 유저 match이력
![image](https://github.com/nbac406/battleGround/assets/125121623/0b576df5-8f3b-469c-a711-cd218971e29a)
</details>

<details>
<summary>플레이어 전적 조회 - 맵로그</summary>
  
- JavaScripts Leaflet 라이브러리 활용 배틀그라운드 맵 구현
- 유저의 킬 & 데스 로그 (이동경로 및 킬로그 확인)

![image](https://github.com/nbac406/battleGround/assets/125121623/bb6aa045-63b6-4bb2-97de-395c5539cf55)
![image](https://github.com/nbac406/battleGround/assets/125121623/c96ff23a-6c36-4672-a837-c0a58a9b7200)
</details>

<details>
<summary>무기분석</summary>
  
- 무기티어 표 기능 
![image](https://github.com/nbac406/battleGround/assets/125121623/1d8ad941-7738-425c-84fd-36fa573158ca)
</details>


<details>
<summary>무기 상세분석</summary>
  
- 1주간의 매치데이터 기반 상대하기 쉬운 무기 top3 & 어려운 무기 top3 표시
- 게임 플레이 내 무기별 가장 많이 착용된 parts정보 제공
- 1주간 매치데이터를 기반으로 무기별 주로 kill 이 발생하는 거리를 측정하고 단위별로 나누어 그래프로 시각화
![image](https://github.com/nbac406/battleGround/assets/125121623/5ffd8153-89ad-4a21-89bc-9bcd25be9988)
![image (1)](https://github.com/nbac406/battleGround/assets/125121623/ddf963e4-607e-400f-977a-fd4a21c6fc2f)
![image (2)](https://github.com/nbac406/battleGround/assets/125121623/64f85804-9300-4a7e-9ec1-17dca213ee0f)
</details>


<details>
<summary>맵 분석</summary>
  
- 배틀그라운드의 총 6개의 맵에 대해 비행기 시작지점과 종착지점을 설정하여 비행기 경로별 유저들의 시작지점 시각화
![image](https://github.com/nbac406/battleGround/assets/125121623/cfd7ab53-ff4a-464b-bb75-6be807caf609)
</details>


### 우리팀 코드 변수명 및 DB 컬럼명 규칙
- - -
- 대문자는 넣지 않는다
- 단어와 단어 사이에는 언더바( _ ) 를 넣는다. [ ex -> matchid (X) / match_id (O) ]
- 변수명은 어느 역할의 변수명인지 확실히 한다. [ ex -> response (X) / match_data_response (O) ], [ url1 (X) / match_api_url (O) ]
- 복수형과 과거형을 구분짓는다. [ex -> 플레이어'들'의 정보를 저장 하는 DB 테이블명 = players ] , [ 생성된 날짜를 저장하는 테이블 컬럼명 = created_at ] , [ 매치 id 를 저장할 변수 = match_id / 매치 id '들'을 저장할 리스트 타입 변수 = match_ids ]
