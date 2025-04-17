# 음식 배달 앱 로그 데이터 분석 프로젝트

## 목차

- [프로젝트 개요](#프로젝트-개요)
- [비즈니스 케이스](#비즈니스-케이스)
- [분석 프로세스](#분석-프로세스)
    - [1. 데이터 정제](#1-데이터-정제)
    - [2. 문제 정의](#2-문제-정의)
    - [3. 실험 설계 및 검증](#3-실험-설계-및-검증)
- [분석 쿼리 및 코드](#분석-쿼리-및-코드)

<br/>

## 프로젝트 개요

- **분석 기간:** 2025.03~2025.04 (1.5개월)  
- **기술 스택:** BigQuery, Python (Seaborn, Plotly)
- **목적:** 음식 배달 앱 로그 데이터를 가지고 문제 정의 및 가설 검증을 통해 주문율 개선 전략 도출
- **원천 데이터:** [가상의 앱 로그 데이터 (73만 건)](https://bit.ly/inflearn_bigquery_advanced)
- **작업 대상:** 2022년 8월~2023년 1월 (약 6개월)

<br/>

## 비즈니스 케이스

Foodie Express는 가상의 음식 배달 앱 서비스 입니다. 런칭 후 6개월 동안 약 73만 건의 로그가 쌓였고, 서비스의 현황을 점검하고 앞으로의 개선 방향을 찾기 위한 분석이 필요했습니다. 이런 상황 아래, 다음과 같은 순서로 분석을 진행했습니다.

<br/>

## 분석 프로세스

### 1. 데이터 정제 

- **동적 PIVOT 쿼리**로 이벤트별 상이한 파라미터 구조를 일괄 처리해 쿼리 유연성 확보  
  → [전처리 (프로시저)](./1_preprocess.sql)
- **회원/비회원 구분:** 비회원(5.95%)은 홈 진입 후 이탈 패턴만 보여 분석 대상에서 제외 
  → [회원/비회원 구분 (프로시저)](./2_analysis_target.sql)

### 2. 문제 정의
- [EDA 쿼리](./4_eda.sql)
- **유저 방문 패턴:** 일회성/간헐적 방문이 다수 (방문 주기 중위수 27일, WAU 고착도 14~15%)
- **주문율 이슈:** 연휴 기간 주문율이 평소 대비 약 3.5배(연휴 35%, 비연휴 10%)로 급증
- **분석 목표:** 주문율 개선

### 3. 실험 설계 및 검증

#### 3-1. 유저 세분화  
- **방문 주기 기준 4개 그룹:**  
    - 일회성(26.5%)  
    - 단기 재방문(14일 이내, 19.4%)  
    - 중기 재방문(15~30일, 17.1%)  
    - 장기 재방문(30일 초과, 36.9%)  
  → [방문 주기 기반 유저 세분화](./5_user_seg_visit_interval.sql)

#### 3-2. 주문 퍼널 정의  
- **유입 경로:** 카테고리, 추천 메뉴, 근처 식당, 검색, 배너 등 5가지  
- **퍼널 단계:** 방문 → 탐색 → 장바구니 → 결제 (Open Funnel, 각 단계 잔존율 분석)  
  → [주문 퍼널](./7_order_funnel.sql)

#### 3-3. 리텐션 및 추가 분석  
- **유저별 연휴/비연휴 주문 분석:** [유저별 비연휴/연휴 주문 횟수 계산](./6_user_seg_order_holiday.sql)
- **방문 주기별 리텐션:** [방문 주기 기반 유저별 리텐션](./8_retention_visit_inteval.sql)
- **재방문 유저 내 주문유무 리텐션:** [재방문 유저 내 주문유무 리텐션](./9_retention_revisit_order.sql)
- **코호트 리텐션:** [방문 주기 기반 코호트 리텐션](./10_cohort_retention_visit_interval.sql)
- **EDA 및 시각화:** [EDA](./4_eda.sql), [시각화 및 통계적 가설 검증](./foodie_viz.ipynb)

---

## 분석 쿼리 및 코드

| 파트 | 설명 | 소스코드 |
|---|---|---|
| 데이터 전처리 | 이벤트 파라미터 통합 처리 | [1_preprocess.sql](./1_preprocess.sql) |
| 분석 대상 선정 | 회원/비회원 구분 | [2_analysis_target.sql](./2_analysis_target.sql) |
| 분석 테이블 정보 | 테이블 스키마 등 | [3_info.sql](./3_info.sql) |
| EDA | 기초 통계 및 분포 분석 | [4_eda.sql](./4_eda.sql) |
| 유저 세분화 | 방문 주기 기반 그룹핑 | [5_user_seg_visit_interval.sql](./5_user_seg_visit_interval.sql) |
| 연휴/비연휴 분석 | 유저별 주문 패턴 | [6_user_seg_order_holiday.sql](./6_user_seg_order_holiday.sql) |
| 주문 퍼널 | 유입경로별 퍼널 분석 | [7_order_funnel.sql](./7_order_funnel.sql) |
| 리텐션 분석 | 방문 주기별 리텐션 | [8_retention_visit_inteval.sql](./8_retention_visit_inteval.sql) |
| 재방문 리텐션 | 재방문 내 주문유무 | [9_retention_revisit_order.sql](./9_retention_revisit_order.sql) |
| 코호트 리텐션 | 방문 주기 기반 코호트 분석 | [10_cohort_retention_visit_interval.sql](./10_cohort_retention_visit_interval.sql) |
| 시각화/통계 | EDA, 가설 검증, 시각화 | [foodie_viz.ipynb](./foodie_viz.ipynb) |
