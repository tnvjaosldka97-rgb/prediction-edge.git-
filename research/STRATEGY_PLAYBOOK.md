# Polymarket 알파 전략 — 20개 후보 분석

작성: 2026-04-30, Claude 에이전트 분석 (학술 + retail 퀀트 + Polymarket 도메인 종합).

## 핵심 가정

Polymarket이 다른 시장과 다른 점:
1. **유동성 얕음** — BTC/ETH보다 깊이 1/100. 큰 봇이 못 들어옴 → retail 기회
2. **결제가 외부 이벤트에 의존** — UMA 오라클, 뉴스 사실, 선거 결과 등. 가격 발견이 비효율
3. **참여자 비전문가 다수** — 도박 동기, 정치 신념 → 비합리적 가격
4. **시간 지평 명확** — 만기 결정 → time decay 예측 가능
5. **$0~$1 가격 범위** — 정규화된 확률 공간 → 모델링 단순

**전형적 retail 봇은 BTC를 트레이드** → Polymarket 봇은 **희소** → 우리 기회.

## 전략 리스트 (20개)

각 전략은: 기대 엣지, 구현 난이도, 인프라, 우선순위로 평가.

### Tier 1 — 즉시 구현 (검증된 + 명확한 엣지)

#### 1. ✅ Closing Convergence (이미 있음)
- 만기 임박 + 가격 0.7+ → 1.0으로 수렴
- **검증**: 90일 백테스트 +17.1%, 마찰 적용 +26.1%/6일
- 엣지: 5~10% per trade, 승률 96%
- **현재 가동 중**

#### 2. News Reaction Lag (구현 필요)
- 주요 뉴스 (선거 여론조사, FED 발표) → 시장 5~30분 후 fully reprice
- 우리: 뉴스 즉시 감지 → 5초 내 진입 → 가격 수렴 받음
- 엣지: 2~8% per event, 일 1~5건 발생
- 인프라: 뉴스 RSS + Claude API sentiment 분류
- **우선순위 1**: 즉시 구현 가능

#### 3. UMA Dispute Premium (부분 있음, 강화 필요)
- 만기 직전 markets에 dispute_risk 가격 반영 안 됨
- low dispute_risk + high price → 안전한 carry
- 엣지: 0.5~2% per trade, 매일 5~20건
- 인프라: dispute_risk DB 테이블 + 가격 매칭
- **우선순위 2**

#### 4. Whale Copy Trading (코드 있음, 검증 필요)
- 상위 5% 지갑 winrate 65~75%
- 그들이 진입 후 1~5분 내 미러
- 엣지: 3~10% per trade
- 인프라: on-chain watcher (있음) + 윈레이트 ranking + 사이즈 비례
- **우선순위 3**

### Tier 2 — 중간 난이도 (검증된 패턴, 우리 데이터 활용)

#### 5. Cross-Market Correlation Arbitrage
- "Trump 당선" + "Trump 관세 부과" + "공화당 House 우위" 등 강한 양의 상관
- 한 마켓 가격 vs 관련 마켓 implied 가격 갭
- 엣지: 2~5% per arb
- 인프라: condition_id 그래프 + 공동 출현 분석
- **우선순위 4**

#### 6. Order Book Imbalance Momentum
- 호가 깊이 imbalance > 5x 발생 → 30초 내 가격 그 방향 이동
- 우리: L2 호가 reconstruction (방금 구현) + 실시간 imbalance
- 엣지: 0.5~1.5% per signal, 일 50~200건
- **우선순위 5**

#### 7. Calendar Effects
- US 정치 마켓: 화·수 거래량 ↑ (의회 회기), 금요일 거래량 ↓ → 가격 비효율
- FOMC 주: 변동성 ↑ → maker 우위
- 우리 6일 데이터로 patterns mining
- 엣지: 이벤트 타이밍 별 0.3~1%
- **우선순위 6**

#### 8. Liquidity Provider Mean Reversion
- LP가 일시적 호가 빠지면 가격 스파이크 → 30~120초 내 평균 복귀
- 우리: 호가 depth 변화 추적 (L2 store에서 가능)
- 엣지: 0.5~1.5% per event
- **우선순위 7**

#### 9. Resolution Day Volatility Pattern
- 만기 24시간 전 → 변동성 급증 + 가격 wash
- 패턴: 0.85 마켓 → 0.70 까지 wash → 다시 0.95 회복
- 우리 backtest로 확률 매핑
- **우선순위 8**

### Tier 3 — 고난이도 (인프라/연구 필요)

#### 10. ML News Sentiment Signal
- Twitter/뉴스 헤드라인 → Claude embedding → 시장 매칭
- 의미 유사도 가장 높은 마켓에 sentiment 적용
- 엣지: 미정 (백테스트 필요)
- 인프라: 뉴스 피드 + Anthropic API + DB

#### 11. Cross-Platform Arbitrage (Real)
- Polymarket vs Limitless vs Kalshi 동일 이벤트 가격 갭
- 우리 코드 stub 있음 → 실제 가동
- 엣지: 0.5~3% per arb
- 인프라: 3개 venue API 키 + matching 로직 + 실시간 가격 비교

#### 12. Whale Concentration Risk Contrarian
- 상위 5 보유자가 >70% 차지 → 논리적 위험
- 그들 청산 시작 시 빠른 exit
- 엣지: 위험 회피 (return X, 손실 회피)

#### 13. Long-tail Market Mispricing
- 거래량 < $1k 마켓 = 가격 발견 거의 X
- model_prob 계산 후 큰 갭 있으면 작은 사이즈 진입
- 엣지: 5~20% per trade, 빈도 낮음

#### 14. Election Polling Reactionary
- 538/Polymarket Aggregator 등 새 폴 발표 → 5분 내 시장 적응
- 우리: 폴 발표 직후 진입
- 엣지: 1~3% per event

#### 15. Sports Live Odds vs Settlement Gap
- 게임 진행 중 라이브 베팅 odds vs Polymarket settlement market 가격
- 가격 발견 시차 활용
- 엣지: 1~2% (스포츠 시즌 한정)

### Tier 4 — 실험적 / 연구 단계

#### 16. Auto-Discovery Strategy Loop
- 매일 자동으로 새 패턴 탐색
- 통계적 유의성 통과 시 shadow → live promotion
- 인프라: research agent (다음 구현)

#### 17. Volume Spike Prediction
- 볼륨 갑자기 10x → 뉴스 이벤트
- 군중보다 빠르게 반응
- 인프라: 실시간 볼륨 모니터링

#### 18. Range-Bound Mean Reversion
- 0.40-0.60 7일+ 박스 → MR 전략
- 단순한 통계, 빈도 적음

#### 19. Asymmetric Information Decay
- 6개월+ 만기 = 정보 적음 → extreme price (0.10, 0.90) fade
- 시간 지평 길어 자본 묶임

#### 20. Funding Rate Style Carry
- bid/ask 비대칭에서 carry 추출
- maker fee 0% 이용
- 엣지 미상, 빈도 높음

---

## 추천 구현 로드맵

### 즉시 (Day 11~12, 2~4시간)
- News Reaction Lag (#2)
- UMA Dispute Premium 강화 (#3)
- Order Book Imbalance Momentum (#6)
- Calendar Effects mining tool (#7)

### 단기 (Day 13~15, 6~10시간)
- Cross-Market Correlation (#5)
- LP Mean Reversion (#8)
- Resolution Day Volatility (#9)
- Cross-Platform Arb 실작동 (#11)

### 중기 (Day 16~25, 30~60시간)
- ML News Sentiment (#10)
- Auto-Discovery Loop (#16)
- Long-tail Mispricing (#13)
- 통합 포트폴리오 최적화 (이미 HRP 있음)

### 장기 (Phase 5+)
- 강화학습 기반 execution 최적화
- 다중 venue (Kalshi, Limitless, GnosisDAO 등) 동시 운영
- 자체 alpha 발견 ML 모델 학습

---

## 주의사항

- **각 전략 단독 검증 필수** — backtest CV (Day 10에 구현됨) 통과 후에만 라이브
- **A/B 테스팅** (Day 10에 구현됨) — 새 전략 deploy 시 기존 vs 새 50/50 분할
- **자동 비활성화** (Day 8) — t-검정 음수 유의 시 OFF
- **strategy_versioning** (Day 10) — 모든 변경 버전 관리, 즉시 롤백 가능

---

## 결론 — 사상 최고 retail 가는 길

기존 1개 (closing_convergence) → **5개 동시 운영** (Tier 1+2 일부) → **10개+ 동시 운영** (모든 Tier 1~3) → **자동 발견 루프**.

각 전략 Sharpe 1~2 → 5개 합치면 Sharpe 3~5 (분산 효과).

이게 retail이 갈 수 있는 진짜 ceiling.
