# Consumer Group과 파티션 리밸런싱 — 소비를 확장하면 무엇이 흔들리는가

주문 서비스가 `order-events` Topic으로 이벤트를 흘려보내고 있습니다.

처음에는 Consumer 하나로 충분했습니다.

그런데 프로모션 날 트래픽이 10배로 뛰었고, lag이 수십만 건씩 쌓이기 시작합니다.

"Consumer를 늘리면 되겠지"라며 인스턴스를 3대로 올렸습니다.

그런데 궁금해집니다.

> 같은 Topic을 읽는 Consumer가 3대가 되면, 누가 어떤 메시지를 가져가는가? 같은 메시지를 셋이 중복으로 읽지는 않는가?

---

## 01. 파티션 — 병렬 처리의 단위

Kafka의 Topic은 하나의 통짜 큐가 아닙니다.

**Partition**이라는 여러 개의 독립된 로그로 쪼개져 있습니다.

```
order-events (Topic)
├── Partition 0: [msg, msg, msg, ...]
├── Partition 1: [msg, msg, msg, ...]
└── Partition 2: [msg, msg, msg, ...]
```

Producer는 메시지 Key(예: `"orderId": "order_501"`)를 해싱해 파티션을 고릅니다.

같은 Key는 항상 같은 파티션으로 갑니다.

그래서 **순서 보장은 파티션 안에서만 성립합니다.** Topic 전체의 순서는 보장되지 않습니다.

쉽게 말하면, 파티션은 "병렬로 처리해도 되는 단위"로 Topic을 미리 잘라둔 것입니다.

그렇다면 이 파티션들을 Consumer 3대가 어떻게 나눠 갖을까요?

---

## 02. Consumer Group — 파티션을 나눠 갖는 팀

같은 `group.id`를 가진 Consumer들은 하나의 **Consumer Group**으로 묶입니다.

Kafka는 그룹 안에서 규칙 하나를 강제합니다.

**하나의 파티션은 그룹 내에서 정확히 하나의 Consumer에게만 할당됩니다.**

```
Partition 0 → Consumer A
Partition 1 → Consumer B
Partition 2 → Consumer C
```

이 규칙 덕분에 두 가지가 동시에 해결됩니다.

그룹 내 중복 소비가 없고, 파티션 내 순서가 소비 시점에도 유지됩니다.

한 파티션을 둘이 읽으면 순서도 깨지고 offset 관리도 불가능해지기 때문입니다.

**Consumer가 파티션보다 많아지면 어떻게 될까?**

파티션이 3개인데 Consumer를 5대로 올리면, 2대는 할당받을 파티션이 없어 **놀게 됩니다(idle).**

그래서 Consumer Group의 병렬성 상한은 파티션 수입니다.

"Consumer만 늘리면 무한히 확장된다"는 오해가 여기서 무너집니다.

참고로 그룹이 다르면 이 규칙과 무관합니다. 다른 `group.id`의 그룹은 같은 Topic을 처음부터 독립적으로 전부 읽습니다 — 이게 Pub/Sub이 구현되는 방식입니다.

---

## 03. 리밸런싱 — 소유권을 다시 나누는 순간

Consumer 3대가 파티션을 사이좋게 나눠 갖고 있었습니다.

그런데 배포가 시작되어 Consumer A가 내려갔습니다.

Partition 0은 이제 주인이 없습니다. 누군가 이어받아야 합니다.

이렇게 **파티션 소유권을 그룹 전체에 다시 배분하는 과정이 Rebalancing**입니다.

리밸런싱은 그룹 구성이나 파티션 구성이 바뀔 때 일어납니다.

```
Consumer 추가 (스케일 아웃, 배포)
↓
Consumer 이탈 (정상 종료)
↓
Consumer 장애 (heartbeat 끊김, session.timeout.ms 초과)
↓
처리 지연 (max.poll.interval.ms 초과 → 죽은 것으로 간주)
↓
Topic 파티션 수 변경
```

조율은 브로커 측의 **Group Coordinator**가 맡습니다.

Consumer들은 Coordinator에 heartbeat를 보내 생존을 알리고, Coordinator는 구성 변화를 감지하면 리밸런싱을 시작합니다.

여기까지 들으면 자연스러운 복구 메커니즘 같습니다.

그런데 이 과정에는 비싼 대가가 있습니다.

---

## 04. Stop-the-world — 리밸런싱의 비용

전통적인 방식(Eager Rebalancing)은 이렇게 동작합니다.

```
리밸런싱 시작
↓
모든 Consumer가 모든 파티션 소유권을 반납 (revoke)
↓
전원 그룹 재가입 (rejoin)
↓
파티션 전체를 재할당 (reassign)
↓
소비 재개
```

문제가 보이시나요?

Consumer 1대가 빠졌을 뿐인데, **멀쩡한 파티션까지 포함해 그룹 전체의 소비가 멈춥니다.**

이것이 리밸런싱의 stop-the-world 문제입니다.

Consumer가 수십 대인 그룹이라면 재할당에 수십 초가 걸리고, 그동안 lag은 계속 쌓입니다.

롤링 배포라면 더 나쁩니다. 인스턴스가 하나 내려가고 올라올 때마다 리밸런싱이 반복됩니다.

그래서 두 가지 개선이 나왔습니다.

**Cooperative(Incremental) Rebalancing** — 전부 반납하는 대신, 실제로 주인이 바뀌어야 하는 파티션만 점진적으로 이동합니다. 나머지 Consumer는 소비를 계속합니다.

**Static Membership** — Consumer에 고정 ID(`group.instance.id`)를 부여합니다. 재시작한 Consumer가 같은 ID로 돌아오면, timeout 안에는 "같은 멤버가 돌아왔다"고 보고 리밸런싱 자체를 생략합니다. 롤링 배포의 불필요한 리밸런싱이 사라집니다.

그런데 리밸런싱이 빨라져도, 완벽하지 않은 지점이 하나 남습니다.

---

## 05. 리밸런싱과 중복 처리

Consumer는 "여기까지 읽었다"는 위치(offset)를 브로커에 주기적으로 commit합니다.

Consumer A가 offset 100까지 commit하고, 110까지 처리한 상태에서 리밸런싱이 일어났다고 합시다.

```
offset 100 commit 완료
↓
101~110 처리 (commit 전)
↓
리밸런싱 발생 — Partition 0이 Consumer B로 이동
↓
Consumer B는 마지막 commit 지점부터 읽음
↓
101~110 재처리 (중복)
```

**리밸런싱은 at-least-once 세계에서 중복이 태어나는 대표적인 순간입니다.**

브로커는 "commit된 곳부터 다시"라는 안전한 선택을 할 뿐이고, 그 대가로 중복이 생깁니다.

그래서 리밸런싱을 줄이는 것만큼 중요한 것이, 이미 배운 멱등성(idempotency)입니다.

Consumer가 같은 메시지를 두 번 받아도 결과가 한 번과 같도록 설계되어 있어야, 리밸런싱은 "느려지는 이벤트"에 그치고 "데이터가 틀어지는 사고"가 되지 않습니다.

기술 선택의 질문도 여기서 나옵니다.

"리밸런싱을 어떻게 없앨까?"가 아니라, **"리밸런싱이 일어나도 우리 시스템은 안전한가?"**를 물어야 합니다.

---

## 스스로 답해봐야 할 질문

1. Kafka Consumer Group 안에서 하나의 파티션은 몇 개의 Consumer에게 할당되며, 그 규칙은 왜 필요한가?
2. Consumer 수가 파티션 수보다 많아지면 무엇이 일어나는가?
3. 파티션 리밸런싱은 어떤 상황들에서 발생하는가?
4. Eager 리밸런싱의 stop-the-world 문제는 무엇이고, Cooperative Rebalancing과 Static Membership은 이를 어떻게 개선하는가?
5. 리밸런싱이 일어나면 메시지 중복 처리가 생길 수 있는 이유는 무엇인가?
