#!/usr/bin/env python3
"""Записывает README.md проекта DSFS. Запускать из корня репозитория."""

from pathlib import Path

README = '''# Distributed Systems Failure Simulator (DSFS)

Детерминированный симулятор распределённых систем с дискретно-событийной
моделью, собственной реализацией Raft, посимуляцией сети, внедрением
отказов и оффлайн-проверкой согласованности.

Цель проекта — не реализм, а **воспроизводимость**: любой сбой можно
воспроизвести по seed, любой инвариант проверяется по записанному журналу,
любой сценарий «а что если» выражается в YAML.

## Статус

- Детерминированный движок с виртуальным временем и полным порядком событий.
- Сеть с задержкой, jitter'ом, потерями, дублированием и разделениями.
- Raft: выборы, heartbeat'ы, репликация лога, commit, apply, recovery.
- Внедрение отказов из YAML-сценариев (`crash_node`, `restart_node`,
  `partition`, `heal`, `set_slow`, `clear_slow`, `submit_write`,
  `wait_for_leader`).
- Оффлайн-checker с пятью инвариантами над JSONL-журналом.
- CLI: `init`, `run`, `inspect`, `validate`, `replay`.
- Property-тесты на случайных сценариях; бенчмарки на Criterion.

Ограничения реализации Raft явно зафиксированы в `docs/raft.md`
(snapshot'ы, изменение состава кластера, pre-vote — **не** реализованы).

## Быстрый старт

    cargo build --release
    ./target/release/dsfs init --force
    ./target/release/dsfs run configs/scenarios/example.yaml
    ./target/release/dsfs inspect results/latest.jsonl
    ./target/release/dsfs validate results/latest.jsonl
    ./target/release/dsfs replay results/latest.jsonl

`validate` возвращает код 0, если журнал удовлетворяет всем инвариантам,
1 — если есть нарушения, 2 — при ошибке ввода-вывода.

## Пример сценария

```yaml
version: 1
name: leader_failure
seed: 42
cluster:
  nodes: 5
  raft:
    election_timeout_ms: 150
    heartbeat_interval_ms: 30
network:
  default:
    base_latency_ms: 5
    jitter_ms: 2
    loss_probability: 0.0
    duplicate_probability: 0.0
events:
  - wait_for_leader: {}
  - at_ms: 1000
    crash_node: 3
  - wait_for_leader: {}
  - at_ms: 3000
    submit_write:
      key: balance
      value: "500"