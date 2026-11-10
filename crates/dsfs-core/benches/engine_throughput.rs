//! Engine throughput benchmarks.

use std::time::Duration as StdDuration;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use dsfs_core::engine::{EngineConfig, FullNetwork, LinkConfig, SimulationEngine};
use dsfs_core::log::InMemoryLog;
use dsfs_core::node::{NodeId, NodeImpl, RaftAdapter};
use dsfs_core::rng::{RaftStreamPurpose, StreamId, StreamRng};
use dsfs_core::time::Duration as VDuration;
use dsfs_raft::{PeerId, RaftConfig, RaftNode};

fn build_cluster(n: u32, seed: u64) -> SimulationEngine<InMemoryLog, FullNetwork> {
    let mut link = LinkConfig::perfect();
    link.base_latency = VDuration::from_millis(5);
    let net = FullNetwork::new(seed, link);
    let mut e = SimulationEngine::new(
        EngineConfig {
            master_seed: seed,
            max_events: u64::MAX,
        },
        net,
        InMemoryLog::new(),
    );
    let peers: Vec<PeerId> = (1..=n).map(PeerId).collect();
    let cfg = RaftConfig {
        election_timeout: StdDuration::from_millis(150),
        heartbeat_interval: StdDuration::from_millis(30),
    };
    for &p in &peers {
        let rng = StreamRng::new(
            seed,
            StreamId::raft(p.0, RaftStreamPurpose::ElectionTimeout),
        );
        let raft = RaftNode::new(p, peers.clone(), cfg.clone(), Box::new(rng));
        e.add_node(NodeImpl::Raft(Box::new(RaftAdapter::new(
            NodeId(p.0),
            raft,
        ))))
        .unwrap();
    }
    e
}

fn bench_three_nodes_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/raft_3_nodes");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("10k_events", |b| {
        b.iter(|| {
            let mut e = build_cluster(3, 42);
            black_box(e.run_until(10_000)).unwrap();
        });
    });
    group.finish();
}

fn bench_five_nodes_10k(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/raft_5_nodes");
    group.throughput(Throughput::Elements(10_000));
    group.bench_function("10k_events", |b| {
        b.iter(|| {
            let mut e = build_cluster(5, 42);
            black_box(e.run_until(10_000)).unwrap();
        });
    });
    group.finish();
}

fn bench_five_nodes_100k(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine/raft_5_nodes");
    group.throughput(Throughput::Elements(100_000));
    group.bench_function("100k_events", |b| {
        b.iter(|| {
            let mut e = build_cluster(5, 42);
            black_box(e.run_until(100_000)).unwrap();
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_three_nodes_10k,
    bench_five_nodes_10k,
    bench_five_nodes_100k
);
criterion_main!(benches);
