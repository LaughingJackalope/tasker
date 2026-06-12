use criterion::{Criterion, criterion_group, criterion_main};
use tasker::{Priority, TaskEngine, TaskResult, TaskSpec, WorkerId};

fn make_spec(task_type: &str) -> TaskSpec {
    TaskSpec {
        task_type: task_type.to_string(),
        payload: vec![],
        priority: Priority(0),
        parent: None,
        blocking_on: vec![],
        metadata: vec![],
    }
}

fn create_throughput(c: &mut Criterion) {
    c.bench_function("create_10000_tasks", |b| {
        b.iter(|| {
            let engine = TaskEngine::new();
            for i in 0..10_000 {
                engine.create(make_spec(&format!("task_{}", i))).unwrap();
            }
        })
    });
}

fn create_with_chain_deps(c: &mut Criterion) {
    c.bench_function("create_1000_chain", |b| {
        b.iter(|| {
            let engine = TaskEngine::new();
            let mut prev = engine.create(make_spec("root")).unwrap();
            for i in 1..1_000 {
                let spec = TaskSpec {
                    blocking_on: vec![prev],
                    ..make_spec(&format!("task_{}", i))
                };
                prev = engine.create(spec).unwrap();
            }
        })
    });
}

fn complete_chain(c: &mut Criterion) {
    c.bench_function("complete_1000_chain", |b| {
        b.iter(|| {
            let engine = TaskEngine::new();
            let mut ids = vec![engine.create(make_spec("root")).unwrap()];

            for i in 1..1_000 {
                ids.push(
                    engine
                        .create(TaskSpec {
                            blocking_on: vec![ids[i - 1]],
                            ..make_spec(&format!("task_{}", i))
                        })
                        .unwrap(),
                );
            }

            for id in ids {
                let mut guard = engine.start(id, WorkerId(1)).unwrap();
                guard.complete(TaskResult::Ok { output_ref: 1 });
            }
        })
    });
}

fn dag_breadth(c: &mut Criterion) {
    c.bench_function("dag_breadth_100", |b| {
        b.iter(|| {
            let engine = TaskEngine::new();
            let root = engine.create(make_spec("root")).unwrap();

            let children: Vec<_> = (0..100)
                .map(|i| {
                    engine
                        .create(TaskSpec {
                            blocking_on: vec![root],
                            ..make_spec(&format!("child_{}", i))
                        })
                        .unwrap()
                })
                .collect();

            let mut guard = engine.start(root, WorkerId(1)).unwrap();
            guard.complete(TaskResult::Ok { output_ref: 1 });

            let stats = engine.stats();
            assert_eq!(stats.ready, 100);

            for child in children {
                let mut guard = engine.start(child, WorkerId(2)).unwrap();
                guard.complete(TaskResult::Ok { output_ref: 2 });
            }
        })
    });
}

fn stats_query(c: &mut Criterion) {
    c.bench_function("stats_10000_tasks", |b| {
        let engine = TaskEngine::new();
        for i in 0..10_000 {
            engine.create(make_spec(&format!("task_{}", i))).unwrap();
        }
        b.iter(|| {
            let _stats = engine.stats();
        })
    });
}

criterion_group!(
    benches,
    create_throughput,
    create_with_chain_deps,
    complete_chain,
    dag_breadth,
    stats_query
);
criterion_main!(benches);
