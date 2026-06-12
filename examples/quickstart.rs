use tasker::{TaskEngine, TaskResult, TaskSpec, WorkerId, Priority};

fn main() {
    let engine = TaskEngine::new();

    // Create a root task.
    let root = engine
        .create(TaskSpec {
            task_type: "root".into(),
            payload: b"root data".to_vec(),
            priority: Priority(0),
            parent: None,
            blocking_on: vec![],
            metadata: vec![],
        })
        .unwrap();

    // Create two child tasks that depend on root.
    let child_a = engine
        .create(TaskSpec {
            task_type: "child_a".into(),
            payload: b"child a".to_vec(),
            priority: Priority(0),
            parent: Some(root),
            blocking_on: vec![root],
            metadata: vec![],
        })
        .unwrap();

    let child_b = engine
        .create(TaskSpec {
            task_type: "child_b".into(),
            payload: b"child b".to_vec(),
            priority: Priority(0),
            parent: Some(root),
            blocking_on: vec![root],
            metadata: vec![],
        })
        .unwrap();

    // Create a final task that depends on both children.
    let final_task = engine
        .create(TaskSpec {
            task_type: "final".into(),
            payload: b"final".to_vec(),
            priority: Priority(0),
            parent: None,
            blocking_on: vec![child_a, child_b],
            metadata: vec![],
        })
        .unwrap();

    println!("Created tasks: root={:?}, a={:?}, b={:?}, final={:?}", root, child_a, child_b, final_task);

    // Start and complete root.
    let mut guard = engine.start(root, WorkerId(1)).unwrap();
    guard.complete(TaskResult::Ok { output_ref: 1 });

    // Both children should now be Ready.
    let stats = engine.stats();
    println!("After root complete: {:?}", stats);
    assert_eq!(stats.ready, 2);
    assert_eq!(stats.pending, 1); // final still waiting

    // Complete child_a.
    let mut guard = engine.start(child_a, WorkerId(2)).unwrap();
    guard.complete(TaskResult::Ok { output_ref: 2 });

    // Complete child_b.
    let mut guard = engine.start(child_b, WorkerId(3)).unwrap();
    guard.complete(TaskResult::Ok { output_ref: 3 });

    // Final should now be Ready.
    let stats = engine.stats();
    println!("After all complete: {:?}", stats);
    assert_eq!(stats.ready, 1);
    assert_eq!(stats.completed, 3);

    // Complete final.
    let mut guard = engine.start(final_task, WorkerId(4)).unwrap();
    guard.complete(TaskResult::Ok { output_ref: 4 });

    let stats = engine.stats();
    println!("Final stats: {:?}", stats);
    assert_eq!(stats.completed, 4);
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.ready, 0);

    println!("All tasks completed successfully!");
}
