use super::*;
use std::cell::RefCell;

#[derive(Debug, PartialEq, Eq)]
enum Observation {
    Callback(usize),
    Free(usize, bool),
    NestedRun(bool),
}

thread_local! {
    static OBSERVATIONS: RefCell<Vec<Observation>> = const { RefCell::new(Vec::new()) };
}

struct JobInput {
    state: *mut ModuleNotificationState,
    next: *mut c_void,
}

extern "C" fn callback(_ctx: *mut ValkeyModuleCtx, data: *mut c_void) {
    OBSERVATIONS.with(|trace| {
        trace
            .borrow_mut()
            .push(Observation::Callback(data as usize))
    });
    // SAFETY: the test owns every JobInput until all jobs are dropped. The queue
    // is boxed and is not borrowed while this synchronous callback executes.
    unsafe {
        let input = &*data.cast::<JobInput>();
        let state = &mut *input.state;
        OBSERVATIONS.with(|trace| {
            trace
                .borrow_mut()
                .push(Observation::NestedRun(state.begin_jobs()))
        });
        if !input.next.is_null() {
            state.enqueue(job(input.next));
        }
    }
}

extern "C" fn free(data: *mut c_void) {
    // SAFETY: JobInput and its boxed queue outlive this synchronous destructor;
    // the test owns the allocation and free only observes the ABI callback.
    let is_protected = unsafe {
        let input = &*data.cast::<JobInput>();
        (*input.state).protects_module("notification-state")
    };
    OBSERVATIONS.with(|trace| {
        trace
            .borrow_mut()
            .push(Observation::Free(data as usize, is_protected))
    });
}

fn job(data: *mut c_void) -> ModulePostNotificationJob {
    ModulePostNotificationJob {
        module_name: "notification-state".into(),
        db_idx: 0,
        callback,
        data,
        free: Some(free),
    }
}

#[test]
fn notification_jobs_appended_by_a_callback_stay_fifo_and_free_once() {
    OBSERVATIONS.with(|trace| trace.borrow_mut().clear());
    let mut state = Box::new(ModuleNotificationState::default());
    let state_ptr = &mut *state as *mut ModuleNotificationState;
    let mut third = Box::new(JobInput {
        state: state_ptr,
        next: std::ptr::null_mut(),
    });
    let third_ptr = (&mut *third as *mut JobInput).cast();
    let mut first = Box::new(JobInput {
        state: state_ptr,
        next: third_ptr,
    });
    let mut second = Box::new(JobInput {
        state: state_ptr,
        next: std::ptr::null_mut(),
    });
    let first_ptr = (&mut *first as *mut JobInput).cast();
    let second_ptr = (&mut *second as *mut JobInput).cast();
    state.enqueue(job(first_ptr));
    state.enqueue(job(second_ptr));
    assert!(state.protects_module("notification-state"));
    assert!(state.begin_jobs());

    while let Some(job) = state.pop_job() {
        assert!(state.protects_module("notification-state"));
        (job.callback)(std::ptr::null_mut(), job.data);
        drop(job);
        assert!(state.protects_module("notification-state"));
        state.release_module("notification-state");
    }
    state.end_jobs();
    assert!(!state.protects_module("notification-state"));
    assert!(!state.begin_jobs());
    assert_eq!(
        OBSERVATIONS.with(|trace| std::mem::take(&mut *trace.borrow_mut())),
        vec![
            Observation::Callback(first_ptr as usize),
            Observation::NestedRun(false),
            Observation::Free(first_ptr as usize, true),
            Observation::Callback(second_ptr as usize),
            Observation::NestedRun(false),
            Observation::Free(second_ptr as usize, true),
            Observation::Callback(third_ptr as usize),
            Observation::NestedRun(false),
            Observation::Free(third_ptr as usize, true),
        ]
    );
    drop(state);
    assert!(OBSERVATIONS.with(|trace| trace.borrow().is_empty()));
}

extern "C" fn no_callback(_ctx: *mut ValkeyModuleCtx, _data: *mut c_void) {}

#[test]
fn notification_reentrancy_is_per_subscription_and_balances_nested_entries() {
    let mut state = ModuleNotificationState::default();
    state.enqueue(ModulePostNotificationJob {
        module_name: "subscriber".into(),
        db_idx: 0,
        callback: no_callback,
        data: std::ptr::null_mut(),
        free: None,
    });
    assert!(state.enter_subscriber("subscriber", 0, false));
    assert!(!state.enter_subscriber("subscriber", 0, false));
    assert!(state.enter_subscriber("subscriber", 1, false));
    assert!(state.enter_subscriber("other-subscriber", 0, false));
    assert!(state.enter_subscriber("subscriber", 0, true));
    assert!(!state.begin_jobs());

    state.leave_subscriber("subscriber", 0);
    assert!(!state.enter_subscriber("subscriber", 0, false));
    state.leave_subscriber("subscriber", 1);
    state.leave_subscriber("other-subscriber", 0);
    assert!(!state.begin_jobs());
    state.leave_subscriber("subscriber", 0);
    assert!(state.begin_jobs());
    assert!(!state.begin_jobs());
    let job = state.pop_job().unwrap();
    (job.callback)(std::ptr::null_mut(), job.data);
    drop(job);
    state.release_module("subscriber");
    state.end_jobs();

    assert!(state.enter_subscriber("subscriber", 0, false));
    state.leave_subscriber("subscriber", 0);
    assert!(!state.protects_module("subscriber"));
    assert!(!state.begin_jobs());
}
