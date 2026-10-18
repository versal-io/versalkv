use super::{fork_owned_child, ForkChild};
use crate::foundation::io::sys::process::{open_pidfd, WaitOutcome};
use std::time::{Duration, Instant};

#[test]
fn forked_child_returns_status_without_running_parent_path() {
    let mut child = fork_owned_child(|| 7).expect("fork child");
    assert_eq!(child.wait().unwrap(), WaitOutcome::Exited(7));
}

#[test]
fn nonblocking_reap_reports_running_then_exit_status() {
    let mut child = fork_owned_child(|| {
        std::thread::sleep(Duration::from_millis(100));
        0
    })
    .expect("fork child");
    assert_eq!(child.try_wait().unwrap(), WaitOutcome::Running);
    assert_eq!(child.wait().unwrap(), WaitOutcome::Exited(0));
}

#[test]
fn child_handle_waits_for_its_exact_pid() {
    let mut first = fork_owned_child(|| 1).expect("fork first child");
    let mut second = fork_owned_child(|| 2).expect("fork second child");
    let second_pid = second.pid();
    assert_eq!(second.pid(), second_pid);
    assert_eq!(second.wait().unwrap(), WaitOutcome::Exited(2));
    assert_eq!(first.wait().unwrap(), WaitOutcome::Exited(1));
}

#[test]
fn wait_timeout_is_bounded() {
    let mut child = fork_owned_child(|| {
        std::thread::sleep(Duration::from_millis(200));
        0
    })
    .expect("fork child");
    let start = Instant::now();
    assert!(!child.wait_timeout(Duration::from_millis(20)).unwrap());
    assert!(start.elapsed() < Duration::from_millis(150));
    child.kill().expect("kill child");
    let _ = child.wait();
}

#[test]
fn pidfd_open_reports_child_lifecycle_capability() {
    let mut child = fork_owned_child(|| {
        std::thread::sleep(Duration::from_millis(50));
        0
    })
    .expect("fork child");

    let pidfd = open_pidfd(child.pid());
    match pidfd {
        Ok(fd) => drop(fd),
        Err(error) => assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::Unsupported
                    | std::io::ErrorKind::PermissionDenied
                    | std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::InvalidInput
                    | std::io::ErrorKind::Other
            ),
            "pidfd failure should be a platform capability error, got {error:?}"
        ),
    }

    child.kill().expect("kill child");
    let _ = child.wait();
}

fn _assert_send_sync_contract(_: ForkChild) {}
