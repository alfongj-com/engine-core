//! Register signals before starting workers so deployment SIGTERM and Ctrl+C
//! both enter the same coordinated HTTP/worker shutdown path.

#[cfg(unix)]
pub(crate) struct ShutdownSignal {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignal {
    pub(crate) fn listen() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    pub(crate) async fn wait(mut self) -> std::io::Result<()> {
        let received = tokio::select! {
            received = self.interrupt.recv() => received,
            received = self.terminate.recv() => received,
        };
        received.ok_or_else(|| std::io::Error::other("shutdown signal listener closed"))
    }
}

#[cfg(not(unix))]
pub(crate) struct ShutdownSignal;

#[cfg(not(unix))]
impl ShutdownSignal {
    pub(crate) fn listen() -> std::io::Result<Self> {
        Ok(Self)
    }

    pub(crate) async fn wait(self) -> std::io::Result<()> {
        tokio::signal::ctrl_c().await
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{
        process::{Command, Stdio},
        time::{Duration, Instant},
    };

    #[test]
    fn sigterm_and_sigint_enter_coordinated_shutdown() {
        const CHILD: &str = "ENGINE_SHUTDOWN_SIGNAL_TEST_FILE";
        if let Some(path) = std::env::var_os(CHILD) {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let signal = ShutdownSignal::listen().unwrap();
                std::fs::write(&path, "ready").unwrap();
                tokio::time::timeout(Duration::from_secs(5), signal.wait())
                    .await
                    .unwrap()
                    .unwrap();
                // Reaching the continuation proves the child did not terminate
                // through the OS default action for either signal.
                std::fs::write(&path, "graceful shutdown").unwrap();
            });
            return;
        }
        struct Child(std::process::Child);
        impl Drop for Child {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        for signal in ["-TERM", "-INT"] {
            let path = std::env::temp_dir().join(format!(
                "engine-shutdown-{}-{}",
                std::process::id(),
                rand::random::<u64>()
            ));
            let mut child = Child(
                Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "shutdown::tests::sigterm_and_sigint_enter_coordinated_shutdown",
                        "--nocapture",
                    ])
                    .env(CHILD, &path)
                    .stdout(Stdio::null())
                    .stderr(Stdio::inherit())
                    .spawn()
                    .unwrap(),
            );
            let deadline = Instant::now() + Duration::from_secs(5);
            while !path.exists() {
                assert!(
                    child.0.try_wait().unwrap().is_none(),
                    "signal child exited before listening"
                );
                assert!(Instant::now() < deadline, "signal child never became ready");
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                Command::new("kill")
                    .args([signal, &child.0.id().to_string()])
                    .status()
                    .unwrap()
                    .success()
            );
            loop {
                if let Some(status) = child.0.try_wait().unwrap() {
                    assert!(status.success(), "{signal} bypassed coordinated shutdown");
                    break;
                }
                assert!(Instant::now() < deadline, "signal child did not shut down");
                std::thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(std::fs::read_to_string(&path).unwrap(), "graceful shutdown");
            std::fs::remove_file(path).unwrap();
        }
    }
}
