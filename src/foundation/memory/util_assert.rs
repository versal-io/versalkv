#[macro_export]
macro_rules! server_assert {
    ($cond:expr) => {
        if !($cond) {
            panic!("Assertion failed: {}", stringify!($cond));
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !($cond) {
            panic!($($arg)+);
        }
    };
}

#[macro_export]
macro_rules! server_panic {
    () => { panic!("fatal error") };
    ($($arg:tt)+) => { panic!($($arg)+) };
}
