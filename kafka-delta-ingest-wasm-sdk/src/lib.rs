pub mod hostcalls;
pub mod traits;
pub mod types;

mod allocator;
mod dispatcher;
mod logger;

// For crate-type="cdylib".
#[cfg(not(wasi_exec_model_reactor))]
#[macro_export]
macro_rules! main {
    ($code:block) => {
        #[cfg(target_os = "wasi")]
        extern "C" {
            fn __wasm_call_ctors();
        }

        #[no_mangle]
        pub extern "C" fn _initialize() {
            #[cfg(target_os = "wasi")]
            unsafe {
                __wasm_call_ctors();
            }

            $code;
        }
    };
}

// For crate-type="bin" with RUSTFLAGS="-Z wasi-exec-model=reactor".
#[cfg(wasi_exec_model_reactor)]
#[macro_export]
macro_rules! main {
    ($code:block) => {
        pub fn main() -> Result<(), Box<dyn std::error::Error>> {
            $code;
            Ok(())
        }
    };
}

pub fn set_log_level(level: types::LogLevel) {
    logger::set_log_level(level);
}

pub fn set_root_context(callback: types::NewRootContext) {
    dispatcher::set_root_context(callback);
}

pub fn set_message_context(callback: types::NewMessageContext) {
    dispatcher::set_message_context(callback);
}
