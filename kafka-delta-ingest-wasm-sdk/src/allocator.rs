use std::mem::MaybeUninit;

#[cfg_attr(
    all(target_arch = "wasm32", target_os = "unknown"),
    export_name = "malloc"
)]
#[no_mangle]
pub extern "C" fn proxy_on_memory_allocate(size: usize) -> *mut u8 {
    let mut vec: Vec<MaybeUninit<u8>> = Vec::with_capacity(size);
    unsafe {
        vec.set_len(size);
    }
    let slice = vec.into_boxed_slice();
    Box::into_raw(slice) as *mut u8
}
