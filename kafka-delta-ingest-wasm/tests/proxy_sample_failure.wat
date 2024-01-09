(module
    (func $set_output (import "host" "proxy_set_output") (param i32) (param i32))
    (func $get_body (import "host" "proxy_get_message_body") (param i32) (param i32))
    
    (memory (export "memory") 2 3)

    ;; Write 'hello world\n' to memory at an offset of 8 bytes
    ;; Note the trailing newline which is required for the text to appear
    (data (i32.const 8) "{}")
    (func $create_context (export "proxy_on_context_create") (param $x i32) (param $y i32))
    (func $receive_message (export "proxy_on_message_received") (param $x i32) (param $y i32) (result i32)
        i32.const 8
        i32.const 2
        call $set_output
        i32.const 1)
)