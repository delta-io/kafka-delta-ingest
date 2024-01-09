// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::types::*;
use std::ptr::null_mut;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

extern "C" {
    fn proxy_log(level: LogLevel, message_data: *const u8, message_size: usize) -> Status;
}

pub fn log(level: LogLevel, message: &str) -> Result<(), Status> {
    unsafe {
        match proxy_log(level, message.as_ptr(), message.len()) {
            Status::Ok => Ok(()),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_get_log_level(return_level: *mut LogLevel) -> Status;
}

pub fn get_log_level() -> Result<LogLevel, Status> {
    let mut return_level: LogLevel = LogLevel::Trace;
    unsafe {
        match proxy_get_log_level(&mut return_level) {
            Status::Ok => Ok(return_level),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_get_current_time_nanoseconds(return_time: *mut u64) -> Status;
}

pub fn get_current_time() -> Result<SystemTime, Status> {
    let mut return_time: u64 = 0;
    unsafe {
        match proxy_get_current_time_nanoseconds(&mut return_time) {
            Status::Ok => Ok(UNIX_EPOCH + Duration::from_nanos(return_time)),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_set_tick_period_milliseconds(period: u32) -> Status;
}

pub fn set_tick_period(period: Duration) -> Result<(), Status> {
    unsafe {
        match proxy_set_tick_period_milliseconds(period.as_millis() as u32) {
            Status::Ok => Ok(()),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_set_output(buffer_data: *const u8, buffer_size: usize) -> Status;
}

pub fn set_output(value: &[u8]) -> Result<(), Status> {
    unsafe {
        match proxy_set_output(value.as_ptr(), value.len()) {
            Status::Ok => Ok(()),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_get_message_body(
        return_buffer_data: *mut *mut u8,
        return_buffer_size: *mut usize,
    ) -> Status;
}

pub fn get_body() -> Result<Option<Bytes>, Status> {
    let mut return_data: *mut u8 = null_mut();
    let mut return_size: usize = 0;
    unsafe {
        match proxy_get_message_body(&mut return_data, &mut return_size) {
            Status::Ok => {
                if !return_data.is_null() {
                    Ok(Some(Vec::from_raw_parts(
                        return_data,
                        return_size,
                        return_size,
                    )))
                } else {
                    Ok(None)
                }
            }
            Status::NotFound => Ok(None),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_get_buffer_bytes(
        buffer_type: BufferType,
        start: usize,
        max_size: usize,
        return_buffer_data: *mut *mut u8,
        return_buffer_size: *mut usize,
    ) -> Status;
}

pub fn get_buffer(
    buffer_type: BufferType,
    start: usize,
    max_size: usize,
) -> Result<Option<Bytes>, Status> {
    let mut return_data: *mut u8 = null_mut();
    let mut return_size: usize = 0;
    unsafe {
        match proxy_get_buffer_bytes(
            buffer_type,
            start,
            max_size,
            &mut return_data,
            &mut return_size,
        ) {
            Status::Ok => {
                if !return_data.is_null() {
                    Ok(Some(Vec::from_raw_parts(
                        return_data,
                        return_size,
                        return_size,
                    )))
                } else {
                    Ok(None)
                }
            }
            Status::NotFound => Ok(None),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_get_property(
        property: i32,
        return_value_data: *mut *mut u8,
        return_value_size: *mut usize,
    ) -> Status;
}

pub fn get_property(path: MapType) -> Result<Option<Bytes>, Status> {
    let mut return_data: *mut u8 = null_mut();
    let mut return_size: usize = 0;
    unsafe {
        match proxy_get_property(path as i32, &mut return_data, &mut return_size) {
            Status::Ok => {
                if !return_data.is_null() {
                    Ok(Some(Vec::from_raw_parts(
                        return_data,
                        return_size,
                        return_size,
                    )))
                } else {
                    Ok(None)
                }
            }
            Status::NotFound => Ok(None),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_define_metric(
        metric_type: MetricType,
        name_data: *const u8,
        name_size: usize,
        return_id: *mut u32,
    ) -> Status;
}

pub fn define_metric(metric_type: MetricType, name: &str) -> Result<u32, Status> {
    let mut return_id: u32 = 0;
    unsafe {
        match proxy_define_metric(metric_type, name.as_ptr(), name.len(), &mut return_id) {
            Status::Ok => Ok(return_id),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_get_metric(metric_id: u32, return_value: *mut u64) -> Status;
}

pub fn get_metric(metric_id: u32) -> Result<u64, Status> {
    let mut return_value: u64 = 0;
    unsafe {
        match proxy_get_metric(metric_id, &mut return_value) {
            Status::Ok => Ok(return_value),
            Status::NotFound => Err(Status::NotFound),
            Status::BadArgument => Err(Status::BadArgument),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_record_metric(metric_id: u32, value: u64) -> Status;
}

pub fn record_metric(metric_id: u32, value: u64) -> Result<(), Status> {
    unsafe {
        match proxy_record_metric(metric_id, value) {
            Status::Ok => Ok(()),
            Status::NotFound => Err(Status::NotFound),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}

extern "C" {
    fn proxy_increment_metric(metric_id: u32, offset: i64) -> Status;
}

pub fn increment_metric(metric_id: u32, offset: i64) -> Result<(), Status> {
    unsafe {
        match proxy_increment_metric(metric_id, offset) {
            Status::Ok => Ok(()),
            Status::NotFound => Err(Status::NotFound),
            Status::BadArgument => Err(Status::BadArgument),
            status => panic!("unexpected status: {}", status as u32),
        }
    }
}
