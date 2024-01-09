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

use crate::hostcalls;
use crate::types::*;
use std::time::{Duration, SystemTime};

pub trait Context {
    fn get_current_time(&self) -> SystemTime {
        hostcalls::get_current_time().unwrap()
    }

    fn get_property(&self, path: MapType) -> Option<Bytes> {
        hostcalls::get_property(path).unwrap()
    }
}

pub trait MessageContext: Context {
    fn on_message_received(&mut self, _body_size: usize) -> Action {
        Action::Use
    }

    fn set_message_body(&self, _start: usize, _size: usize, value: &[u8]) {
        hostcalls::set_output(value).unwrap();
    }

    fn on_log(&mut self) {}
}

pub trait RootContext: Context {
    fn on_vm_start(&mut self, _vm_configuration_size: usize) -> bool {
        true
    }

    fn get_vm_configuration(&self) -> Option<Bytes> {
        hostcalls::get_buffer(BufferType::VmConfiguration, 0, usize::MAX).unwrap()
    }

    fn on_configure(&mut self, _plugin_configuration_size: usize) -> bool {
        true
    }

    fn get_plugin_configuration(&self) -> Option<Bytes> {
        hostcalls::get_buffer(BufferType::PluginConfiguration, 0, usize::MAX).unwrap()
    }

    fn set_tick_period(&self, period: Duration) {
        hostcalls::set_tick_period(period).unwrap()
    }

    fn on_tick(&mut self) {}

    fn on_log(&mut self) {}

    fn create_message_context(&self, _context_id: u32) -> Option<Box<dyn MessageContext>> {
        None
    }

    fn get_type(&self) -> Option<ContextType> {
        None
    }
}
