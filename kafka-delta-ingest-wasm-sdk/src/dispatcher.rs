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

use crate::traits::*;
use crate::types::*;
use hashbrown::HashMap;
use std::cell::{Cell, RefCell};

thread_local! {
static DISPATCHER: Dispatcher = Dispatcher::new();
}

pub(crate) fn set_root_context(callback: NewRootContext) {
    DISPATCHER.with(|dispatcher| dispatcher.set_root_context(callback));
}

pub(crate) fn set_message_context(callback: NewMessageContext) {
    DISPATCHER.with(|dispatcher| dispatcher.set_message_context(callback));
}

struct NoopRoot;

impl Context for NoopRoot {}
impl RootContext for NoopRoot {}

struct Dispatcher {
    new_root: Cell<Option<NewRootContext>>,
    roots: RefCell<HashMap<u32, Box<dyn RootContext>>>,
    new_message: Cell<Option<NewMessageContext>>,
    messages: RefCell<HashMap<u32, Box<dyn MessageContext>>>,
    active_id: Cell<u32>,
}

impl Dispatcher {
    fn new() -> Dispatcher {
        Dispatcher {
            new_root: Cell::new(None),
            roots: RefCell::new(HashMap::new()),
            new_message: Cell::new(None),
            messages: RefCell::new(HashMap::new()),
            active_id: Cell::new(0),
        }
    }

    fn set_root_context(&self, callback: NewRootContext) {
        self.new_root.set(Some(callback));
    }

    fn set_message_context(&self, callback: NewMessageContext) {
        self.new_message.set(Some(callback));
    }

    fn create_root_context(&self, context_id: u32) {
        let new_context = match self.new_root.get() {
            Some(f) => f(context_id),
            None => Box::new(NoopRoot),
        };
        if self
            .roots
            .borrow_mut()
            .insert(context_id, new_context)
            .is_some()
        {
            panic!("duplicate context_id")
        }
    }

    fn create_message_context(&self, context_id: u32, root_context_id: u32) {
        let new_context = match self.roots.borrow().get(&root_context_id) {
            Some(root_context) => match self.new_message.get() {
                Some(f) => f(context_id, root_context_id),
                None => match root_context.create_message_context(context_id) {
                    Some(stream_context) => stream_context,
                    None => panic!("create_message_context returned None"),
                },
            },
            None => panic!("invalid root_context_id"),
        };
        if self
            .messages
            .borrow_mut()
            .insert(context_id, new_context)
            .is_some()
        {
            panic!("duplicate context_id")
        }
    }

    fn on_create_context(&self, context_id: u32, root_context_id: u32) {
        if root_context_id == 0 {
            self.create_root_context(context_id);
        } else if self.new_message.get().is_some() {
            self.create_message_context(context_id, root_context_id);
        } else if let Some(root_context) = self.roots.borrow().get(&root_context_id) {
            match root_context.get_type() {
                Some(ContextType::MessageContext) => {
                    self.create_message_context(context_id, root_context_id)
                }
                None => panic!("missing ContextType on root_context"),
            }
        } else {
            panic!("invalid root_context_id and missing constructors");
        }
    }

    fn on_log(&self, context_id: u32) {
        if let Some(message) = self.messages.borrow_mut().get_mut(&context_id) {
            self.active_id.set(context_id);
            message.on_log()
        } else if let Some(root) = self.roots.borrow_mut().get_mut(&context_id) {
            self.active_id.set(context_id);
            root.on_log()
        } else {
            panic!("invalid context_id")
        }
    }

    fn on_vm_start(&self, context_id: u32, vm_configuration_size: usize) -> bool {
        if let Some(root) = self.roots.borrow_mut().get_mut(&context_id) {
            self.active_id.set(context_id);
            root.on_vm_start(vm_configuration_size)
        } else {
            panic!("invalid context_id")
        }
    }

    fn on_configure(&self, context_id: u32, plugin_configuration_size: usize) -> bool {
        if let Some(root) = self.roots.borrow_mut().get_mut(&context_id) {
            self.active_id.set(context_id);
            root.on_configure(plugin_configuration_size)
        } else {
            panic!("invalid context_id")
        }
    }

    fn on_tick(&self, context_id: u32) {
        if let Some(root) = self.roots.borrow_mut().get_mut(&context_id) {
            self.active_id.set(context_id);
            root.on_tick()
        } else {
            panic!("invalid context_id")
        }
    }

    fn on_message_received(&self, context_id: u32, body_size: usize) -> Action {
        if let Some(message) = self.messages.borrow_mut().get_mut(&context_id) {
            self.active_id.set(context_id);
            message.on_message_received(body_size)
        } else {
            panic!("invalid context_id")
        }
    }
}

#[no_mangle]
pub extern "C" fn proxy_on_context_create(context_id: u32, root_context_id: u32) {
    DISPATCHER.with(|dispatcher| dispatcher.on_create_context(context_id, root_context_id))
}

#[no_mangle]
pub extern "C" fn proxy_on_log(context_id: u32) {
    DISPATCHER.with(|dispatcher| dispatcher.on_log(context_id))
}

#[no_mangle]
pub extern "C" fn proxy_on_vm_start(context_id: u32, vm_configuration_size: usize) -> bool {
    DISPATCHER.with(|dispatcher| dispatcher.on_vm_start(context_id, vm_configuration_size))
}

#[no_mangle]
pub extern "C" fn proxy_on_configure(context_id: u32, plugin_configuration_size: usize) -> bool {
    DISPATCHER.with(|dispatcher| dispatcher.on_configure(context_id, plugin_configuration_size))
}

#[no_mangle]
pub extern "C" fn proxy_on_tick(context_id: u32) {
    DISPATCHER.with(|dispatcher| dispatcher.on_tick(context_id))
}

#[no_mangle]
pub extern "C" fn proxy_on_message_received(context_id: u32, body_size: usize) -> Action {
    DISPATCHER.with(|dispatcher| dispatcher.on_message_received(context_id, body_size))
}
