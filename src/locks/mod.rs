use std::sync::{RwLockReadGuard, RwLockWriteGuard};
use serde::Deserialize;

pub mod memory;

#[derive(Debug, Deserialize, Clone)]
pub(crate) enum LocksType {
    #[serde(rename = "memory")]
    Memory,
}

pub(crate) trait Locks {
    fn new() -> Box<Self>
    where
        Self: Sized;

    fn acquire_shared(&mut self, key: &str);
    fn acquire_exclusive(&mut self, key: &str);
     fn release(&mut self, key: &str) -> bool;
}