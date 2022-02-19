pub mod error;

use warp_module::Module;

use crate::error::Error;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub type DataObject = Data;

/// Standard DataObject used throughout warp.
/// Unifies output from all modules
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Data {
    pub id: Uuid,
    pub version: i32,
    pub timestamp: DateTime<Utc>,
    pub size: u64,
    pub module: Module,
    pub payload: Value,
}

impl Default for Data {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4(),
            version: 0,
            timestamp: Utc::now(),
            size: 0,
            module: Module::default(),
            payload: Value::Null,
        }
    }
}

impl Data {
    pub fn new<T>(module: &Module, payload: T) -> Result<Self, Error>
    where
        T: Serialize,
    {
        let module = module.clone();
        let payload = serde_json::to_value(payload)?;
        Ok(Data {
            module,
            payload,
            ..Default::default()
        })
    }

    pub fn payload<T>(&self) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_value(self.payload.clone()).map_err(Error::from)
    }

    pub fn timestamp(&self) -> i64 {
        self.timestamp.timestamp()
    }
}
