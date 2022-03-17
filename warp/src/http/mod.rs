use std::sync::{Arc, Mutex};
use warp::{PocketDimension};
use warp_common::anyhow;

mod hconstellation;

pub struct CacheSystem(Arc<Mutex<Box<dyn PocketDimension>>>);

impl AsRef<Arc<Mutex<Box<dyn PocketDimension>>>> for CacheSystem {
    fn as_ref(&self) -> &Arc<Mutex<Box<dyn PocketDimension>>> {
        &self.0
    }
}

#[allow(unused_imports)]
use rocket::{
    self, catch, catchers,
    data::{Data, ToByteUnit},
    get,
    http::Status,
    response::{content, status},
    routes,
    serde::json::{json, Json, Value},
    Build, Request, Rocket, State,
};

use crate::manager::ModuleManager;

#[get("/")]
fn index() -> String {
    String::from("Hello, World!")
}

pub async fn http_main(manage: &ModuleManager) -> anyhow::Result<()> {
    //TODO: This is temporary as things are setup
    let fs = manage.get_filesystem()?;
    let cache = manage.get_cache()?;
    fs.lock()
        .unwrap()
        .from_buffer("Cargo.toml", &include_bytes!("../../Cargo.toml").to_vec())
        .await?;

    fs.lock()
        .unwrap()
        .from_buffer("lib.rs", &include_bytes!("../lib.rs").to_vec())
        .await?;

    rocket::build()
        .mount(
            "/v1",
            routes![
                index,
                hconstellation::export,
            ]
        )
        .manage(hconstellation::FsSystem(fs.clone()))
        .manage(CacheSystem(cache.clone()))
        .launch()
        .await?;
    Ok(())
}
