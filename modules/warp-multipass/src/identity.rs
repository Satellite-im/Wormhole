use std::collections::HashMap;
use warp_common::serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "warp_common::serde")]
pub struct Role {
    /// Name of the role
    pub name: String,

    /// TBD
    pub level: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "warp_common::serde")]
pub struct Badge {
    /// TBD
    pub name: String,

    /// TBD
    pub icon: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "warp_common::serde")]
pub struct Graphics {
    /// Hash to profile picture
    pub profile_picture: String,

    /// Hash to profile banner
    pub profile_banner: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "warp_common::serde")]
pub struct Identity {
    /// Username of the identity
    pub username: String,

    /// Short 4-digit numeric id to be used along side `Identity::username` (eg `Username#0000`)
    pub short_id: u16,

    /// Public key for the identity
    pub public_key: PublicKey,

    /// TBD
    pub graphics: Graphics,

    /// Status message
    pub status_message: Option<String>,

    /// List of roles
    pub roles: Vec<Role>,

    /// List of available badges
    pub available_badges: Vec<Badge>,

    /// Active badge for identity
    pub active_badge: Badge,

    /// TBD
    pub linked_accounts: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "warp_common::serde")]
pub struct PublicKey(Vec<u8>);

#[derive(Debug, Clone)]
pub enum Identifier {
    /// Select identity based on public key
    PublicKey(PublicKey),

    /// Select identity based on Username (eg `Username#0000`)
    Username(String),

    /// Select own identity.
    Own,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(crate = "warp_common::serde")]
pub enum Username {
    Full(String),
    Format(String, u16),
}

impl Username {
    pub fn valid(&self) -> bool {
        match self {
            Username::Full(..) => true,
            Username::Format(..) => true,
        }
    }
}

impl From<PublicKey> for Identifier {
    fn from(pubkey: PublicKey) -> Self {
        Identifier::PublicKey(pubkey)
    }
}

impl<S: AsRef<str>> From<S> for Identifier {
    fn from(username: S) -> Self {
        Identifier::Username(username.as_ref().to_string())
    }
}

#[derive(Debug, Clone)]
pub enum IdentityUpdate {
    /// Update Username
    Username(String),

    /// Update graphics
    Graphics(Graphics),

    /// Update status message
    StatusMessage(String),
}
