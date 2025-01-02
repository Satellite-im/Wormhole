#![allow(clippy::result_large_err)]
use crate::crypto::DID;
use crate::error::Error;
use macro_utils::impl_funcs;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Clone, Copy, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub enum GroupStatus {
    Opened,
    Closed,
}

impl Default for GroupStatus {
    fn default() -> Self {
        Self::Closed
    }
}

#[derive(Default, Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct Member {
    member: DID,
    //permission: Permission
}

#[derive(Default, Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct Group {
    id: Uuid,
    name: String,
    owner: DID,
    admin: Vec<Member>,
    members: Vec<Member>,
    banned: Vec<DID>,
    limit: u64,
    status: GroupStatus,
}

impl Group {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn owner(&self) -> DID {
        self.owner.clone()
    }

    pub fn admin(&self) -> Vec<Member> {
        self.admin.clone()
    }

    pub fn members(&self) -> Vec<Member> {
        self.members.clone()
    }

    pub fn banned(&self) -> Vec<DID> {
        self.banned.clone()
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn status(&self) -> GroupStatus {
        self.status
    }
}

impl Group {
    pub fn set_id(&mut self, id: Uuid) {
        self.id = id;
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }

    pub fn set_owner(&mut self, owner: DID) {
        self.owner = owner;
    }

    pub fn set_admin(&mut self, admin: Vec<Member>) {
        self.admin = admin;
    }

    pub fn set_members(&mut self, members: Vec<Member>) {
        self.members = members;
    }

    pub fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
    }

    pub fn set_status(&mut self, status: GroupStatus) {
        self.status = status;
    }
}

#[derive(Default, Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct GroupInvitation {
    id: Uuid,
    group: Uuid,
    sender: DID,
    recipient: DID,
    #[serde(flatten)]
    metadata: HashMap<String, serde_json::Value>,
}

impl GroupInvitation {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn group(&self) -> Uuid {
        self.group
    }

    pub fn sender(&self) -> DID {
        self.sender.clone()
    }

    pub fn recipient(&self) -> DID {
        self.recipient.clone()
    }

    pub fn metadata(&self) -> HashMap<String, serde_json::Value> {
        self.metadata.clone()
    }
}

impl GroupInvitation {
    pub fn set_id(&mut self, id: Uuid) {
        self.id = id;
    }

    pub fn set_group(&mut self, group: Uuid) {
        self.group = group;
    }

    pub fn set_sender(&mut self, sender: DID) {
        self.sender = sender;
    }

    pub fn set_recipient(&mut self, recipient: DID) {
        self.recipient = recipient;
    }

    pub fn set_metadata(&mut self, metadata: HashMap<String, serde_json::Value>) {
        self.metadata = metadata
    }
}

// General/Base GroupChat Trait
#[impl_funcs(name = "raygun_group_chat_impls")]
pub trait GroupChat: GroupInvite + GroupChatManagement {
    /// Join a existing group
    fn join_group(&mut self, _: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Leave a group
    fn leave_group(&mut self, _: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// List members of group
    fn list_members(&self, _: Uuid) -> Result<Vec<Member>, Error> {
        Err(Error::Unimplemented)
    }
}

// Group Invite Management Trait
#[impl_funcs(name = "raygun_group_invite_impls")]
pub trait GroupInvite {
    /// Sends a invite to join a group
    fn send_invite(&mut self, _: Uuid, _: DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Accepts an invite to a group
    fn accept_invite(&mut self, _: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Dent an invite to a group
    fn deny_invite(&mut self, _: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Block invitations to a group
    fn block_group(&mut self, _: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
}

// Group Admin Management Trait
#[impl_funcs(name = "raygun_group_management_impls")]
pub trait GroupChatManagement {
    /// Create a group
    fn create_group(&mut self, _: &str) -> Result<Group, Error> {
        Err(Error::Unimplemented)
    }

    /// Change group name
    fn change_group_name(&mut self, _: Uuid, _: &str) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Open group for invites
    fn open_group(&mut self, _: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Close group for invites
    fn close_group(&mut self, _: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Change the administrator of the group
    fn change_admin(&mut self, _: Uuid, _: Member) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Assign an administrator to the group
    fn assign_admin(&mut self, _: Uuid, _: Member) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Kick member from group
    fn kick_member(&mut self, _: Uuid, _: DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Ban member from group
    fn ban_member(&mut self, _: Uuid, _: DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
}
