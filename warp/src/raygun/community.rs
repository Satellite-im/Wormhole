use chrono::{DateTime, Utc};
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::crypto::DID;
use crate::raygun::{Error, Location};

use super::ConversationImage;

pub type RoleId = Uuid;
pub type CommunityRoles = IndexMap<RoleId, CommunityRole>;
pub type CommunityPermissions = IndexMap<CommunityPermission, IndexSet<RoleId>>;
pub type CommunityChannelPermissions = IndexMap<CommunityChannelPermission, IndexSet<RoleId>>;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommunityRole {
    id: RoleId,
    name: String,
    members: IndexSet<DID>,
}
impl CommunityRole {
    pub fn id(&self) -> RoleId {
        self.id
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn members(&self) -> &IndexSet<DID> {
        &self.members
    }
}
impl CommunityRole {
    pub fn set_id(&mut self, id: RoleId) {
        self.id = id;
    }
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }
    pub fn set_members(&mut self, members: IndexSet<DID>) {
        self.members = members;
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct CommunityInvite {
    id: Uuid,
    target_user: Option<DID>,
    created: DateTime<Utc>,
    expiry: Option<DateTime<Utc>>,
}
impl CommunityInvite {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn target_user(&self) -> &Option<DID> {
        &self.target_user
    }
    pub fn created(&self) -> DateTime<Utc> {
        self.created
    }
    pub fn expiry(&self) -> Option<DateTime<Utc>> {
        self.expiry
    }
}
impl CommunityInvite {
    pub fn set_id(&mut self, id: Uuid) {
        self.id = id;
    }
    pub fn set_target_user(&mut self, target_user: Option<DID>) {
        self.target_user = target_user;
    }
    pub fn set_created(&mut self, created: DateTime<Utc>) {
        self.created = created;
    }
    pub fn set_expiry(&mut self, expiry: Option<DateTime<Utc>>) {
        self.expiry = expiry;
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct Community {
    id: Uuid,
    name: String,
    description: Option<String>,
    creator: DID,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    members: IndexSet<DID>,
    channels: IndexSet<Uuid>,
    roles: CommunityRoles,
    permissions: CommunityPermissions,
    invites: IndexSet<Uuid>,
}
impl Community {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn description(&self) -> &Option<String> {
        &self.description
    }
    pub fn creator(&self) -> &DID {
        &self.creator
    }
    pub fn created(&self) -> DateTime<Utc> {
        self.created
    }
    pub fn modified(&self) -> DateTime<Utc> {
        self.modified
    }
    pub fn members(&self) -> &IndexSet<DID> {
        &self.members
    }
    pub fn channels(&self) -> &IndexSet<Uuid> {
        &self.channels
    }
    pub fn roles(&self) -> &CommunityRoles {
        &self.roles
    }
    pub fn permissions(&self) -> &CommunityPermissions {
        &self.permissions
    }
    pub fn invites(&self) -> &IndexSet<Uuid> {
        &self.invites
    }
}
impl Community {
    pub fn set_id(&mut self, id: Uuid) {
        self.id = id;
    }
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }
    pub fn set_description(&mut self, description: Option<String>) {
        self.description = description;
    }
    pub fn set_creator(&mut self, creator: DID) {
        self.creator = creator;
    }
    pub fn set_created(&mut self, created: DateTime<Utc>) {
        self.created = created;
    }
    pub fn set_modified(&mut self, modified: DateTime<Utc>) {
        self.modified = modified;
    }
    pub fn set_members(&mut self, members: IndexSet<DID>) {
        self.members = members;
    }
    pub fn set_channels(&mut self, channels: IndexSet<Uuid>) {
        self.channels = channels;
    }
    pub fn set_roles(&mut self, roles: CommunityRoles) {
        self.roles = roles;
    }
    pub fn set_permissions(&mut self, permissions: CommunityPermissions) {
        self.permissions = permissions;
    }
    pub fn set_invites(&mut self, invites: IndexSet<Uuid>) {
        self.invites = invites;
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct CommunityChannel {
    id: Uuid,
    name: String,
    description: Option<String>,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    channel_type: CommunityChannelType,
    permissions: CommunityChannelPermissions,
}

impl CommunityChannel {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn name(&self) -> &String {
        &self.name
    }
    pub fn description(&self) -> &Option<String> {
        &self.description
    }
    pub fn created(&self) -> DateTime<Utc> {
        self.created
    }
    pub fn modified(&self) -> DateTime<Utc> {
        self.modified
    }
    pub fn channel_type(&self) -> CommunityChannelType {
        self.channel_type
    }
    pub fn permissions(&self) -> &CommunityChannelPermissions {
        &self.permissions
    }
}
impl CommunityChannel {
    pub fn set_id(&mut self, id: Uuid) {
        self.id = id;
    }
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }
    pub fn set_description(&mut self, description: Option<String>) {
        self.description = description;
    }
    pub fn set_created(&mut self, created: DateTime<Utc>) {
        self.created = created;
    }
    pub fn set_modified(&mut self, modified: DateTime<Utc>) {
        self.modified = modified;
    }
    pub fn set_channel_type(&mut self, channel_type: CommunityChannelType) {
        self.channel_type = channel_type;
    }
    pub fn set_permissions(&mut self, permissions: CommunityChannelPermissions) {
        self.permissions = permissions;
    }
}

#[derive(Default, Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CommunityChannelType {
    #[default]
    Standard,
    VoiceEnabled,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CommunityPermission {
    EditName,
    EditDescription,
    ManageRoles,
    ManagePermissions,
    ManageMembers,
    ManageChannels,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum CommunityChannelPermission {
    ViewChannel,
    EditName,
    EditDescription,
    SendMessages,
    DeleteMessages,
}

#[async_trait::async_trait]
pub trait RayGunCommunity: Sync + Send {
    async fn create_community(&mut self, _name: &str) -> Result<Community, Error> {
        Err(Error::Unimplemented)
    }
    async fn delete_community(&mut self, _community_id: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn get_community(&mut self, _community_id: Uuid) -> Result<Community, Error> {
        Err(Error::Unimplemented)
    }

    async fn get_community_icon(&self, _community_id: Uuid) -> Result<ConversationImage, Error> {
        Err(Error::Unimplemented)
    }
    async fn get_community_banner(&self, _community_id: Uuid) -> Result<ConversationImage, Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_icon(
        &mut self,
        _community_id: Uuid,
        _location: Location,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_banner(
        &mut self,
        _community_id: Uuid,
        _location: Location,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    async fn create_community_invite(
        &mut self,
        _community_id: Uuid,
        _target_user: Option<DID>,
        _expiry: Option<DateTime<Utc>>,
    ) -> Result<CommunityInvite, Error> {
        Err(Error::Unimplemented)
    }
    async fn delete_community_invite(
        &mut self,
        _community_id: Uuid,
        _invite_id: Uuid,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn get_community_invite(
        &mut self,
        _community_id: Uuid,
        _invite_id: Uuid,
    ) -> Result<CommunityInvite, Error> {
        Err(Error::Unimplemented)
    }
    async fn accept_community_invite(
        &mut self,
        _community_id: Uuid,
        _invite_id: Uuid,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_invite(
        &mut self,
        _community_id: Uuid,
        _invite_id: Uuid,
        _invite: CommunityInvite,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    async fn create_community_channel(
        &mut self,
        _community_id: Uuid,
        _channel_name: &str,
        _channel_type: CommunityChannelType,
    ) -> Result<CommunityChannel, Error> {
        Err(Error::Unimplemented)
    }
    async fn delete_community_channel(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn get_community_channel(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
    ) -> Result<CommunityChannel, Error> {
        Err(Error::Unimplemented)
    }

    async fn edit_community_name(&mut self, _community_id: Uuid, _name: &str) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_description(
        &mut self,
        _community_id: Uuid,
        _description: Option<String>,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_roles(
        &mut self,
        _community_id: Uuid,
        _roles: CommunityRoles,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_permissions(
        &mut self,
        _community_id: Uuid,
        _permissions: CommunityPermissions,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn remove_community_member(
        &mut self,
        _community_id: Uuid,
        _member: DID,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    async fn edit_community_channel_name(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _name: &str,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_channel_description(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _description: Option<String>,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_channel_permissions(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _permissions: CommunityChannelPermissions,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn send_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message: &str,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn delete_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
}
