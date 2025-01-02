use std::fmt::Display;
use std::path::PathBuf;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use indexmap::{IndexMap, IndexSet};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::constellation::ConstellationProgressStream;
use crate::crypto::DID;
use crate::raygun::{Error, Location};

use super::{
    AttachmentEventStream, ConversationImage, Message, MessageEvent, MessageEventStream,
    MessageOptions, MessageReference, MessageStatus, Messages, PinState, ReactionState,
};
use enum_macro::EnumValues;

pub type RoleId = Uuid;
pub type CommunityRoles = IndexMap<RoleId, CommunityRole>;
pub type CommunityPermissions = IndexMap<String, IndexSet<RoleId>>;
pub type CommunityChannelPermissions = IndexMap<String, IndexSet<RoleId>>;

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
    pub fn name(&self) -> &str {
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

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    pub fn target_user(&self) -> Option<&DID> {
        self.target_user.as_ref()
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

#[derive(Default, Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Community {
    id: Uuid,
    name: String,
    description: Option<String>,
    creator: DID,
    created: DateTime<Utc>,
    modified: DateTime<Utc>,
    members: IndexSet<DID>,
    channels: IndexSet<Uuid>,
    roles: IndexSet<RoleId>,
    permissions: CommunityPermissions,
    invites: IndexSet<Uuid>,
}
impl Community {
    pub fn id(&self) -> Uuid {
        self.id
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
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
    pub fn roles(&self) -> &IndexSet<RoleId> {
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
    pub fn set_roles(&mut self, roles: IndexSet<RoleId>) {
        self.roles = roles;
    }
    pub fn set_permissions(&mut self, permissions: CommunityPermissions) {
        self.permissions = permissions;
    }
    pub fn set_invites(&mut self, invites: IndexSet<Uuid>) {
        self.invites = invites;
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn description(&self) -> Option<&str> {
        self.description.as_deref()
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
#[serde(rename_all = "snake_case")]
pub enum CommunityChannelType {
    #[default]
    Standard,
    VoiceEnabled,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, EnumValues, PartialEq, Eq, Hash)]
pub enum CommunityPermission {
    #[serde(rename = "community.visuals.edit_name")]
    EditName,
    #[serde(rename = "community.visuals.edit_description")]
    EditDescription,
    #[serde(rename = "community.visuals.edit_icon")]
    EditIcon,
    #[serde(rename = "community.visuals.edit_banner")]
    EditBanner,

    #[serde(rename = "community.roles.create_roles")]
    CreateRoles,
    #[serde(rename = "community.roles.edit_roles")]
    EditRoles,
    #[serde(rename = "community.roles.delete_roles")]
    DeleteRoles,

    #[serde(rename = "community.roles.grant_roles")]
    GrantRoles,
    #[serde(rename = "community.roles.revoke_roles")]
    RevokeRoles,

    #[serde(rename = "community.permissions.grant_permissions")]
    GrantPermissions,
    #[serde(rename = "community.permissions.revoke_permissions")]
    RevokePermissions,

    #[serde(rename = "community.invites.create_invites")]
    CreateInvites,
    #[serde(rename = "community.invites.edit_invites")]
    EditInvites,
    #[serde(rename = "community.invites.delete_invites")]
    DeleteInvites,

    #[serde(rename = "community.channels.create_channels")]
    CreateChannels,
    #[serde(rename = "community.channels.edit_channels")]
    EditChannels,
    #[serde(rename = "community.channels.delete_channels")]
    DeleteChannels,

    #[serde(rename = "community.members.remove_members")]
    RemoveMembers,

    #[serde(rename = "community.messages.delete_messages")]
    DeleteMessages,
    #[serde(rename = "community.messages.pin_messages")]
    PinMessages,
}

impl Display for CommunityPermission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(self).unwrap())
    }
}

impl CommunityPermission {
    pub fn default_disabled() -> Vec<CommunityPermission> {
        vec![
            CommunityPermission::EditName,
            CommunityPermission::EditDescription,
            CommunityPermission::EditIcon,
            CommunityPermission::EditBanner,
            CommunityPermission::CreateRoles,
            CommunityPermission::EditRoles,
            CommunityPermission::DeleteRoles,
            CommunityPermission::GrantRoles,
            CommunityPermission::RevokeRoles,
            CommunityPermission::GrantPermissions,
            CommunityPermission::RevokePermissions,
            //We don't add CreateInvites permission since by default we leave it unrestricted.
            CommunityPermission::EditInvites,
            CommunityPermission::DeleteInvites,
            CommunityPermission::CreateChannels,
            CommunityPermission::EditChannels,
            CommunityPermission::DeleteChannels,
            CommunityPermission::RemoveMembers,
            CommunityPermission::DeleteMessages,
            CommunityPermission::PinMessages,
        ]
    }

    pub fn sub_permissions(node: &str) -> Vec<CommunityPermission> {
        CommunityPermission::values()
            .iter()
            .filter(|p| p.to_string().starts_with(node))
            .cloned()
            .collect()
    }
}

/// Built-in CommunityChannelPermission for ease of access
#[derive(Debug, Copy, Clone, Serialize, Deserialize, EnumValues, PartialEq, Eq, Hash)]
pub enum CommunityChannelPermission {
    #[serde(rename = "community_channel.view_channel")]
    ViewChannel,
    #[serde(rename = "community_channel.messages.send_messages")]
    SendMessages,
    SendAttachments,
}

impl Display for CommunityChannelPermission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(self).unwrap())
    }
}

impl CommunityChannelPermission {
    pub fn sub_permissions(node: &str) -> Vec<CommunityChannelPermission> {
        CommunityChannelPermission::values()
            .iter()
            .filter(|p| p.to_string().starts_with(node))
            .cloned()
            .collect()
    }
}

#[async_trait::async_trait]
pub trait RayGunCommunity: Sync + Send {
    async fn get_community_stream(
        &mut self,
        _community_id: Uuid,
    ) -> Result<MessageEventStream, Error> {
        Err(Error::Unimplemented)
    }

    async fn create_community(&mut self, _name: &str) -> Result<Community, Error> {
        Err(Error::Unimplemented)
    }
    async fn delete_community(&mut self, _community_id: Uuid) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn get_community(&self, _community_id: Uuid) -> Result<Community, Error> {
        Err(Error::Unimplemented)
    }

    async fn list_communities_joined(&self) -> Result<IndexSet<Uuid>, Error> {
        Err(Error::Unimplemented)
    }
    async fn list_communities_invited_to(&self) -> Result<Vec<(Uuid, CommunityInvite)>, Error> {
        Err(Error::Unimplemented)
    }
    async fn leave_community(&mut self, _community_id: Uuid) -> Result<(), Error> {
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
        &self,
        _community_id: Uuid,
        _invite_id: Uuid,
    ) -> Result<CommunityInvite, Error> {
        Err(Error::Unimplemented)
    }
    async fn request_join_community(&mut self, _community_id: Uuid) -> Result<(), Error> {
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

    async fn create_community_role(
        &mut self,
        _community_id: Uuid,
        _name: &str,
    ) -> Result<CommunityRole, Error> {
        Err(Error::Unimplemented)
    }
    async fn delete_community_role(
        &mut self,
        _community_id: Uuid,
        _role_id: RoleId,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn get_community_role(
        &mut self,
        _community_id: Uuid,
        _role_id: RoleId,
    ) -> Result<CommunityRole, Error> {
        Err(Error::Unimplemented)
    }
    async fn edit_community_role_name(
        &mut self,
        _community_id: Uuid,
        _role_id: RoleId,
        _new_name: String,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn grant_community_role(
        &mut self,
        _community_id: Uuid,
        _role_id: RoleId,
        _user: DID,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    async fn revoke_community_role(
        &mut self,
        _community_id: Uuid,
        _role_id: RoleId,
        _user: DID,
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
        &self,
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
    async fn grant_community_permission<T>(
        &mut self,
        _community_id: Uuid,
        _permission: T,
        _role_id: RoleId,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }
    async fn revoke_community_permission<T>(
        &mut self,
        _community_id: Uuid,
        _permission: T,
        _role_id: RoleId,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }
    async fn grant_community_permission_for_all<T>(
        &mut self,
        _community_id: Uuid,
        _permission: T,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }
    async fn revoke_community_permission_for_all<T>(
        &mut self,
        _community_id: Uuid,
        _permission: T,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }
    async fn has_community_permission<T>(
        &mut self,
        _community_id: Uuid,
        _permission: T,
        _member: DID,
    ) -> Result<bool, Error>
    where
        T: ToString + Send,
    {
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
    async fn grant_community_channel_permission<T>(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _permission: T,
        _role_id: RoleId,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }
    async fn revoke_community_channel_permission<T>(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _permission: T,
        _role_id: RoleId,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }
    async fn grant_community_channel_permission_for_all<T>(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _permission: T,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }
    async fn revoke_community_channel_permission_for_all<T>(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _permission: T,
    ) -> Result<(), Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }
    async fn has_community_channel_permission<T>(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _permission: T,
        _member: DID,
    ) -> Result<bool, Error>
    where
        T: ToString + Send,
    {
        Err(Error::Unimplemented)
    }

    /// Retrieve all messages from a conversation
    async fn get_community_channel_message(
        &self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
    ) -> Result<Message, Error> {
        Err(Error::Unimplemented)
    }
    /// Retrieve all messages from a conversation
    async fn get_community_channel_messages(
        &self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _options: MessageOptions,
    ) -> Result<Messages, Error> {
        Err(Error::Unimplemented)
    }

    /// Get a number of messages in a conversation
    async fn get_community_channel_message_count(
        &self,
        _community_id: Uuid,
        _channel_id: Uuid,
    ) -> Result<usize, Error> {
        Err(Error::Unimplemented)
    }

    /// Retrieve a message reference from a conversation
    async fn get_community_channel_message_reference(
        &self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
    ) -> Result<MessageReference, Error> {
        Err(Error::Unimplemented)
    }

    /// Retrieve all message references from a conversation
    async fn get_community_channel_message_references(
        &self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _options: MessageOptions,
    ) -> Result<BoxStream<'static, MessageReference>, Error> {
        Err(Error::Unimplemented)
    }

    /// Get a status of a message in a conversation
    async fn community_channel_message_status(
        &self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
    ) -> Result<MessageStatus, Error> {
        Err(Error::Unimplemented)
    }

    /// Sends a message to a conversation.
    async fn send_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message: Vec<String>,
    ) -> Result<Uuid, Error> {
        Err(Error::Unimplemented)
    }

    /// Edit an existing message in a conversation.
    async fn edit_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
        _message: Vec<String>,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Reply to a message within a conversation
    async fn reply_to_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
        _message: Vec<String>,
    ) -> Result<Uuid, Error> {
        Err(Error::Unimplemented)
    }

    /// Delete message from a conversation
    async fn delete_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Pin a message within a conversation
    async fn pin_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
        _state: PinState,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// React to a message
    async fn react_to_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
        _state: ReactionState,
        _emoji: String,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Send an event to a conversation
    async fn send_community_channel_messsage_event(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _event: MessageEvent,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Cancel event that was sent, if any.
    async fn cancel_community_channel_messsage_event(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _event: MessageEvent,
    ) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
    /// Send files to a conversation.
    /// If no files is provided in the array, it will throw an error
    async fn attach_to_community_channel_message(
        &mut self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Option<Uuid>,
        _locations: Vec<Location>,
        _message: Vec<String>,
    ) -> Result<(Uuid, AttachmentEventStream), Error> {
        Err(Error::Unimplemented)
    }
    /// Downloads a file that been attached to a message
    /// Note: Must use the filename associated when downloading
    async fn download_from_community_channel_message(
        &self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
        _file: String,
        _path: PathBuf,
    ) -> Result<ConstellationProgressStream, Error> {
        Err(Error::Unimplemented)
    }
    /// Stream a file that been attached to a message
    /// Note: Must use the filename associated when downloading
    async fn download_stream_from_community_channel_message(
        &self,
        _community_id: Uuid,
        _channel_id: Uuid,
        _message_id: Uuid,
        _file: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, std::io::Error>>, Error> {
        Err(Error::Unimplemented)
    }
}
