use super::{
    conversation::message::MessageDocument, keystore::Keystore, topics::ConversationTopic,
    PeerIdExt,
};
use crate::store::conversation::reference::MessageReferenceList;
use crate::store::DidExt;
use chrono::{DateTime, Utc};
use core::hash::Hash;
use either::Either;
use futures::{
    stream::{self, BoxStream},
    StreamExt, TryFutureExt,
};
use indexmap::{IndexMap, IndexSet};
use ipld_core::cid::Cid;
use rust_ipfs::{Ipfs, Keypair};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};
use uuid::Uuid;
use warp::{
    crypto::DID,
    error::Error,
    raygun::{
        community::{
            Community, CommunityChannel, CommunityChannelPermissions, CommunityChannelType,
            CommunityInvite, CommunityPermission, CommunityPermissions, CommunityRole, RoleId,
        },
        Message, MessageOptions, MessagePage, MessageReference, Messages, MessagesType,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommunityRoleDocument {
    pub id: RoleId,
    pub name: String,
    pub members: IndexSet<DID>,
}
impl CommunityRoleDocument {
    pub fn new(name: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            members: IndexSet::new(),
        }
    }
}
impl From<CommunityRoleDocument> for CommunityRole {
    fn from(value: CommunityRoleDocument) -> Self {
        let mut role = CommunityRole::default();
        role.set_id(value.id);
        role.set_name(value.name);
        role.set_members(value.members);
        role
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommunityInviteDocument {
    pub id: Uuid,
    pub target_user: Option<DID>,
    pub created: DateTime<Utc>,
    pub expiry: Option<DateTime<Utc>>,
}
impl CommunityInviteDocument {
    pub fn new(target_user: Option<DID>, expiry: Option<DateTime<Utc>>) -> Self {
        Self {
            id: Uuid::new_v4(),
            target_user,
            created: Utc::now(),
            expiry,
        }
    }
}
impl From<CommunityInviteDocument> for CommunityInvite {
    fn from(value: CommunityInviteDocument) -> Self {
        let mut community_invite = CommunityInvite::default();
        community_invite.set_id(value.id);
        community_invite.set_target_user(value.target_user);
        community_invite.set_created(value.created);
        community_invite.set_expiry(value.expiry);
        community_invite
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq)]
pub struct CommunityDocument {
    pub id: Uuid,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub owner: DID,
    pub created: DateTime<Utc>,
    pub modified: DateTime<Utc>,
    pub members: IndexSet<DID>,
    pub channels: IndexMap<String, CommunityChannelDocument>,
    pub roles: IndexMap<String, CommunityRoleDocument>,
    pub permissions: CommunityPermissions,
    pub invites: IndexMap<String, CommunityInviteDocument>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<Cid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub banner: Option<Cid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl Hash for CommunityDocument {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl PartialEq for CommunityDocument {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl CommunityDocument {
    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn topic(&self) -> String {
        self.id.base()
    }

    pub fn event_topic(&self) -> String {
        self.id.event_topic()
    }

    pub fn exchange_topic(&self, did: &DID) -> String {
        self.id.exchange_topic(did)
    }

    pub fn join_topic(&self) -> String {
        self.id.join_topic()
    }

    pub fn sign(&mut self, keypair: &Keypair) -> Result<(), Error> {
        let construct = warp::crypto::hash::sha256_iter(
            [
                Some(self.id().into_bytes().to_vec()),
                Some(self.owner.to_string().into_bytes()),
                Some(self.created.to_string().into_bytes()),
            ]
            .into_iter(),
            None,
        );

        let signature = keypair.sign(&construct).expect("not RSA");
        self.signature = Some(bs58::encode(signature).into_string());

        Ok(())
    }

    pub fn verify(&self) -> Result<(), Error> {
        let creator_pk = self.owner.to_public_key()?;

        let Some(signature) = &self.signature else {
            return Err(Error::InvalidSignature);
        };

        let signature = bs58::decode(signature).into_vec()?;

        let construct = warp::crypto::hash::sha256_iter(
            [
                Some(self.id().into_bytes().to_vec()),
                Some(self.owner.to_string().into_bytes()),
                Some(self.created.to_string().into_bytes()),
            ]
            .into_iter(),
            None,
        );

        if !creator_pk.verify(&construct, &signature) {
            return Err(Error::InvalidSignature);
        }
        Ok(())
    }
}
impl CommunityDocument {
    pub fn new(keypair: &Keypair, name: String) -> Result<Self, Error> {
        let creator = keypair.to_did()?;

        let mut permissions = CommunityPermissions::new();
        for permission in CommunityPermission::default_disabled() {
            permissions.insert(permission.to_string(), IndexSet::new());
        }

        let mut members = IndexSet::new();
        members.insert(creator.clone());

        let mut document = Self {
            id: Uuid::new_v4(),
            name,
            description: None,
            owner: creator,
            created: Utc::now(),
            modified: Utc::now(),
            members,
            channels: IndexMap::new(),
            roles: IndexMap::new(),
            permissions,
            invites: IndexMap::new(),
            deleted: false,
            icon: None,
            banner: None,
            signature: None,
        };
        document.sign(keypair)?;
        Ok(document)
    }
}
impl From<CommunityDocument> for Community {
    fn from(value: CommunityDocument) -> Self {
        let mut community = Community::default();
        community.set_id(value.id);
        community.set_name(value.name);
        community.set_description(value.description);
        community.set_creator(value.owner);
        community.set_created(value.created);
        community.set_modified(value.modified);
        community.set_members(value.members);
        community.set_channels(
            value
                .channels
                .iter()
                .map(|(k, _)| RoleId::parse_str(k).expect("should be valid uuid"))
                .collect(),
        );
        community.set_roles(
            value
                .roles
                .iter()
                .map(|(k, _)| RoleId::parse_str(k).expect("should be valid uuid"))
                .collect(),
        );
        community.set_permissions(value.permissions);
        community.set_invites(
            value
                .invites
                .iter()
                .map(|(k, _)| RoleId::parse_str(k).expect("should be valid uuid"))
                .collect(),
        );
        community
    }
}
impl CommunityDocument {
    pub fn participants(&self) -> IndexSet<DID> {
        let mut participants = self.members.clone();
        participants.insert(self.owner.clone());
        participants
    }
    pub fn has_valid_invite(&self, user: &DID) -> bool {
        for (_, invite) in &self.invites {
            let is_expired = match &invite.expiry {
                Some(expiry) => expiry < &Utc::now(),
                None => false,
            };
            let is_valid_target = match &invite.target_user {
                Some(target) => user == target,
                None => true,
            };
            if !is_expired && is_valid_target {
                return true;
            }
        }
        false
    }
    pub fn has_permission<T>(&self, user: &DID, has_permission: &T) -> bool
    where
        T: ToString,
    {
        if &self.owner == user {
            return true;
        }
        if !self.members.contains(user) {
            return false;
        }
        let Some(authorized_roles) = self.permissions.get(&has_permission.to_string()) else {
            return true;
        };
        for authorized_role in authorized_roles {
            if let Some(role) = self.roles.get(&authorized_role.to_string()) {
                if role.members.contains(user) {
                    return true;
                }
            }
        }
        false
    }
    pub fn has_channel_permission<T>(
        &self,
        user: &DID,
        has_permission: &T,
        channel_id: Uuid,
    ) -> bool
    where
        T: ToString,
    {
        if &self.owner == user {
            return true;
        }
        if !self.members.contains(user) {
            return false;
        }
        let Some(channel) = self.channels.get(&channel_id.to_string()) else {
            return false;
        };
        let Some(authorized_roles) = channel.permissions.get(&has_permission.to_string()) else {
            return true;
        };
        for authorized_role in authorized_roles {
            if let Some(role) = self.roles.get(&authorized_role.to_string()) {
                if role.members.contains(user) {
                    return true;
                }
            }
        }
        false
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommunityChannelDocument {
    pub id: Uuid,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub created: DateTime<Utc>,
    pub modified: DateTime<Utc>,
    pub channel_type: CommunityChannelType,
    pub permissions: CommunityChannelPermissions,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub messages: Option<Cid>,
}
impl CommunityChannelDocument {
    pub fn new(
        name: String,
        description: Option<String>,
        channel_type: CommunityChannelType,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            description,
            created: Utc::now(),
            modified: Utc::now(),
            channel_type,
            permissions: CommunityChannelPermissions::new(),
            messages: None,
        }
    }
}
impl CommunityChannelDocument {
    pub async fn message_reference_list(&self, ipfs: &Ipfs) -> Result<MessageReferenceList, Error> {
        let refs = match self.messages {
            Some(cid) => {
                ipfs.get_dag(cid)
                    .timeout(Duration::from_secs(10))
                    .deserialized()
                    .await?
            }
            None => MessageReferenceList::default(),
        };

        Ok(refs)
    }

    pub async fn contains(&self, ipfs: &Ipfs, message_id: Uuid) -> Result<bool, Error> {
        let list = self.message_reference_list(ipfs).await?;
        Ok(list.contains(ipfs, message_id).await)
    }

    pub async fn set_message_reference_list(
        &mut self,
        ipfs: &Ipfs,
        list: MessageReferenceList,
    ) -> Result<(), Error> {
        self.modified = Utc::now();
        let next_cid = ipfs.put_dag(list).await?;
        self.messages.replace(next_cid);
        Ok(())
    }

    pub async fn insert_message_document(
        &mut self,
        ipfs: &Ipfs,
        message_document: &MessageDocument,
    ) -> Result<Cid, Error> {
        let mut list = self.message_reference_list(ipfs).await?;
        let cid = list.insert(ipfs, message_document).await?;
        self.set_message_reference_list(ipfs, list).await?;
        Ok(cid)
    }

    pub async fn update_message_document(
        &mut self,
        ipfs: &Ipfs,
        message_document: &MessageDocument,
    ) -> Result<Cid, Error> {
        let mut list = self.message_reference_list(ipfs).await?;
        let cid = list.update(ipfs, message_document).await?;
        self.set_message_reference_list(ipfs, list).await?;
        Ok(cid)
    }

    pub async fn messages_length(&self, ipfs: &Ipfs) -> Result<usize, Error> {
        let list = self.message_reference_list(ipfs).await?;
        Ok(list.count(ipfs).await)
    }

    pub async fn get_message_list(&self, ipfs: &Ipfs) -> Result<BTreeSet<MessageDocument>, Error> {
        let refs = self.message_reference_list(ipfs).await?;
        let list = refs.list(ipfs).collect::<BTreeSet<_>>().await;
        Ok(list)
    }

    pub async fn get_messages(
        &self,
        ipfs: &Ipfs,
        keypair: &Keypair,
        option: MessageOptions,
        keystore: Either<DID, Keystore>,
    ) -> Result<Vec<Message>, Error> {
        let list = self
            .get_messages_stream(ipfs, keypair, option, keystore)
            .await?
            .collect::<Vec<_>>()
            .await;
        Ok(list)
    }

    pub async fn get_messages_reference_stream<'a>(
        &self,
        ipfs: &Ipfs,
        option: MessageOptions,
    ) -> Result<BoxStream<'a, MessageReference>, Error> {
        let message_list = self.get_message_list(ipfs).await?;

        if message_list.is_empty() {
            return Ok(stream::empty().boxed());
        }

        let mut messages = Vec::from_iter(message_list);

        if option.reverse() {
            messages.reverse()
        }

        if option.first_message() && !messages.is_empty() {
            let message = messages.first().cloned().ok_or(Error::MessageNotFound)?;
            return Ok(stream::once(async move { message.into() }).boxed());
        }

        if option.last_message() && !messages.is_empty() {
            let message = messages.last().cloned().ok_or(Error::MessageNotFound)?;
            return Ok(stream::once(async move { message.into() }).boxed());
        }

        let stream = async_stream::stream! {
            let mut remaining = option.limit();
            for (index, document) in messages.iter().enumerate() {
                if remaining.as_ref().map(|x| *x == 0).unwrap_or_default() {
                    break;
                }
                if let Some(range) = option.range() {
                    if range.start > index || range.end < index {
                        continue
                    }
                }
                if let Some(range) = option.date_range() {
                    if !(document.date >= range.start && document.date <= range.end) {
                        continue
                    }
                }

                if option.pinned() && !document.pinned {
                    continue;
                }

                if let Some(remaining) = remaining.as_mut() {
                    *remaining = remaining.saturating_sub(1);
                }

                yield document.into()
            }
        };

        Ok(stream.boxed())
    }

    pub async fn get_messages_stream<'a>(
        &self,
        ipfs: &Ipfs,
        keypair: &Keypair,
        option: MessageOptions,
        keystore: Either<DID, Keystore>,
    ) -> Result<BoxStream<'a, Message>, Error> {
        let message_list = self.get_message_list(ipfs).await?;

        if message_list.is_empty() {
            return Ok(stream::empty().boxed());
        }

        let mut messages = Vec::from_iter(message_list);

        if option.reverse() {
            messages.reverse()
        }

        if option.first_message() && !messages.is_empty() {
            let message = messages
                .first()
                .ok_or(Error::MessageNotFound)?
                .resolve(ipfs, keypair, true, keystore.as_ref())
                .await?;
            return Ok(stream::once(async { message }).boxed());
        }

        if option.last_message() && !messages.is_empty() {
            let message = messages
                .last()
                .ok_or(Error::MessageNotFound)?
                .resolve(ipfs, keypair, true, keystore.as_ref())
                .await?;
            return Ok(stream::once(async { message }).boxed());
        }
        let keystore = keystore.clone();
        let ipfs = ipfs.clone();
        let keypair = keypair.clone();
        let stream = async_stream::stream! {
            let mut remaining = option.limit();
            for (index, document) in messages.iter().enumerate() {
                if remaining.as_ref().map(|x| *x == 0).unwrap_or_default() {
                    break;
                }
                if let Some(range) = option.range() {
                    if range.start > index || range.end < index {
                        continue
                    }
                }
                if let Some(range) = option.date_range() {
                    if !(document.date >= range.start && document.date <= range.end) {
                        continue
                    }
                }

                if option.pinned() && !document.pinned {
                    continue;
                }

                if let Ok(message) = document.resolve(&ipfs, &keypair, true, keystore.as_ref()).await {
                    let should_yield = if let Some(keyword) = option.keyword() {
                         message
                            .lines()
                            .iter()
                            .any(|line| line.to_lowercase().contains(&keyword.to_lowercase()))
                    } else {
                        true
                    };
                    if should_yield {
                        if let Some(remaining) = remaining.as_mut() {
                            *remaining = remaining.saturating_sub(1);
                        }
                        yield message;
                    }
                }
            }
        };

        Ok(stream.boxed())
    }

    pub async fn get_messages_pages(
        &self,
        ipfs: &Ipfs,
        did: &Keypair,
        option: MessageOptions,
        keystore: Either<&DID, &Keystore>,
    ) -> Result<Messages, Error> {
        let message_list = self.get_message_list(ipfs).await?;

        if message_list.is_empty() {
            return Ok(Messages::Page {
                pages: vec![],
                total: 0,
            });
        }

        let mut messages = Vec::from_iter(message_list);

        if option.reverse() {
            messages.reverse()
        }

        let (page_index, amount_per_page) = match option.messages_type() {
            MessagesType::Pages {
                page,
                amount_per_page,
            } => (
                page,
                amount_per_page
                    .map(|amount| if amount == 0 { u8::MAX as _ } else { amount })
                    .unwrap_or(u8::MAX as _),
            ),
            _ => (None, u8::MAX as _),
        };

        let messages_chunk = messages.chunks(amount_per_page as _).collect::<Vec<_>>();
        let mut pages = vec![];
        // First check to determine if there is a page that was selected
        if let Some(index) = page_index {
            let page = messages_chunk.get(index).ok_or(Error::PageNotFound)?;
            let mut messages = vec![];
            for document in page.iter() {
                if let Ok(message) = document.resolve(ipfs, did, true, keystore).await {
                    messages.push(message);
                }
            }
            let total = messages.len();
            pages.push(MessagePage::new(index, messages, total));
            return Ok(Messages::Page { pages, total: 1 });
        }

        for (index, chunk) in messages_chunk.iter().enumerate() {
            let mut messages = vec![];
            for document in chunk.iter() {
                if let Ok(message) = document.resolve(ipfs, did, true, keystore).await {
                    if option.pinned() && !message.pinned() {
                        continue;
                    }
                    messages.push(message);
                }
            }

            let total = messages.len();
            pages.push(MessagePage::new(index, messages, total));
        }

        let total = pages.len();

        Ok(Messages::Page { pages, total })
    }

    pub async fn get_message_document(
        &self,
        ipfs: &Ipfs,
        message_id: Uuid,
    ) -> Result<MessageDocument, Error> {
        let refs = self.message_reference_list(ipfs).await?;
        refs.get(ipfs, message_id).await
    }

    pub async fn get_message(
        &self,
        ipfs: &Ipfs,
        keypair: &Keypair,
        message_id: Uuid,
        keystore: Either<&DID, &Keystore>,
    ) -> Result<Message, Error> {
        self.get_message_document(ipfs, message_id)
            .and_then(|doc| async move { doc.resolve(ipfs, keypair, true, keystore).await })
            .await
    }

    pub async fn delete_message(&mut self, ipfs: &Ipfs, message_id: Uuid) -> Result<(), Error> {
        let mut list = self.message_reference_list(ipfs).await?;
        list.remove(ipfs, message_id).await?;
        self.set_message_reference_list(ipfs, list).await?;
        Ok(())
    }
}
impl From<CommunityChannelDocument> for CommunityChannel {
    fn from(value: CommunityChannelDocument) -> Self {
        let mut community_channel = CommunityChannel::default();
        community_channel.set_id(value.id);
        community_channel.set_name(value.name);
        community_channel.set_description(value.description);
        community_channel.set_created(value.created);
        community_channel.set_modified(value.modified);
        community_channel.set_channel_type(value.channel_type);
        community_channel.set_permissions(value.permissions);
        community_channel
    }
}
