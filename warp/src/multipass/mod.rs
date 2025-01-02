#![allow(clippy::result_large_err)]

use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use macro_utils::impl_funcs;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use identity::Identity;

use crate::crypto::DID;
use crate::error::Error;
use crate::multipass::identity::{FriendRequest, Identifier, IdentityUpdate};
use crate::tesseract::Tesseract;
use crate::{Extension, SingleHandle};

use self::identity::{IdentityImage, IdentityProfile, IdentityStatus, Platform, Relationship};

pub mod generator;
pub mod identity;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum MultiPassEventKind {
    FriendRequestReceived { from: DID, date: DateTime<Utc> },
    FriendRequestSent { to: DID, date: DateTime<Utc> },
    IncomingFriendRequestRejected { did: DID },
    OutgoingFriendRequestRejected { did: DID },
    IncomingFriendRequestClosed { did: DID },
    OutgoingFriendRequestClosed { did: DID },
    FriendAdded { did: DID },
    FriendRemoved { did: DID },
    IdentityOnline { did: DID },
    IdentityOffline { did: DID },
    IdentityUpdate { did: DID },
    Blocked { did: DID },
    BlockedBy { did: DID },
    Unblocked { did: DID },
    UnblockedBy { did: DID },
}

#[derive(Debug, PartialEq, Eq)]
pub enum ImportLocation<'a> {
    /// Remote location where the identity is stored
    Remote,

    /// Local path where the identity is stored
    Local { path: PathBuf },

    /// Buffer memory of where identity is stored
    Memory { buffer: &'a mut Vec<u8> },
}

#[derive(Debug, PartialEq, Eq)]
pub enum IdentityImportOption<'a> {
    Locate {
        /// Location of the identity
        location: ImportLocation<'a>,

        /// Passphrase of the identity
        passphrase: String,
    },
}

pub type MultiPassEventStream = BoxStream<'static, MultiPassEventKind>;

#[async_trait::async_trait]
#[impl_funcs(name = "multipass_impls")]
pub trait MultiPass:
    Extension
    + IdentityInformation
    + MultiPassImportExport
    + Friends
    + LocalIdentity
    + MultiPassEvent
    + Sync
    + Send
    + SingleHandle
{
    /// Create an [`Identity`]
    async fn create_identity(
        &mut self,
        username: Option<&str>,
        passphrase: Option<&str>,
    ) -> Result<IdentityProfile, Error>;

    /// Obtain an [`Identity`] using [`Identifier`]
    fn get_identity(&self, id: impl Into<Identifier>) -> GetIdentity;
}

#[async_trait::async_trait]
#[impl_funcs(name = "multipass_local_id_impls")]
pub trait LocalIdentity: Sync + Send {
    /// Reference to the local [`Identity`]
    async fn identity(&self) -> Result<Identity, Error>;

    /// Reference to the profile picture
    async fn profile_picture(&self) -> Result<IdentityImage, Error>;

    /// Reference to the profile picture
    async fn profile_banner(&self) -> Result<IdentityImage, Error>;

    /// Update your own [`Identity`] using [`IdentityUpdate`]
    async fn update_identity(&mut self, option: IdentityUpdate) -> Result<(), Error>;

    fn tesseract(&self) -> Tesseract;
}

#[async_trait::async_trait]
#[impl_funcs(name = "multipass_im_export_impls")]
pub trait MultiPassImportExport: Sync + Send {
    /// Import identity from a specific location
    async fn import_identity<'a>(
        &mut self,
        _: IdentityImportOption<'a>,
    ) -> Result<Identity, Error> {
        Err(Error::Unimplemented)
    }

    /// Manually export identity to a specific location
    async fn export_identity<'a>(&mut self, _: ImportLocation<'a>) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }
}

#[async_trait::async_trait]
#[impl_funcs(name = "multipass_friends_impls")]
pub trait Friends: Sync + Send {
    /// Send friend request to corresponding public key
    async fn send_request(&mut self, _: &DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Accept friend request from public key
    async fn accept_request(&mut self, _: &DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Deny friend request from public key
    async fn deny_request(&mut self, _: &DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Closing or retracting friend request
    async fn close_request(&mut self, _: &DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Check to determine if a request been received from the DID
    async fn received_friend_request_from(&self, _: &DID) -> Result<bool, Error> {
        Err(Error::Unimplemented)
    }

    /// List the incoming friend request
    async fn list_incoming_request(&self) -> Result<Vec<FriendRequest>, Error> {
        Err(Error::Unimplemented)
    }

    /// Check to determine if a request been sent to the DID
    async fn sent_friend_request_to(&self, _: &DID) -> Result<bool, Error> {
        Err(Error::Unimplemented)
    }

    /// List the outgoing friend request
    async fn list_outgoing_request(&self) -> Result<Vec<FriendRequest>, Error> {
        Err(Error::Unimplemented)
    }

    /// Remove friend from contacts
    async fn remove_friend(&mut self, _: &DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Block public key, rather it be a friend or not, from being able to send request to account public address
    async fn block(&mut self, _: &DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Unblock public key
    async fn unblock(&mut self, _: &DID) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// List block list
    async fn block_list(&self) -> Result<Vec<DID>, Error> {
        Err(Error::Unimplemented)
    }

    /// Check to see if public key is blocked
    async fn is_blocked(&self, _: &DID) -> Result<bool, Error> {
        Err(Error::Unimplemented)
    }

    /// List all friends public key
    async fn list_friends(&self) -> Result<Vec<DID>, Error> {
        Err(Error::Unimplemented)
    }

    /// Check to see if public key is friend of the account
    async fn has_friend(&self, _: &DID) -> Result<bool, Error> {
        Err(Error::Unimplemented)
    }
}

#[async_trait::async_trait]
#[impl_funcs(name = "multipass_event_impls")]
pub trait MultiPassEvent: Sync + Send {
    /// Subscribe to an stream of events
    async fn multipass_subscribe(&mut self) -> Result<MultiPassEventStream, Error> {
        Err(Error::Unimplemented)
    }
}

#[async_trait::async_trait]
#[impl_funcs(name = "multipass_identity_impls")]
pub trait IdentityInformation: Send + Sync {
    /// Profile picture belonging to the `Identity`
    async fn identity_picture(&self, _: &DID) -> Result<IdentityImage, Error> {
        Err(Error::Unimplemented)
    }

    /// Profile banner belonging to the `Identity`
    async fn identity_banner(&self, _: &DID) -> Result<IdentityImage, Error> {
        Err(Error::Unimplemented)
    }

    /// Identity status to determine if they are online or offline
    async fn identity_status(&self, _: &DID) -> Result<IdentityStatus, Error> {
        Err(Error::Unimplemented)
    }

    /// Identity status to determine if they are online or offline
    async fn set_identity_status(&mut self, _: IdentityStatus) -> Result<(), Error> {
        Err(Error::Unimplemented)
    }

    /// Find the relationship with an existing identity.
    async fn identity_relationship(&self, _: &DID) -> Result<Relationship, Error> {
        Err(Error::Unimplemented)
    }

    /// Returns the identity platform while online.
    async fn identity_platform(&self, _: &DID) -> Result<Platform, Error> {
        Err(Error::Unimplemented)
    }
}

pub struct GetIdentity {
    identifier: Identifier,
    stream: Option<BoxStream<'static, Identity>>,
}

impl GetIdentity {
    pub fn new(identifier: impl Into<Identifier>, stream: BoxStream<'static, Identity>) -> Self {
        let identifier = identifier.into();
        GetIdentity {
            identifier,
            stream: Some(stream),
        }
    }
}

impl Stream for GetIdentity {
    type Item = Identity;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };

        match futures::ready!(stream.poll_next_unpin(cx)) {
            Some(item) => Poll::Ready(Some(item)),
            None => {
                self.stream.take();
                Poll::Ready(None)
            }
        }
    }
}

impl Future for GetIdentity {
    type Output = Result<Identity, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        let Some(stream) = this.stream.as_mut() else {
            return Poll::Ready(Err(Error::IdentityDoesntExist));
        };

        if matches!(this.identifier, Identifier::DIDList(_)) {
            this.stream.take();
            return Poll::Ready(Err(Error::InvalidIdentifierCondition));
        }

        let result = match futures::ready!(stream.poll_next_unpin(cx)) {
            Some(identity) => Ok(identity),
            None => Err(Error::IdentityDoesntExist),
        };
        this.stream.take();
        Poll::Ready(result)
    }
}
