/// Errors that would host custom errors for modules, utilities, etc.
use thiserror::Error;

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
pub enum Error {
    //Hook Errors
    #[error("Hook is not registered")]
    HookUnregistered,
    #[error("Hook with this name already registered")]
    DuplicateHook,
    #[error("Already subscribed to this hook")]
    AlreadySubscribed,

    //Constellation Errors
    #[error("Constellation extension is unavailable")]
    ConstellationExtensionUnavailable,
    #[error("Item with name already exists in current directory")]
    DuplicateName,
    #[error("Directory cannot contain itself")]
    DirParadox,
    #[error("Directory cannot be found or is invalid")]
    InvalidDirectory,
    #[error("File cannot be found or is invalid")]
    InvalidFile,
    #[error("Item cannot be found or is invalid")]
    InvalidItem,
    #[error("Item is not a valid file")]
    ItemNotFile,
    #[error("Item is not a valid Directory")]
    ItemNotDirectory,
    #[error("Attempted conversion is invalid")]
    InvalidConversion,
    #[error("Attempted community is invalid")]
    InvalidCommunity,
    #[error("Attempted community invite is invalid")]
    InvalidCommunityInvite,
    #[error("Path supplied is invalid")]
    InvalidPath,
    #[error("Directory already exist")]
    DirectoryExist,
    #[error("File already exist")]
    FileExist,
    #[error("File cannot be found")]
    FileNotFound,
    #[error("Directory cannot be found")]
    DirectoryNotFound,
    #[error("Directory not empty")]
    DirectoryNotEmpty,

    //PocketDimension Errors
    #[error("Pocket dimension extension is unavailable")]
    PocketDimensionExtensionUnavailable,
    #[error("Data module supplied does not match dimension module")]
    DimensionMismatch,
    #[error("Data object provided already exist within the dimension")]
    DataObjectExist,
    #[error("Data object does not exist within the dimension")]
    DataObjectNotFound,

    //MultiPass Errors
    #[error("MultiPass extension is unavailable")]
    MultiPassExtensionUnavailable,
    #[error("Identity has not been created")]
    IdentityNotCreated,
    #[error("Identity exist with the same information")]
    IdentityExist,
    #[error("Identity does not exist")]
    IdentityDoesntExist,
    #[error("Identity was invalid")]
    IdentityInvalid,
    #[error("Picture is invalid or unavailable")]
    InvalidIdentityPicture,
    #[error("Banner is invalid or unavailable")]
    InvalidIdentityBanner,
    #[error("Username cannot be updated for identity")]
    CannotUpdateIdentityUsername,
    #[error("Picture cannot be updated for identity")]
    CannotUpdateIdentityPicture,
    #[error("Banner cannot be updated for identity")]
    CannotUpdateIdentityBanner,
    #[error("Status cannot be updated for identity")]
    CannotUpdateIdentityStatus,
    #[error("Identity could not be updated")]
    CannotUpdateIdentity,
    #[error("Public Key is Blocked")]
    PublicKeyIsBlocked,
    #[error("Public Key isnt Blocked")]
    PublicKeyIsntBlocked,
    #[error("Unable to send a friend request")]
    CannotSendFriendRequest,
    #[error("Friend request already exist")]
    FriendRequestExist,
    #[error("Friend request doesnt exist")]
    FriendRequestDoesntExist,
    #[error("You cannot send yourself a friend request")]
    CannotSendSelfFriendRequest,
    #[error("You cannot accept yourself as a friend")]
    CannotAcceptSelfAsFriend,
    #[error("You cannot deny yourself as a friend")]
    CannotDenySelfAsFriend,
    #[error("You cannot block yourself")]
    CannotBlockOwnKey,
    #[error("You cannot unblock yourself")]
    CannotUnblockOwnKey,
    #[error("You cannot remove yourself as a friend")]
    CannotRemoveSelfAsFriend,
    #[error("You cannot use yourself")]
    CannotUseSelfAsFriend,
    #[error("You cannot accept friend request")]
    CannotAcceptFriendRequest,
    #[error("Cannot find friend request")]
    CannotFindFriendRequest,
    #[error("Unable to close friend request")]
    CannotCloseFriendRequest,
    #[error("User does not exist as a friend")]
    FriendDoesntExist,
    #[error("User already exist as a friend")]
    FriendExist,
    #[error("User has blocked you from being able to interact with them")]
    BlockedByUser,
    #[error("Invalid identifier condition provided. Must be either public key, username, or your own identity")]
    InvalidIdentifierCondition,

    //RayGun Errors
    #[error("Unable to create conversation")]
    CannotCreateConversation,
    #[error("RayGun extension is unavailable")]
    RayGunExtensionUnavailable,
    #[error("Conversation was invalid")]
    InvalidConversation,
    #[error("Conversation already exist")]
    ConversationExist {
        conversation: crate::raygun::Conversation,
    },
    #[error("Maximum conversations has been reached")]
    ConversationLimitReached,
    #[error("Message is empty")]
    EmptyMessage,
    #[error("Message was invalid")]
    InvalidMessage,
    #[error("Sender of the message is invalid")]
    SenderMismatch,
    #[error("Cannot react to the message with the same reaction")]
    ReactionExist,
    #[error("Reaction to the message does not exist")]
    ReactionDoesntExist,
    #[error("Message is already pinned")]
    MessagePinned,
    #[error("Message is not pinned")]
    MessageNotPinned,
    #[error("Message exist within conversation")]
    MessageFound,
    #[error("Message not found within conversation")]
    MessageNotFound,
    #[error("Page of messages not found")]
    PageNotFound,
    #[error("Group could not be created at this time")]
    CannotCreateGroup,
    #[error("Unable to join group")]
    CannotJoinGroup,
    #[error("Unable to obtain the list of members from the group")]
    CannotGetMembers,
    #[error("Invalid Group Id")]
    InvalidGroupId,
    #[error("Invalid Group Member")]
    InvalidGroupMember,
    #[error("Already Community Member")]
    AlreadyCommunityMember,
    #[error("Invalid Community Member")]
    InvalidCommunityMember,
    #[error("Invite is invalid")]
    InvalidInvite,
    #[error("No valid Community invite")]
    NoValidCommunityInvite,
    #[error("Invite is targeting a different user")]
    CommunityInviteIncorrectUser,
    #[error("Invite is expired")]
    CommunityInviteExpired,
    #[error("Community invite doesn't exist")]
    CommunityInviteDoesntExist,
    #[error("Community channel doesn't exist")]
    CommunityChannelDoesntExist,
    #[error("Community role doesn't exist")]
    CommunityRoleDoesntExist,
    #[error("Community channel limit reached")]
    CommunityChannelLimitReached,
    #[error("Cant revoke inexistent permission")]
    CantRevokeInexistentPermission,
    #[error("Permission already granted")]
    PermissionAlreadyGranted,
    #[error("Unable to change group status")]
    CannotChangeGroupStatus,
    #[error("Group name exceed maximum limit")]
    GroupNameTooLong,
    #[error("Group name does not meet minimum limit")]
    GroupNameTooShort,
    #[error("Group is closed")]
    GroupClosed,
    #[error("Group is opened")]
    GroupOpened,
    #[error("No attachments provided for message")]
    NoAttachments,

    //Crypto Errors
    #[error("{0}")]
    Ed25519Error(#[from] ed25519_dalek::SignatureError),
    #[error("Encryption key does not exist")]
    KeyDoesntExist,
    #[error("Unable to encrypt data")]
    EncryptionError,
    #[error("Unable to decrypt data")]
    DecryptionError,
    #[error("Unable to encrypt stream")]
    EncryptionStreamError,
    #[error("Unable to decrypt stream")]
    DecryptionStreamError,
    #[error("Public key is invalid")]
    PublicKeyInvalid,
    #[error("Public key doesnt exist")]
    PublicKeyDoesntExist,
    #[error("Private key is invalid")]
    PrivateKeyInvalid,
    #[error("Public key length is invalid")]
    InvalidPublicKeyLength,
    #[error("Private key length is invalid")]
    InvalidPrivateKeyLength,
    #[error("Signature is invalid")]
    InvalidSignature,

    //Tesseract Errors
    #[error("Tesseract is unavailable")]
    TesseractUnavailable,
    #[error("Tesseract is locked")]
    TesseractLocked,
    #[error("Passphrase supplied is invalid")]
    InvalidPassphrase,
    #[error("One or more items in the datastore are corrupted or invalid")]
    CorruptedDataStore,
    #[error("Unable to save tesseract")]
    CannotSaveTesseract,

    //Data Errors
    #[error("Invalid data type")]
    InvalidDataType,

    //Blink Errors
    #[error("Audio device not found")]
    AudioDeviceNotFound,
    #[error("AudioDeviceDisconnected")]
    AudioDeviceDisconnected,
    // indicates a problem enumerating audio I/O devices
    #[error("AudioHostError: {_0}")]
    AudioHostError(String),
    #[error("BlinkNotInitialized")]
    BlinkNotInitialized,
    #[error("CallNotFound")]
    CallNotFound,
    #[error("CallNotInProgress")]
    CallNotInProgress,
    #[error("CallAlreadyInProgress")]
    CallAlreadyInProgress,
    #[error("FailedToSendSignal: {_0}")]
    FailedToSendSignal(String),
    #[error("Invalid MIME type: {_0}")]
    InvalidMimeType(String),
    #[error("InvalidAudioConfig")]
    InvalidAudioConfig,
    #[error("MicrophoneMissing")]
    MicrophoneMissing,
    #[error("SpeakerMissing")]
    SpeakerMissing,

    //Misc
    #[error("Unauthorized")]
    Unauthorized,
    #[error("Invalid permission")]
    InvalidPermission,
    #[error("Length for '{context}' is invalid. Current length: {current}. Minimum Length: {minimum:?}, Maximum: {maximum:?}")]
    InvalidLength {
        context: String,
        current: usize,
        minimum: Option<usize>,
        maximum: Option<usize>,
    },
    #[error("Context \"{pointer}\" cannot be null")]
    NullPointerContext { pointer: String },
    #[error("{0}")]
    OtherWithContext(String),
    #[error("Async runtime is unavailable")]
    AsyncRuntimeUnavailable,
    #[error("Sender Channel Unavailable")]
    SenderChannelUnavailable,
    #[error("Receiver Channel Unavailable")]
    ReceiverChannelUnavailable,
    #[error("Cannot find position of array content.")]
    ArrayPositionNotFound,
    #[error("Object is not found")]
    ObjectNotFound,
    #[error("The length of the key is invalid")]
    InvalidKeyLength,
    #[error("Error to be determined")]
    ToBeDetermined,
    #[error("{0}")]
    SerdeJsonError(#[from] serde_json::Error),
    #[error("{0}")]
    SerdeCborError(#[from] serde_cbor::Error),
    #[error("{0}")]
    UuidError(#[from] uuid::Error),
    #[error("{0}")]
    BincodeError(#[from] bincode::Error),
    #[error(transparent)]
    Any(#[from] anyhow::Error),
    #[error(transparent)]
    Bs58Error(#[from] bs58::decode::Error),
    #[error("{0}")]
    IoError(#[from] std::io::Error),
    #[error("Functionality is not yet implemented")]
    Unimplemented,
    #[error(transparent)]
    Boxed(Box<dyn std::error::Error + Sync + Send>),
    #[error("An unknown error has occurred")]
    Other,
}
