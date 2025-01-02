mod common;
#[cfg(test)]
mod test {
    use futures::{stream::FuturesUnordered, FutureExt, Stream, StreamExt, TryStreamExt};
    use std::{path::PathBuf, time::Duration};
    use uuid::Uuid;
    use warp::{
        constellation::{Constellation, Progression},
        raygun::{
            community::{
                Community, CommunityChannelPermission, CommunityChannelType, CommunityInvite,
                CommunityPermission, RayGunCommunity,
            },
            Location, Message, MessageEvent, MessageEventKind, MessageEventStream, MessageOptions,
            MessageReference, MessageStatus, Messages, RayGunEventKind, RayGunStream,
        },
    };

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as async_test;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    #[cfg(not(target_arch = "wasm32"))]
    use tokio::test as async_test;

    use warp::error::Error;

    use crate::common::create_accounts;

    const RED_PNG: [u8; 30] = [
        66, 77, 30, 0, 0, 0, 0, 0, 0, 0, 26, 0, 0, 0, 12, 0, 0, 0, 1, 0, 1, 0, 1, 0, 24, 0, 0, 0,
        255, 0,
    ];

    async fn assert_next_msg_event(
        streams: Vec<&mut MessageEventStream>,
        timeout: Duration,
        expected_event: MessageEventKind,
    ) -> Result<(), std::io::Error> {
        FuturesUnordered::from_iter(
            streams
                .into_iter()
                .map(|st| next_event(st, timeout).boxed()),
        )
        .try_for_each(|ev| {
            assert_eq!(ev, expected_event);
            futures::future::ok(())
        })
        .await
    }
    async fn next_event<S: Stream + Unpin>(
        stream: &mut S,
        timeout: Duration,
    ) -> Result<S::Item, std::io::Error> {
        crate::common::timeout(
            timeout,
            stream
                .next()
                .map(|item| item.expect("stream not to terminate")),
        )
        .await
    }
    #[async_test]
    async fn get_community_stream() -> anyhow::Result<()> {
        let context = Some("test::get_community_stream".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();
        let (instance_c, did_c, _) = &mut accounts[2].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut stream_a = instance_a.get_community_stream(community.id()).await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CreatedCommunityInvite {
                community_id: community.id(),
                invite: invite.clone()
            }
        );
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone(),
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let mut rg_stream_c = instance_c.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_c.clone()), None)
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::CreatedCommunityInvite {
                community_id: community.id(),
                invite: invite.clone(),
            },
        )
        .await?;
        assert_eq!(
            next_event(&mut rg_stream_c, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        instance_c.request_join_community(community.id()).await?;

        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_c.clone(),
            },
        )
        .await?;

        let community_seen_by_a = instance_a.get_community(community.id()).await?;
        let community_seen_by_b = instance_b.get_community(community.id()).await?;
        let community_seen_by_c = instance_c.get_community(community.id()).await?;

        assert_eq!(
            format!("{:?}", community_seen_by_a),
            format!("{:?}", community_seen_by_b)
        );
        assert_eq!(
            format!("{:?}", community_seen_by_b),
            format!("{:?}", community_seen_by_c)
        );
        Ok(())
    }

    #[async_test]
    async fn delete_community_as_owner() -> anyhow::Result<()> {
        let context = Some("test::delete_community_as_owner".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let mut rg_stream_a = instance_a.raygun_subscribe().await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let community = instance_a.create_community("Community0").await?;
        assert_eq!(
            next_event(&mut rg_stream_a, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityCreated {
                community_id: community.id()
            }
        );
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityJoined {
                community_id: community.id()
            }
        );

        instance_a.delete_community(community.id()).await?;
        assert_eq!(
            next_event(&mut rg_stream_a, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityDeleted {
                community_id: community.id()
            }
        );
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityDeleted {
                community_id: community.id()
            }
        );

        let community_a = instance_a.get_community(community.id()).await;
        let community_b = instance_b.get_community(community.id()).await;

        let expected_err = Err::<Community, warp::error::Error>(Error::InvalidCommunity);
        assert_eq!(format!("{:?}", community_a), format!("{:?}", expected_err));
        assert_eq!(format!("{:?}", community_b), format!("{:?}", expected_err));

        Ok(())
    }
    #[async_test]
    async fn delete_community_as_non_owner_member() -> anyhow::Result<()> {
        let context = Some("test::delete_community_as_non_owner_member".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b.delete_community(community.id()).await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }

    #[async_test]
    async fn delete_community_as_non_member() -> anyhow::Result<()> {
        let context = Some("test::delete_community_as_non_member".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, _, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;

        let result = instance_b.delete_community(community.id()).await;
        let expected_err = Err::<Community, warp::error::Error>(Error::InvalidCommunity);
        assert_eq!(format!("{:?}", result), format!("{:?}", expected_err));
        Ok(())
    }

    #[async_test]
    async fn get_community_as_uninvited_user() -> anyhow::Result<()> {
        let context = Some("test::get_community_as_uninvited_user".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, _, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;

        let result = instance_b.get_community(community.id()).await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::InvalidCommunity))
        );
        Ok(())
    }
    #[async_test]
    async fn get_community_as_invited_user() -> anyhow::Result<()> {
        let context = Some("test::get_community_as_invited_user".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(
                community.id(),
                Some(did_b.clone()),
                Some(chrono::Utc::now() + chrono::Duration::days(1)),
            )
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let result = instance_b.get_community(community.id()).await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::InvalidCommunity)),
        );
        Ok(())
    }
    #[async_test]
    async fn get_community_as_member() -> anyhow::Result<()> {
        let context = Some("test::get_community_as_member".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        instance_b.request_join_community(community.id()).await?;

        let got_community = instance_b.get_community(community.id()).await?;
        assert!(got_community.members().contains(&did_b.clone()));
        Ok(())
    }

    #[async_test]
    async fn list_community_joined() -> anyhow::Result<()> {
        let context = Some("test::list_communites_joined".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        instance_b.request_join_community(community.id()).await?;

        let list = instance_b.list_communities_joined().await?;
        assert!(list.len() == 1);
        assert!(list[0] == community.id());
        Ok(())
    }

    #[async_test]
    async fn list_communites_invited_to() -> anyhow::Result<()> {
        let context = Some("test::list_communites_invited_to".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let list = instance_b.list_communities_invited_to().await?;
        assert!(list.len() == 1);
        let (community_id, invited_to) = &list[0];
        assert!(community_id == &community.id());
        assert!(invited_to == &invite);
        Ok(())
    }

    // #[async_test]
    // async fn leave_community() -> anyhow::Result<()> {
    //     assert!(false);
    //     Ok(())
    // }

    #[async_test]
    async fn unauthorized_edit_community_icon() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_icon".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let file_name = "red.png";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let location = Location::Constellation {
            path: file_name.to_string(),
        };
        let result = instance_b
            .edit_community_icon(community.id(), location)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<(), Error>(Error::Unauthorized)),
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_icon() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_icon".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        instance_a
            .grant_community_permission_for_all(community.id(), CommunityPermission::EditIcon)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let file_name = "red.png";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let location = Location::Constellation {
            path: file_name.to_string(),
        };

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_b
            .edit_community_icon(community.id(), location)
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::EditedCommunityIcon {
                community_id: community.id(),
            },
        )
        .await?;

        let image = instance_a.get_community_icon(community.id()).await?;
        assert_eq!(image.data(), &RED_PNG);
        Ok(())
    }
    #[async_test]
    async fn unauthorized_edit_community_banner() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_banner".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let file_name = "red.png";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let location = Location::Constellation {
            path: file_name.to_string(),
        };
        let result = instance_b
            .edit_community_banner(community.id(), location)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<(), Error>(Error::Unauthorized)),
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_banner() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_icon".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        instance_a
            .grant_community_permission_for_all(community.id(), CommunityPermission::EditBanner)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let file_name = "red.png";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let location = Location::Constellation {
            path: file_name.to_string(),
        };

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_b
            .edit_community_banner(community.id(), location)
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::EditedCommunityBanner {
                community_id: community.id(),
            },
        )
        .await?;

        let image = instance_a.get_community_banner(community.id()).await?;
        assert_eq!(image.data(), &RED_PNG);
        Ok(())
    }
    #[async_test]
    async fn unauthorized_create_community_invite() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_create_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        instance_a
            .revoke_community_permission_for_all(community.id(), CommunityPermission::CreateInvites)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .create_community_invite(community.id(), None, None)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<CommunityInvite, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_create_community_invite() -> anyhow::Result<()> {
        let context = Some("test::authorized_create_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();
        let (instance_c, did_c, _) = &mut accounts[2].clone();

        let community = instance_a.create_community("Community0").await?;
        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::CreateInvites,
                role.id(),
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite_for_b = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_b.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityJoined {
                community_id: community.id()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::GrantedCommunityRole {
                community_id: community.id(),
                role_id: role.id(),
                user: did_b.clone(),
            },
        )
        .await?;

        let mut rg_stream_c = instance_c.raygun_subscribe().await?;
        let invite_for_c = instance_b
            .create_community_invite(community.id(), Some(did_c.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CreatedCommunityInvite {
                community_id: community.id(),
                invite: invite_for_c.clone(),
            }
        );
        assert_eq!(
            next_event(&mut rg_stream_c, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_c.id()
            }
        );

        instance_c.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_c.clone()
            }
        );
        assert_eq!(
            next_event(&mut rg_stream_c, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityJoined {
                community_id: community.id()
            }
        );
        Ok(())
    }
    #[async_test]
    async fn unauthorized_delete_community_invite() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_delete_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite_for_b = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_b.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let invite_to_try_delete = instance_a
            .create_community_invite(community.id(), None, None)
            .await?;

        let result = instance_b
            .delete_community_invite(community.id(), invite_to_try_delete.id())
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_delete_community_invite() -> anyhow::Result<()> {
        let context = Some("test::authorized_delete_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite_for_b = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_b.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::DeleteInvites,
                role.id(),
            )
            .await?;

        let invite_to_delete = instance_a
            .create_community_invite(community.id(), None, None)
            .await?;
        instance_b
            .delete_community_invite(community.id(), invite_to_delete.id())
            .await?;

        let community = instance_a.get_community(community.id()).await?;
        assert!(!community.invites().contains(&invite_to_delete.id()));
        Ok(())
    }
    #[async_test]
    async fn get_community_invite() -> anyhow::Result<()> {
        let context = Some("test::get_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let got_invite = instance_a
            .get_community_invite(community.id(), invite.id())
            .await?;
        assert_eq!(invite, got_invite);
        Ok(())
    }
    #[async_test]
    async fn unauthorized_edit_community_invite() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite_for_b = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_b.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut invite = instance_a
            .create_community_invite(community.id(), None, None)
            .await?;
        invite.set_target_user(Some(did_b.clone()));
        let result = instance_b
            .edit_community_invite(community.id(), invite.id(), invite)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_invite() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite_for_b = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_b.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(community.id(), CommunityPermission::EditInvites, role.id())
            .await?;

        let mut invite = instance_a
            .create_community_invite(community.id(), None, None)
            .await?;
        invite.set_target_user(Some(did_b.clone()));
        instance_b
            .edit_community_invite(community.id(), invite.id(), invite.clone())
            .await?;

        let got_invite = instance_a
            .get_community_invite(community.id(), invite.id())
            .await?;
        assert_eq!(invite, got_invite);
        Ok(())
    }
    #[async_test]
    async fn accept_valid_community_invite() -> anyhow::Result<()> {
        let context = Some("test::accept_valid_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite_for_b = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_b.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );
        Ok(())
    }
    #[async_test]
    async fn try_accept_wrong_target_community_invite() -> anyhow::Result<()> {
        let context = Some("test::try_accept_wrong_target_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, _, _) = &mut accounts[1].clone();
        let (instance_c, did_c, _) = &mut accounts[2].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_c = instance_c.raygun_subscribe().await?;
        let invite_for_b = instance_a
            .create_community_invite(community.id(), Some(did_c.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_c, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_b.id()
            }
        );

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityJoinRejected {
                community_id: community.id(),
            }
        );
        Ok(())
    }
    #[async_test]
    async fn try_accept_expired_community_invite() -> anyhow::Result<()> {
        let context = Some("test::try_accept_expired_community_invite".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite_for_b = instance_a
            .create_community_invite(
                community.id(),
                Some(did_b.clone()),
                Some(chrono::Utc::now() - chrono::Duration::days(1)),
            )
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite_for_b.id()
            }
        );

        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityJoinRejected {
                community_id: community.id(),
            }
        );
        Ok(())
    }

    #[async_test]
    async fn unauthorized_get_community_channel() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_get_community_channel".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .get_community_channel(community.id(), channel.id())
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_get_community_channel() -> anyhow::Result<()> {
        let context = Some("test::authorized_get_community_channel".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let got_channel = instance_b
            .get_community_channel(community.id(), channel.id())
            .await?;

        assert_eq!(channel, got_channel);
        Ok(())
    }
    #[async_test]
    async fn unauthorized_create_community_channel() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_create_community_channel".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_create_community_channel() -> anyhow::Result<()> {
        let context = Some("test::authorized_create_community_channel".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::CreateChannels,
                role.id(),
            )
            .await?;

        let channel = instance_b
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let got_channel = instance_a
            .get_community_channel(community.id(), channel.id())
            .await?;

        assert_eq!(channel, got_channel);
        Ok(())
    }
    #[async_test]
    async fn unauthorized_delete_community_channel() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_delete_community_channel".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        let result = instance_b
            .delete_community_channel(community.id(), channel.id())
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_delete_community_channel() -> anyhow::Result<()> {
        let context = Some("test::authorized_delete_community_channel".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::DeleteChannels,
                role.id(),
            )
            .await?;

        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        let community = instance_a.get_community(community.id()).await?;
        assert!(community.channels().contains(&channel.id()));

        instance_b
            .delete_community_channel(community.id(), channel.id())
            .await?;
        let community = instance_a.get_community(community.id()).await?;
        assert!(!community.channels().contains(&channel.id()));
        Ok(())
    }

    #[async_test]
    async fn unauthorized_edit_community_name() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_name".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .edit_community_name(community.id(), "Community1")
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_name() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_name".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(community.id(), CommunityPermission::EditName, role.id())
            .await?;

        let new_name = "Community1";
        instance_b
            .edit_community_name(community.id(), new_name)
            .await?;
        let community = instance_a.get_community(community.id()).await?;
        assert_eq!(new_name, community.name());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_edit_community_description() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_description".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .edit_community_description(community.id(), Some("description".to_string()))
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_description() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_description".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::EditDescription,
                role.id(),
            )
            .await?;

        let new_description = Some("description".to_string());
        instance_b
            .edit_community_description(community.id(), new_description.clone())
            .await?;
        let community = instance_a.get_community(community.id()).await?;

        assert_eq!(new_description.as_deref(), community.description());
        Ok(())
    }
    #[async_test]
    async fn edit_community_description_as_owner() -> anyhow::Result<()> {
        let context = Some("test::edit_community_description_as_owner".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();

        let community = instance_a.create_community("Community0").await?;
        let new_description = Some("description".to_string());
        instance_a
            .edit_community_description(community.id(), new_description.clone())
            .await?;

        let community = instance_a.get_community(community.id()).await?;
        assert_eq!(community.description(), new_description.as_deref());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_edit_community_role_name() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_role_name".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .edit_community_role_name(community.id(), role.id(), "new_name".to_string())
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_role_name() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_role_name".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(community.id(), CommunityPermission::EditRoles, role.id())
            .await?;

        let new_name = "new_name".to_string();
        instance_b
            .edit_community_role_name(community.id(), role.id(), new_name.clone())
            .await?;
        let role = instance_a
            .get_community_role(community.id(), role.id())
            .await?;
        assert_eq!(new_name, role.name());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_edit_community_permissions() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_permissions".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .grant_community_permission(community.id(), CommunityPermission::EditName, role.id())
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_permissions() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_permissions".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::GrantPermissions,
                role.id(),
            )
            .await?;

        instance_b
            .grant_community_permission(community.id(), CommunityPermission::EditName, role.id())
            .await?;
        let community = instance_a.get_community(community.id()).await?;
        assert!(community
            .permissions()
            .get(&CommunityPermission::EditName.to_string())
            .unwrap()
            .contains(&role.id()));
        Ok(())
    }
    #[async_test]
    async fn unauthorized_remove_community_member() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_remove_community_member".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();
        let (instance_c, did_c, _) = &mut accounts[2].clone();

        let community = instance_a.create_community("Community0").await?;
        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut rg_stream_c = instance_c.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_c.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_c, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_c.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_c.clone()
            }
        );

        let result = instance_b
            .remove_community_member(community.id(), did_c.clone())
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_remove_community_member() -> anyhow::Result<()> {
        let context = Some("test::authorized_remove_community_member".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();
        let (instance_c, did_c, _) = &mut accounts[2].clone();

        let community = instance_a.create_community("Community0").await?;
        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::RemoveMembers,
                role.id(),
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::GrantedCommunityRole {
                community_id: community.id(),
                role_id: role.id(),
                user: did_b.clone(),
            },
        )
        .await?;

        let mut rg_stream_c = instance_c.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_c.clone()), None)
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::CreatedCommunityInvite {
                community_id: community.id(),
                invite: invite.clone(),
            },
        )
        .await?;
        assert_eq!(
            next_event(&mut rg_stream_c, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        instance_c.request_join_community(community.id()).await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_c.clone(),
            },
        )
        .await?;

        instance_b
            .remove_community_member(community.id(), did_c.clone())
            .await?;

        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::RemovedCommunityMember {
                community_id: community.id(),
                member: did_c.clone()
            }
        );
        let community = instance_a.get_community(community.id()).await?;

        assert!(!community.members().contains(&did_c.clone()));
        Ok(())
    }

    #[async_test]
    async fn unauthorized_edit_community_channel_name() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_channel_name".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .edit_community_channel_name(community.id(), channel.id(), "new_name")
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_channel_name() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_channel_name".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::EditChannels,
                role.id(),
            )
            .await?;

        let new_name = "new_name";
        instance_b
            .edit_community_channel_name(community.id(), channel.id(), new_name)
            .await?;
        let channel = instance_a
            .get_community_channel(community.id(), channel.id())
            .await?;

        assert_eq!(new_name, channel.name());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_edit_community_channel_description() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_channel_description".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .edit_community_channel_description(
                community.id(),
                channel.id(),
                Some("description".to_string()),
            )
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_channel_description() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_channel_description".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::EditChannels,
                role.id(),
            )
            .await?;

        let new_description = Some("description".to_string());
        instance_b
            .edit_community_channel_description(
                community.id(),
                channel.id(),
                new_description.clone(),
            )
            .await?;
        let channel = instance_a
            .get_community_channel(community.id(), channel.id())
            .await?;

        assert_eq!(new_description.as_deref(), channel.description());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_edit_community_channel_permissions() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_channel_permissions".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let _ = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .grant_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Community, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_channel_permissions() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_channel_permissions".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let role = instance_a
            .create_community_role(community.id(), "Role0")
            .await?;
        instance_a
            .grant_community_role(community.id(), role.id(), did_b.clone())
            .await?;
        instance_a
            .grant_community_permission(
                community.id(),
                CommunityPermission::GrantPermissions,
                role.id(),
            )
            .await?;

        instance_b
            .grant_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let channel = instance_a
            .get_community_channel(community.id(), channel.id())
            .await?;
        assert!(channel
            .permissions()
            .get(&CommunityChannelPermission::ViewChannel.to_string())
            .is_none());
        Ok(())
    }

    #[async_test]
    async fn unauthorized_get_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_get_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let result = instance_b
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Message, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_get_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_get_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let message = instance_b
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await?;
        assert_eq!(message.lines().len(), 1);
        assert_eq!(message.lines()[0], text);
        Ok(())
    }
    #[async_test]
    async fn unauthorized_get_community_channel_messages() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_get_community_channel_messages".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let result = instance_b
            .get_community_channel_messages(community.id(), channel.id(), MessageOptions::default())
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Messages, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_get_community_channel_messages() -> anyhow::Result<()> {
        let context = Some("test::authorized_get_community_channel_messages".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let messages = instance_b
            .get_community_channel_messages(community.id(), channel.id(), MessageOptions::default())
            .await?;
        match messages {
            Messages::List(vec) => {
                assert_eq!(vec.len(), 1);
                assert_eq!(vec[0].lines().len(), 1);
                assert_eq!(vec[0].lines()[0], text);
            }
            Messages::Stream(_) => panic!("should not be Messages::Stream"),
            Messages::Page { pages: _, total: _ } => panic!("should not be Messages::Page"),
        }
        Ok(())
    }
    #[async_test]
    async fn unauthorized_get_community_channel_message_count() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_get_community_channel_message_count".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let result = instance_b
            .get_community_channel_message_count(community.id(), channel.id())
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<usize, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_get_community_channel_message_count() -> anyhow::Result<()> {
        let context = Some("test::authorized_get_community_channel_message_count".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let count = instance_b
            .get_community_channel_message_count(community.id(), channel.id())
            .await?;
        assert_eq!(count, 1);
        Ok(())
    }
    #[async_test]
    async fn unauthorized_get_community_channel_message_reference() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_get_community_channel_message_reference".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let result = instance_b
            .get_community_channel_message_reference(community.id(), channel.id(), message_id)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<MessageReference, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_get_community_channel_message_reference() -> anyhow::Result<()> {
        let context = Some("test::authorized_get_community_channel_message_reference".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, did_a, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let reference = instance_b
            .get_community_channel_message_reference(community.id(), channel.id(), message_id)
            .await?;
        assert_eq!(reference.id(), message_id);
        assert_eq!(reference.sender(), did_a);
        assert_eq!(reference.conversation_id(), channel.id());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_get_community_channel_message_references() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_get_community_channel_message_references".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let result = instance_b
            .get_community_channel_message_references(
                community.id(),
                channel.id(),
                MessageOptions::default(),
            )
            .await;
        match result {
            Ok(_) => panic!("should not have succeeded"),
            Err(e) => assert_eq!(format!("{:?}", e), format!("{:?}", Error::Unauthorized),),
        }
        Ok(())
    }
    #[async_test]
    async fn authorized_get_community_channel_message_references() -> anyhow::Result<()> {
        let context = Some("test::authorized_get_community_channel_message_references".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, did_a, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let mut references = instance_b
            .get_community_channel_message_references(
                community.id(),
                channel.id(),
                MessageOptions::default(),
            )
            .await?;
        let reference: MessageReference =
            references.next().await.expect("should contain a reference");
        assert_eq!(reference.id(), message_id);
        assert_eq!(reference.sender(), did_a);
        assert_eq!(reference.conversation_id(), channel.id());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_send_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_send_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::SendMessages,
            )
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let result = instance_b
            .send_community_channel_message(community.id(), channel.id(), vec!["Hello".to_string()])
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Uuid, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_send_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_send_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let message = instance_b
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await?;

        assert_eq!(message.lines().len(), 1);
        assert_eq!(message.lines()[0], text);
        Ok(())
    }

    #[async_test]
    async fn get_community_channel_message_status() -> anyhow::Result<()> {
        let context = Some("test::community_channel_message_status".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        let status_a = instance_a
            .community_channel_message_status(community.id(), channel.id(), message_id)
            .await?;
        assert_eq!(status_a, MessageStatus::Sent);
        let status_b = instance_b
            .community_channel_message_status(community.id(), channel.id(), message_id)
            .await?;
        assert_eq!(status_b, MessageStatus::Sent);
        Ok(())
    }

    #[async_test]
    async fn send_and_cancel_community_channel_messsage_event() -> anyhow::Result<()> {
        let context = Some("test::send_and_cancel_community_channel_messsage_event".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        instance_b
            .send_community_channel_messsage_event(
                community.id(),
                channel.id(),
                MessageEvent::Typing,
            )
            .await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityEventReceived {
                community_id: community.id(),
                community_channel_id: channel.id(),
                did_key: did_b.clone(),
                event: MessageEvent::Typing,
            }
        );

        instance_b
            .cancel_community_channel_messsage_event(
                community.id(),
                channel.id(),
                MessageEvent::Typing,
            )
            .await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityEventCancelled {
                community_id: community.id(),
                community_channel_id: channel.id(),
                did_key: did_b.clone(),
                event: MessageEvent::Typing,
            }
        );
        Ok(())
    }

    #[async_test]
    async fn unauthorized_edit_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_edit_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_b
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_b = next_event(&mut stream_b, Duration::from_secs(60)).await?;
        let message_id = match event_b {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_b
            ),
        };
        assert_eq!(
            event_b,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::SendMessages,
            )
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::RevokedCommunityChannelPermissionForAll {
                community_id: community.id(),
                channel_id: channel.id(),
                permissions: vec![CommunityChannelPermission::SendMessages.to_string()],
            },
        )
        .await?;

        let text = vec!["Edit".to_string()];
        let result = instance_b
            .edit_community_channel_message(community.id(), channel.id(), message_id, text)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Uuid, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_edit_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_edit_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        let text = vec!["Edit".to_string()];
        instance_a
            .edit_community_channel_message(community.id(), channel.id(), message_id, text.clone())
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::CommunityMessageEdited {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            },
        )
        .await?;
        let message = instance_b
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await?;
        assert_eq!(message.lines(), text.as_slice());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_reply_to_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_reply_to_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_b
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_b = next_event(&mut stream_b, Duration::from_secs(60)).await?;
        let message_id = match event_b {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_b
            ),
        };
        assert_eq!(
            event_b,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::SendMessages,
            )
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::RevokedCommunityChannelPermissionForAll {
                community_id: community.id(),
                channel_id: channel.id(),
                permissions: vec![CommunityChannelPermission::SendMessages.to_string()],
            },
        )
        .await?;

        let text = vec!["Reply".to_string()];
        let result = instance_b
            .reply_to_community_channel_message(community.id(), channel.id(), message_id, text)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Uuid, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_reply_to_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_reply_to_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        let text = vec!["Edit".to_string()];
        instance_a
            .reply_to_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                text.clone(),
            )
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        let message = instance_b
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await?;
        assert_eq!(message.lines(), text.as_slice());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_delete_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_delete_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_b
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_b = next_event(&mut stream_b, Duration::from_secs(60)).await?;
        let message_id = match event_b {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_b
            ),
        };
        assert_eq!(
            event_b,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        let result = instance_b
            .delete_community_channel_message(community.id(), channel.id(), message_id)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Uuid, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_delete_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_delete_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        instance_a
            .delete_community_channel_message(community.id(), channel.id(), message_id)
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::CommunityMessageDeleted {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            },
        )
        .await?;
        let result = instance_b
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Message, Error>(Error::MessageNotFound))
        );
        Ok(())
    }
    #[async_test]
    async fn unauthorized_pin_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_pin_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_b
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_b = next_event(&mut stream_b, Duration::from_secs(60)).await?;
        let message_id = match event_b {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_b
            ),
        };
        assert_eq!(
            event_b,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        instance_a
            .revoke_community_permission_for_all(community.id(), CommunityPermission::PinMessages)
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::RevokedCommunityPermissionForAll {
                community_id: community.id(),
                permissions: vec![CommunityPermission::PinMessages.to_string()],
            },
        )
        .await?;

        let result = instance_b
            .pin_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                warp::raygun::PinState::Pin,
            )
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Uuid, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_pin_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_pin_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        instance_a
            .pin_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                warp::raygun::PinState::Pin,
            )
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::CommunityMessagePinned {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            },
        )
        .await?;
        let message = instance_b
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await?;
        assert!(message.pinned());
        Ok(())
    }
    #[async_test]
    async fn unauthorized_react_to_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_react_to_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_b
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_b = next_event(&mut stream_b, Duration::from_secs(60)).await?;
        let message_id = match event_b {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_b
            ),
        };
        assert_eq!(
            event_b,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::RevokedCommunityChannelPermissionForAll {
                community_id: community.id(),
                channel_id: channel.id(),
                permissions: vec![CommunityChannelPermission::ViewChannel.to_string()],
            },
        )
        .await?;

        let emoji = "😀".to_string();
        let result = instance_b
            .react_to_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                warp::raygun::ReactionState::Add,
                emoji,
            )
            .await;
        assert_eq!(
            format!("{:?}", result),
            format!("{:?}", Err::<Uuid, Error>(Error::Unauthorized))
        );
        Ok(())
    }
    #[async_test]
    async fn authorized_react_to_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_react_to_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, did_a, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let text = "Hello".to_string();
        instance_a
            .send_community_channel_message(community.id(), channel.id(), vec![text.clone()])
            .await?;
        let event_a = next_event(&mut stream_a, Duration::from_secs(60)).await?;
        let message_id = match event_a {
            MessageEventKind::CommunityMessageSent {
                community_id: _,
                channel_id: _,
                message_id,
            } => message_id,
            _ => panic!(
                "expected MessageEventKind::CommunityMessageSent event, got: {:?}",
                event_a
            ),
        };
        assert_eq!(
            event_a,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );

        let emoji = "😀".to_string();
        instance_a
            .react_to_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                warp::raygun::ReactionState::Add,
                emoji.clone(),
            )
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::CommunityMessageReactionAdded {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
                did_key: did_a.clone(),
                reaction: emoji,
            },
        )
        .await?;
        Ok(())
    }
    #[async_test]
    async fn unauthorized_attach_to_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_attach_to_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::SendAttachments,
            )
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::RevokedCommunityChannelPermissionForAll {
                community_id: community.id(),
                channel_id: channel.id(),
                permissions: vec![CommunityChannelPermission::SendAttachments.to_string()],
            },
        )
        .await?;

        let file_name = "my_file.txt";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let locations = vec![Location::Constellation {
            path: file_name.to_string(),
        }];
        let message = vec!["here is my file".to_string()];
        let result = instance_b
            .attach_to_community_channel_message(
                community.id(),
                channel.id(),
                None,
                locations,
                message,
            )
            .await;

        match result {
            Ok(_) => panic!("should not have succeeded"),
            Err(e) => {
                assert_eq!(format!("{:?}", e), format!("{:?}", Error::Unauthorized));
            }
        }
        Ok(())
    }
    #[async_test]
    async fn authorized_attach_to_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_attach_to_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let file_name = "my_file.txt";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let locations = vec![Location::Constellation {
            path: file_name.to_string(),
        }];
        let message = vec!["here is my file".to_string()];
        let _ = instance_b
            .attach_to_community_channel_message(
                community.id(),
                channel.id(),
                None,
                locations,
                message,
            )
            .await?;
        Ok(())
    }
    #[async_test]
    async fn unauthorized_download_from_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::unauthorized_download_from_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let file_name = "my_file.txt";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let locations = vec![Location::Constellation {
            path: file_name.to_string(),
        }];
        let message = vec!["here is my file".to_string()];
        let (message_id, mut attachment_event_stream) = instance_b
            .attach_to_community_channel_message(
                community.id(),
                channel.id(),
                None,
                locations,
                message,
            )
            .await?;
        loop {
            if attachment_event_stream.next().await.is_none() {
                break;
            }
        }
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id
            }
        );

        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::RevokedCommunityChannelPermissionForAll {
                community_id: community.id(),
                channel_id: channel.id(),
                permissions: vec![CommunityChannelPermission::ViewChannel.to_string()],
            },
        )
        .await?;

        let result = instance_b
            .download_from_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                file_name.to_string(),
                PathBuf::new(),
            )
            .await;
        match result {
            Ok(_) => panic!("should not have succeeded"),
            Err(e) => assert_eq!(format!("{:?}", e), format!("{:?}", Error::Unauthorized)),
        }
        Ok(())
    }
    #[async_test]
    async fn authorized_download_from_community_channel_message() -> anyhow::Result<()> {
        let context = Some("test::authorized_download_from_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let file_name = "my_file.txt";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let locations = vec![Location::Constellation {
            path: file_name.to_string(),
        }];
        let message = vec!["here is my file".to_string()];
        let (message_id, mut attachment_event_stream) = instance_b
            .attach_to_community_channel_message(
                community.id(),
                channel.id(),
                None,
                locations,
                message.clone(),
            )
            .await?;
        loop {
            if attachment_event_stream.next().await.is_none() {
                break;
            }
        }
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id
            }
        );
        let path = PathBuf::from("test::authorized_download_from_community_channel_message");
        let mut constellation_progress_stream = instance_b
            .download_from_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                file_name.to_string(),
                path.clone(),
            )
            .await?;
        loop {
            if let Some(progress) = constellation_progress_stream.next().await {
                match progress {
                    Progression::ProgressComplete { name: _, total } => {
                        assert_eq!(total, Some(RED_PNG.len()));
                        break;
                    }
                    Progression::ProgressFailed {
                        name: _,
                        last_size: _,
                        error,
                    } => panic!("progress shouldn't have failed: {}", error),
                    Progression::CurrentProgress {
                        name,
                        current,
                        total,
                    } => {
                        println!("name: {}, current: {}, total: {:?}", name, current, total)
                    }
                }
            }
        }
        let contents = fs::read(path.clone()).await?;
        assert_eq!(contents, RED_PNG.clone());
        fs::remove_file(path).await?;

        let msg = instance_a
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await?;
        assert_eq!(msg.lines(), &message);
        assert_eq!(msg.attachments().len(), 1);
        Ok(())
    }
    #[async_test]
    async fn unauthorized_download_stream_from_community_channel_message() -> anyhow::Result<()> {
        let context =
            Some("test::unauthorized_download_stream_from_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let file_name = "my_file.txt";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let locations = vec![Location::Constellation {
            path: file_name.to_string(),
        }];
        let message = vec!["here is my file".to_string()];
        let (message_id, mut attachment_event_stream) = instance_b
            .attach_to_community_channel_message(
                community.id(),
                channel.id(),
                None,
                locations,
                message.clone(),
            )
            .await?;
        loop {
            if attachment_event_stream.next().await.is_none() {
                break;
            }
        }
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id
            }
        );

        instance_a
            .revoke_community_channel_permission_for_all(
                community.id(),
                channel.id(),
                CommunityChannelPermission::ViewChannel,
            )
            .await?;
        assert_next_msg_event(
            vec![&mut stream_a, &mut stream_b],
            Duration::from_secs(60),
            MessageEventKind::RevokedCommunityChannelPermissionForAll {
                community_id: community.id(),
                channel_id: channel.id(),
                permissions: vec![CommunityChannelPermission::ViewChannel.to_string()],
            },
        )
        .await?;

        let result = instance_b
            .download_stream_from_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                file_name,
            )
            .await;
        match result {
            Ok(_) => panic!("should not have succeeded"),
            Err(e) => assert_eq!(format!("{:?}", e), format!("{:?}", Error::Unauthorized)),
        }
        Ok(())
    }
    #[async_test]
    async fn authorized_download_stream_from_community_channel_message() -> anyhow::Result<()> {
        let context =
            Some("test::authorized_download_stream_from_community_channel_message".into());
        let acc = (None, None, context);
        let accounts = create_accounts(vec![acc.clone(), acc]).await?;
        let (instance_a, _, _) = &mut accounts[0].clone();
        let (instance_b, did_b, _) = &mut accounts[1].clone();

        let community = instance_a.create_community("Community0").await?;
        let channel = instance_a
            .create_community_channel(community.id(), "Channel0", CommunityChannelType::Standard)
            .await?;

        let mut rg_stream_b = instance_b.raygun_subscribe().await?;
        let invite = instance_a
            .create_community_invite(community.id(), Some(did_b.clone()), None)
            .await?;
        assert_eq!(
            next_event(&mut rg_stream_b, Duration::from_secs(60)).await?,
            RayGunEventKind::CommunityInvited {
                community_id: community.id(),
                invite_id: invite.id()
            }
        );

        let mut stream_a = instance_a.get_community_stream(community.id()).await?;
        instance_b.request_join_community(community.id()).await?;
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityJoined {
                community_id: community.id(),
                user: did_b.clone()
            }
        );

        let mut stream_b = instance_b.get_community_stream(community.id()).await?;
        let file_name = "my_file.txt";
        instance_b.put_buffer(file_name, &RED_PNG).await?;
        let locations = vec![Location::Constellation {
            path: file_name.to_string(),
        }];
        let message = vec!["here is my file".to_string()];
        let (message_id, mut attachment_event_stream) = instance_b
            .attach_to_community_channel_message(
                community.id(),
                channel.id(),
                None,
                locations,
                message.clone(),
            )
            .await?;
        loop {
            if attachment_event_stream.next().await.is_none() {
                break;
            }
        }
        assert_eq!(
            next_event(&mut stream_b, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageSent {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id,
            }
        );
        assert_eq!(
            next_event(&mut stream_a, Duration::from_secs(60)).await?,
            MessageEventKind::CommunityMessageReceived {
                community_id: community.id(),
                channel_id: channel.id(),
                message_id
            }
        );
        let mut download_stream = instance_b
            .download_stream_from_community_channel_message(
                community.id(),
                channel.id(),
                message_id,
                file_name,
            )
            .await?;

        let mut bytes = Vec::with_capacity(RED_PNG.len());

        while let Some(result) = download_stream.next().await {
            let b = result.expect("valid");
            bytes.extend(b.to_vec());
        }

        assert_eq!(bytes, RED_PNG);

        let msg = instance_a
            .get_community_channel_message(community.id(), channel.id(), message_id)
            .await?;
        assert_eq!(msg.lines(), &message);
        assert_eq!(msg.attachments().len(), 1);
        Ok(())
    }
}
