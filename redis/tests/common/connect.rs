use std::{marker::PhantomData, pin::Pin};

use dope::{core::driver::route::KeyTag, manifold::network::listener::conn, net::wire::Identity};
use dope_fiber::{abi::Fiber, net::Io};

#[pin_project::pin_project]
#[derive(dope::Application)]
#[dispatcher(driver = 'd)]
pub struct App<'d, C, L> {
    #[pin]
    #[app]
    connector: C,
    #[pin]
    #[manifold]
    listener: L,
    #[dispatcher(state)]
    scope: PhantomData<&'d ()>,
}

impl<'d, C, L> App<'d, C, L> {
    pub fn new(connector: C, listener: L) -> Self {
        Self {
            connector,
            listener,
            scope: PhantomData,
        }
    }

    pub fn connector(self: Pin<&Self>) -> Pin<&C> {
        self.project_ref().connector
    }
}

pub fn expect<'io, 'scope, 'w, 'd: 'w>(
    io: &'io mut Io<'scope, 'w, 'd, Identity, conn::Id<'d, KeyTag<1>>>,
    mut expected: &'static [u8],
) -> impl Fiber<'d, Output = ()> + 'io {
    dope_fiber::fiber!('d => async move {
        while !expected.is_empty() {
            let chunk = io.read().await.ok().expect("peer read").expect("request bytes");
            let actual = chunk.as_ref();
            assert!(!actual.is_empty());
            assert!(actual.len() <= expected.len(), "unexpected trailing command");
            assert_eq!(actual, &expected[..actual.len()]);
            expected = &expected[actual.len()..];
        }
    })
}
