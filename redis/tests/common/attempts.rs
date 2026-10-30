use dope::{
    core::driver::{route::KeyTag, storage},
    manifold::app::creation,
    manifold::client::attempt::queue,
    net::tcp::Tcp,
};
use o3::depot::capacity::Count;

pub struct Attempts<const ID: u8>(Count);

impl<const ID: u8> Attempts<ID> {
    pub const fn new(capacity: Count) -> Self {
        Self(capacity)
    }
}

impl<const ID: u8> storage::Plan for Attempts<ID> {
    type Output<'p, 'd>
        = queue::Source<'d, Tcp, KeyTag<ID>>
    where
        Self: 'd,
        'd: 'p;
}

impl<const ID: u8> storage::Create for Attempts<ID> {
    type Error = creation::Error;
    fn try_build<'p, 'd>(
        self,
        mut context: storage::Context<'_, 'd>,
    ) -> Result<Self::Output<'p, 'd>, Self::Error>
    where
        Self: 'd,
        'd: 'p,
    {
        queue::Source::try_new(self.0, &mut context)
    }
}
