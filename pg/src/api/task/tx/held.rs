use crate::api::task::lease::{Lease, Recover};
use crate::{port::pool::access, schema};

pub(in crate::api) type Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, const ID: u8 = 0> =
    Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, access::Transaction<'d, ID>, Recover, ID>;

impl<'app, 'g, 'r, 's, 'b, 'c, 'd: 'app, B: ?Sized, I: schema::QuerySet + 'd, const ID: u8>
    Held<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, ID>
{
    pub(in crate::api) fn begin(
        self,
    ) -> Lease<'app, 'g, 'r, 's, 'b, 'c, 'd, B, I, access::Finalization<'d, ID>, Recover, ID> {
        let client = self.client();
        let state = client.port.shared.transaction().begin_finish(self.settle());
        Lease::new(client, state)
    }
}
