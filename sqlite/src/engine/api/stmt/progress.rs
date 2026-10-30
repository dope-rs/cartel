pub(crate) enum Progress<P, R> {
    Pending(P),
    Ready(R),
}
