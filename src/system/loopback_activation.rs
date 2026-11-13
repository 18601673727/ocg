#[derive(Clone, Debug)]
pub(crate) struct ArmedActivation(());

impl ArmedActivation {
    pub(crate) const fn new() -> Self {
        Self(())
    }
}
