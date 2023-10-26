pub mod shared_resource;

#[cfg(feature = "systemd")]
mod systemd;

#[cfg(not(feature = "systemd"))]
mod no_systemd;

pub mod daemon {
    #[cfg(not(feature = "systemd"))]
    pub use crate::no_systemd::{add_args, exiting, ready, start};
    #[cfg(feature = "systemd")]
    pub use crate::systemd::{add_args, exiting, ready, start};
}
