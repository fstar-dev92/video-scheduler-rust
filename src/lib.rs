//! Video Scheduler GStreamer - A Rust-based video streaming application
//! 
//! This crate provides functionality for:
//! - HLS/RTP stream processing with video/audio mixing
//! - SCTE-35 ad insertion handling
//! - Video compositing and switching between streams
//! - Stream scheduling and recording capabilities

pub mod error;
pub mod pipeline;
pub mod scte35;
pub mod scheduler;
pub mod recorder;
pub mod types;

#[cfg(test)]
mod tests;

pub use error::{Error, Result};
pub use types::*;

// Re-export commonly used types
pub use gstreamer::prelude::*;
pub use gstreamer::*;
