//! Error types and result handling for the video scheduler

use thiserror::Error;

/// Main error type for the video scheduler
#[derive(Error, Debug)]
pub enum Error {
    #[error("GStreamer error: {0}")]
    GStreamer(#[from] gstreamer::glib::Error),

    #[error("Pipeline error: {message}")]
    Pipeline { message: String },

    #[error("Element creation error: {element_name} - {message}")]
    ElementCreation { element_name: String, message: String },

    #[error("Element linking error: {from} -> {to} - {message}")]
    ElementLinking { from: String, to: String, message: String },

    #[error("State change error: {element_name} - {message}")]
    StateChange { element_name: String, message: String },

    #[error("SCTE-35 parsing error: {message}")]
    Scte35Parsing { message: String },

    #[error("Scheduler error: {message}")]
    Scheduler { message: String },

    #[error("Recorder error: {message}")]
    Recorder { message: String },

    #[error("Configuration error: {message}")]
    Configuration { message: String },

    #[error("Network error: {message}")]
    Network { message: String },

    #[error("File system error: {message}")]
    FileSystem { message: String },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("URL parsing error: {0}")]
    Url(#[from] url::ParseError),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

/// Result type alias for the video scheduler
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Create a new pipeline error
    pub fn pipeline(message: impl Into<String>) -> Self {
        Self::Pipeline {
            message: message.into(),
        }
    }

    /// Create a new element creation error
    pub fn element_creation(element_name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ElementCreation {
            element_name: element_name.into(),
            message: message.into(),
        }
    }

    /// Create a new element linking error
    pub fn element_linking(
        from: impl Into<String>,
        to: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::ElementLinking {
            from: from.into(),
            to: to.into(),
            message: message.into(),
        }
    }

    /// Create a new state change error
    pub fn state_change(element_name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::StateChange {
            element_name: element_name.into(),
            message: message.into(),
        }
    }

    /// Create a new SCTE-35 parsing error
    pub fn scte35_parsing(message: impl Into<String>) -> Self {
        Self::Scte35Parsing {
            message: message.into(),
        }
    }

    /// Create a new scheduler error
    pub fn scheduler(message: impl Into<String>) -> Self {
        Self::Scheduler {
            message: message.into(),
        }
    }

    /// Create a new recorder error
    pub fn recorder(message: impl Into<String>) -> Self {
        Self::Recorder {
            message: message.into(),
        }
    }

    /// Create a new configuration error
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Create a new network error
    pub fn network(message: impl Into<String>) -> Self {
        Self::Network {
            message: message.into(),
        }
    }

    /// Create a new file system error
    pub fn file_system(message: impl Into<String>) -> Self {
        Self::FileSystem {
            message: message.into(),
        }
    }
}
