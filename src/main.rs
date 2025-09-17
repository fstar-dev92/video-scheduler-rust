//! Main entry point for the video scheduler application

use clap::{Arg, Command};
use std::path::Path;
use std::process;
use tracing::{error, info, warn};
use tracing_subscriber;

use video_scheduler_gstreamer::{
    error::Result,
    pipeline::{HlsPipeline, RtpPipeline},
    types::PipelineConfig,
};

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let matches = Command::new("video-scheduler")
        .version("0.1.0")
        .about("A Rust-based video streaming application with GStreamer")
        .arg(
            Arg::new("input-host")
                .long("input-host")
                .value_name("HOST")
                .help("Input RTP stream host")
                .default_value("239.9.9.9"),
        )
        .arg(
            Arg::new("input-port")
                .long("input-port")
                .value_name("PORT")
                .help("Input RTP stream port")
                .default_value("5000")
                .value_parser(clap::value_parser!(u16)),
        )
        .arg(
            Arg::new("output-host")
                .long("output-host")
                .value_name("HOST")
                .help("Output RTP stream host")
                .default_value("239.8.8.8"),
        )
        .arg(
            Arg::new("output-port")
                .long("output-port")
                .value_name("PORT")
                .help("Output RTP stream port")
                .default_value("6000")
                .value_parser(clap::value_parser!(u16)),
        )
        .arg(
            Arg::new("asset")
                .long("asset")
                .value_name("PATH")
                .help("Path to local asset video file")
                .default_value("/home/fstar/work/video-scheduler-gstreamer/videos/input_cut.mp4"),
        )
        .arg(
            Arg::new("hlslink")
                .long("hlslink")
                .value_name("URL")
                .help("HLS stream URL")
                .default_value("https://1404062696.rsc.cdn77.org/HLS/FIDO_SCTE.m3u8"),
        )
        .arg(
            Arg::new("verbose")
                .short('v')
                .long("verbose")
                .help("Enable verbose logging")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    // Extract arguments
    let input_host = matches.get_one::<String>("input-host").unwrap().clone();
    let input_port = *matches.get_one::<u16>("input-port").unwrap();
    let output_host = matches.get_one::<String>("output-host").unwrap().clone();
    let output_port = *matches.get_one::<u16>("output-port").unwrap();
    let asset_path = matches.get_one::<String>("asset").unwrap().clone();
    let hls_url = matches.get_one::<String>("hlslink").unwrap().clone();
    let verbose = matches.get_flag("verbose");

    // Validate asset file exists
    if !Path::new(&asset_path).exists() {
        error!("Asset file does not exist: {}", asset_path);
        process::exit(1);
    }

    info!("Starting GStreamer Pipeline with Compositor and Audio Mixer");
    info!("Input: {}:{}", input_host, input_port);
    info!("Output: {}:{}", output_host, output_port);
    info!("Asset: {}", asset_path);
    info!("HLS URL: {}", hls_url);
    info!("Pipeline will switch to asset after 1 minute");

    // Create pipeline configuration
    let config = PipelineConfig {
        input_host,
        input_port,
        output_host,
        output_port,
        asset_path,
        hls_url: Some(hls_url),
        verbose,
        ..Default::default()
    };

    // Set up signal handling for graceful shutdown
    let shutdown_signal = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
        info!("Received shutdown signal");
    };

    // Run the HLS pipeline
    let pipeline_task = tokio::spawn(async move {
        if let Err(e) = run_hls_pipeline(config).await {
            error!("Pipeline error: {}", e);
        }
    });

    // Wait for either the pipeline to complete or a shutdown signal
    tokio::select! {
        _ = pipeline_task => {
            info!("Pipeline completed");
        }
        _ = shutdown_signal => {
            info!("Received shutdown signal, stopping pipeline...");
        }
    }
}

/// Run the HLS GStreamer pipeline
async fn run_hls_pipeline(config: PipelineConfig) -> Result<()> {
    // Initialize GStreamer
    gstreamer::init()?;

    // Create and start the HLS pipeline
    let mut pipeline = HlsPipeline::new(config)?;
    pipeline.start().await?;

    // Keep the pipeline running
    tokio::signal::ctrl_c().await?;
    
    // Stop the pipeline
    pipeline.stop().await?;
    
    Ok(())
}
