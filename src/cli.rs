use clap::Parser;

#[derive(Parser, Debug)]
pub struct Opt {
    /// Fuel node instance to connect to
    pub url: String,
    /// Block height to start from
    pub height: u32,
}