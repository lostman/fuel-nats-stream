use clap::Parser;

#[derive(Parser, Debug)]
pub struct Opt {
    /// Fuel node instance to connect to
    pub url: String,
    /// Block height to start from. Defaults to the latest block height.
    pub height: Option<Height>,
}

#[derive(Debug, Clone)]
pub enum Height {
    Latest,
    Numeric(u32),
}

impl std::str::FromStr for Height {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "latest" {
            Ok(Height::Latest)
        } else {
            s.parse::<u32>()
                .map(Height::Numeric)
                .map_err(|_| "Invalid number format for height".to_string())
        }
    }
}
