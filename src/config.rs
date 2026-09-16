use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tracing::{info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentConfig {
    pub token: Option<String>,
    pub relay: Option<String>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            token: None,
            relay: Some("wss://ws.ezbchat.fun/ws".to_string()),
        }
    }
}

impl AgentConfig {
    pub fn config_path() -> PathBuf {
        if let Some(home) = dirs_home() {
            let config_dir = home.join(".config").join("tunnelit");
            let _ = fs::create_dir_all(&config_dir);
            config_dir.join("agent.json")
        } else {
            PathBuf::from("tunnelit-agent.json")
        }
    }

    pub fn load() -> Self {
        let path = Self::config_path();
        if path.exists() {
            if let Ok(content) = fs::read_to_string(&path) {
                if let Ok(cfg) = serde_json::from_str::<AgentConfig>(&content) {
                    return cfg;
                }
            }
        }
        Self::default()
    }

    pub fn save(&self) {
        let path = Self::config_path();
        if let Ok(json) = serde_json::to_string_pretty(self) {
            if let Err(e) = fs::write(&path, json) {
                warn!("Failed to save config to {:?}: {}", path, e);
            } else {
                info!("Saved configuration to {:?}", path);
            }
        }
    }
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

