pub mod batch;
pub mod codec;
pub mod io;
pub mod packets;

/// Bedrock 1.26.60 uses network protocol 2225. This is also the protocol
/// version accepted by the Dragonfly/Gophertunnel 1.63 transport edge.
pub const CURRENT_PROTOCOL: i32 = 2225;

/// Minecraft version advertised by the server in its identity/skin metadata.
pub const MINECRAFT_VERSION: &str = "1.26.60";

/// Version de base du jeu annoncée dans StartGame et ResourcePackStack : `*`
/// demande au client les comportements de sa propre version, comme BDS
/// 1.26.52.3 (`dev-docs/34-BDS-RETRO-INGENIERIE.md`).
pub const BASE_GAME_VERSION: &str = "*";
