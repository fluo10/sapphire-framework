use thiserror::Error;

/// 台帳ファイルの読み書きで起きる失敗。
#[derive(Debug, Error)]
pub enum Error {
    /// ファイル操作が失敗した。
    #[error("registry io error: {0}")]
    Io(#[from] std::io::Error),

    /// パース・保存に失敗した、id / name が重複していた、または
    /// セレクタがどのエントリにも解決しなかった。
    #[error("registry file error: {0}")]
    File(String),
}

pub type Result<T> = std::result::Result<T, Error>;
