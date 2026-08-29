//! 台帳ファイル共通の書き出し。
//!
//! 内容は `users.rs` / `devices.rs` がそれぞれ組み立てる。ここが受け持つのは
//! 「ヘッダ + 本文を、途中で壊れない形で置く」ことだけ。

use std::path::Path;

use crate::error::{Error, Result};

/// `header` + 空行 + `body` を **一時ファイル → rename** で書き出す。
///
/// その場で truncate すると、書き込み中にクラッシュした瞬間に台帳が消える。
/// `device_id` はジャーナルのフロントマターに焼かれていて、台帳を失うと過去の
/// 参照が解決できなくなるので、`KeyStore::save_entries` と同じ手口を取る。
///
/// `keys.rs` と違って 0600 では作らない。この台帳に秘密は無く（トークンは
/// 鍵ファイル側にある）、ワークスペースごと同期される前提のファイルなので、
/// 所有者限定のパーミッションは意味を持たない。
pub(crate) fn write_atomic(path: &Path, header: &str, body: &str) -> Result<()> {
    use std::io::Write as _;

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;

    let file_name = path
        .file_name()
        .ok_or_else(|| Error::File(format!("{} is not a file path", path.display())))?;
    let mut tmp_name = file_name.to_os_string();
    tmp_name.push(".tmp");
    let tmp = parent.join(tmp_name);

    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(header.as_bytes())?;
        file.write_all(b"\n")?;
        file.write_all(body.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })?;
    Ok(())
}
