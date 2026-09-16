//! Workspace-relative paths.

use std::path::{Component, Path, PathBuf};

/// Whether the local filesystem is assumed case-insensitive.
pub const CASE_INSENSITIVE_FS: bool = cfg!(any(windows, target_os = "macos"));

/// Join a POSIX workspace-relative path onto `root`.
pub fn to_native(root: &Path, rel: &str) -> PathBuf {
    let mut out = root.to_path_buf();
    for segment in rel.split('/') {
        out.push(segment);
    }
    out
}

/// The POSIX workspace-relative form of `abs`.
pub fn rel_from_native(root: &Path, abs: &Path) -> Option<String> {
    let rel = abs.strip_prefix(root).ok()?;
    let mut parts = Vec::new();
    for component in rel.components() {
        match component {
            Component::Normal(s) => parts.push(s.to_str()?.to_owned()),
            _ => return None,
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("/"))
    }
}

/// A well-formed workspace-relative POSIX path that cannot escape the root.
/// Rejects paths with drive-letter prefixes in any segment (e.g., `C:`, `x:`) to prevent
/// escaping on Windows, where `PathBuf::push` replaces the whole path if given a drive prefix.
pub fn is_valid_rel(rel: &str) -> bool {
    if rel.is_empty() || rel.starts_with('/') || rel.contains('\\') {
        return false;
    }
    rel.split('/').all(|seg| {
        if seg.is_empty() || seg == "." || seg == ".." {
            return false;
        }
        let bytes = seg.as_bytes();
        if bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':' {
            return false;
        }
        true
    })
}

/// Whether this OS can hold a file at `rel`.
pub fn representable(rel: &str) -> bool {
    representable_on(rel, cfg!(windows))
}

/// Whether an OS (Windows when `windows`, otherwise POSIX) can hold a file at `rel`.
pub fn representable_on(rel: &str, windows: bool) -> bool {
    if rel.contains('\0') {
        return false;
    }
    if !windows {
        return true;
    }
    rel.split('/').all(|seg| {
        !seg.chars()
            .any(|c| matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*') || (c as u32) < 32)
            && !seg.ends_with('.')
            && !seg.ends_with(' ')
            && !is_reserved_windows_name(seg)
    })
}

fn is_reserved_windows_name(segment: &str) -> bool {
    let stem = segment
        .split('.')
        .next()
        .unwrap_or(segment)
        .to_ascii_uppercase();
    match stem.as_str() {
        "CON" | "PRN" | "AUX" | "NUL" => true,
        s if s.len() == 4 && (s.starts_with("COM") || s.starts_with("LPT")) => {
            matches!(s.as_bytes()[3], b'1'..=b'9')
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_round_trip() {
        let root = Path::new("root");
        let abs = to_native(root, "a/b/c.txt");
        assert_eq!(abs, Path::new("root").join("a").join("b").join("c.txt"));
        assert_eq!(rel_from_native(root, &abs).as_deref(), Some("a/b/c.txt"));
        assert_eq!(rel_from_native(root, root), None);
        assert_eq!(rel_from_native(root, Path::new("elsewhere/x")), None);
    }

    #[test]
    fn validity() {
        for ok in ["a", "a/b.md", "2026-08-25T10:00.md", ".app/x"] {
            assert!(is_valid_rel(ok), "{ok}");
        }
        for bad in [
            "", "/a", "a\\b", "C:/x", "c:x", "a//b", "./a", "a/../b", "a/.", "a/C:/b", "a/c:x",
            "dir/Z:",
        ] {
            assert!(!is_valid_rel(bad), "{bad}");
        }
    }

    #[test]
    fn windows_representability() {
        assert!(representable_on("a/b.txt", true));
        for bad in [
            "a:b.txt",
            "x/what?.md",
            "trailing.",
            "space ",
            "CON",
            "con.txt",
            "dir/LPT1.log",
            "a\u{1}b",
        ] {
            assert!(!representable_on(bad, true), "{bad}");
            assert!(
                representable_on(bad, false) || bad.contains('\u{0}'),
                "{bad} is fine elsewhere"
            );
        }
        assert!(representable_on("COM0", true), "COM0 is not reserved");
        assert!(representable_on("CONSOLE.txt", true));
        assert!(!representable_on("nul\u{0}", false));
    }
}
