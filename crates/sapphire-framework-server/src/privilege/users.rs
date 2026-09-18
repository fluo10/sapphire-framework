//! Resolving a [`UserSpec`](super::UserSpec) against the password database.

use crate::error::{Error, Result};

use super::UserSpec;

/// A user, resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedUser {
    /// Numeric user id.
    pub uid: u32,
    /// The user's primary group.
    pub gid: u32,
    /// Login name, needed to look up supplementary groups.
    pub name: String,
}

/// This process's real user id.
pub fn current_uid() -> u32 {
    // SAFETY: getuid has no preconditions and always succeeds.
    unsafe { libc::getuid() }
}

/// Look `spec` up in the password database.
///
/// Refuses root: neither identity in privilege separation may be root, and enforcing it here
/// means no caller can forget.
pub fn resolve(spec: &UserSpec) -> Result<ResolvedUser> {
    let resolved = match spec {
        UserSpec::Uid(uid) => by_uid(*uid)?,
        UserSpec::Name(name) => by_name(name)?,
    };
    if resolved.uid == 0 {
        return Err(Error::Privilege(format!(
            "{spec} is root; privilege separation needs two non-root users"
        )));
    }
    Ok(resolved)
}

/// Run `f` with a `getpw*_r` buffer, growing it while the call asks for more room.
fn with_passwd<F>(describe: &str, mut f: F) -> Result<ResolvedUser>
where
    F: FnMut(*mut libc::passwd, *mut libc::c_char, usize, *mut *mut libc::passwd) -> libc::c_int,
{
    let mut size = match unsafe { libc::sysconf(libc::_SC_GETPW_R_SIZE_MAX) } {
        n if n > 0 => n as usize,
        _ => 1024,
    };
    loop {
        let mut passwd: libc::passwd = unsafe { std::mem::zeroed() };
        let mut buf = vec![0 as libc::c_char; size];
        let mut result: *mut libc::passwd = std::ptr::null_mut();
        let rc = f(
            &raw mut passwd,
            buf.as_mut_ptr(),
            buf.len(),
            &raw mut result,
        );
        if rc == libc::ERANGE && size < 1 << 20 {
            size *= 2;
            continue;
        }
        if rc != 0 {
            return Err(Error::Privilege(format!(
                "could not look up {describe}: {}",
                std::io::Error::from_raw_os_error(rc)
            )));
        }
        if result.is_null() {
            return Err(Error::Privilege(format!("no such user: {describe}")));
        }
        // SAFETY: `result` points at `passwd`, which the call filled in, and `pw_name`
        // points into `buf`, which is alive for this block.
        let name = unsafe { std::ffi::CStr::from_ptr(passwd.pw_name) }
            .to_string_lossy()
            .into_owned();
        return Ok(ResolvedUser {
            uid: passwd.pw_uid,
            gid: passwd.pw_gid,
            name,
        });
    }
}

fn by_uid(uid: u32) -> Result<ResolvedUser> {
    with_passwd(&uid.to_string(), |pw, buf, len, out| {
        // SAFETY: all pointers are valid for the sizes passed.
        unsafe { libc::getpwuid_r(uid, pw, buf, len, out) }
    })
}

fn by_name(name: &str) -> Result<ResolvedUser> {
    let c_name = std::ffi::CString::new(name)
        .map_err(|_| Error::Privilege(format!("a user name may not contain a NUL: {name:?}")))?;
    with_passwd(name, |pw, buf, len, out| {
        // SAFETY: `c_name` is NUL-terminated and outlives the call; the rest are valid.
        unsafe { libc::getpwnam_r(c_name.as_ptr(), pw, buf, len, out) }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_user_spec_parses_a_name_or_a_number() {
        assert_eq!(
            "agent".parse::<UserSpec>().unwrap(),
            UserSpec::Name("agent".into())
        );
        assert_eq!("1001".parse::<UserSpec>().unwrap(), UserSpec::Uid(1001));
    }

    #[test]
    fn a_user_spec_round_trips_through_its_text_form() {
        for text in ["agent", "1001"] {
            let spec: UserSpec = text.parse().unwrap();
            assert_eq!(spec.to_string(), text);
        }
    }

    #[test]
    fn a_user_spec_deserialises_from_a_plain_string() {
        let spec: UserSpec = serde_json::from_value(serde_json::json!("agent")).unwrap();
        assert_eq!(spec, UserSpec::Name("agent".into()));
        let spec: UserSpec = serde_json::from_value(serde_json::json!("0")).unwrap();
        assert_eq!(spec, UserSpec::Uid(0));
    }

    #[test]
    fn an_empty_user_spec_is_refused() {
        assert!("".parse::<UserSpec>().is_err());
    }

    #[test]
    fn the_current_user_resolves_by_uid() {
        let resolved = resolve(&UserSpec::Uid(current_uid())).unwrap();
        assert_eq!(resolved.uid, current_uid());
        assert!(!resolved.name.is_empty());
    }

    #[test]
    fn the_current_user_resolves_by_name_to_the_same_uid() {
        let by_uid = resolve(&UserSpec::Uid(current_uid())).unwrap();
        let by_name = resolve(&UserSpec::Name(by_uid.name.clone())).unwrap();
        assert_eq!(by_name.uid, by_uid.uid);
        assert_eq!(by_name.gid, by_uid.gid);
    }

    #[test]
    fn an_unknown_user_is_an_error_not_a_panic() {
        let err = resolve(&UserSpec::Name("no-such-user-9f3a".into())).unwrap_err();
        assert!(err.to_string().contains("no-such-user-9f3a"), "{err}");
    }

    #[test]
    fn root_is_refused() {
        let err = resolve(&UserSpec::Uid(0)).unwrap_err();
        assert!(err.to_string().contains("root"), "{err}");
    }
}
