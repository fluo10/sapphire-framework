//! Windows named pipe carrier, restricted to the current user (spec §2.3).

use std::ffi::c_void;

use tokio::net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions};
use windows_sys::Win32::Foundation::{CloseHandle, ERROR_PIPE_BUSY, HANDLE, LocalFree};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
};
use windows_sys::Win32::Security::{
    GetTokenInformation, PSECURITY_DESCRIPTOR, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER,
    TokenUser,
};
use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

use crate::conn::Connection;
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};

/// How many bytes a pipe instance buffers in each direction.
const PIPE_BUFFER: u32 = 64 * 1024;

fn last_error() -> Error {
    Error::Io(std::io::Error::last_os_error())
}

/// Decode a NUL-terminated wide string.
///
/// # Safety
/// `ptr` must point to a valid, NUL-terminated UTF-16 string.
unsafe fn wide_to_string(ptr: *const u16) -> String {
    let mut len = 0usize;
    // SAFETY: the caller guarantees a NUL terminator.
    while unsafe { *ptr.add(len) } != 0 {
        len += 1;
    }
    // SAFETY: `ptr` is valid for `len` elements by the loop above.
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(ptr, len) })
}

/// The current user's SID, in string form (`S-1-5-21-…`).
pub fn current_user_sid() -> Result<String> {
    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: `token` is a valid out-pointer; the handle is closed below.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) } == 0 {
        return Err(last_error());
    }
    struct TokenGuard(HANDLE);
    impl Drop for TokenGuard {
        fn drop(&mut self) {
            // SAFETY: the handle came from OpenProcessToken and is closed exactly once.
            unsafe { CloseHandle(self.0) };
        }
    }
    let _guard = TokenGuard(token);

    let mut needed: u32 = 0;
    // SAFETY: querying the required size with a null buffer is the documented pattern.
    unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &raw mut needed) };
    if needed == 0 {
        return Err(last_error());
    }
    let mut buf = vec![0u8; needed as usize];
    // SAFETY: `buf` is valid for `needed` bytes.
    if unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buf.as_mut_ptr().cast::<c_void>(),
            needed,
            &raw mut needed,
        )
    } == 0
    {
        return Err(last_error());
    }
    // SAFETY: on success the buffer holds a TOKEN_USER.
    let user = unsafe { &*buf.as_ptr().cast::<TOKEN_USER>() };

    let mut raw: *mut u16 = std::ptr::null_mut();
    // SAFETY: `user.User.Sid` is a valid SID owned by `buf`.
    if unsafe { ConvertSidToStringSidW(user.User.Sid, &raw mut raw) } == 0 {
        return Err(last_error());
    }
    // SAFETY: `raw` is a NUL-terminated wide string allocated by the call above.
    let sid = unsafe { wide_to_string(raw) };
    // SAFETY: `raw` was allocated with LocalAlloc by ConvertSidToStringSidW.
    unsafe { LocalFree(raw.cast::<c_void>()) };
    Ok(sid)
}

/// A self-freeing security descriptor granting full control to `sid` and to `SYSTEM`.
struct Descriptor(PSECURITY_DESCRIPTOR);

impl Descriptor {
    fn for_current_user() -> Result<Descriptor> {
        let sid = current_user_sid()?;
        // D:P            — a DACL, protected from inheritance
        // (A;;GA;;;<sid>) — allow generic-all to this user
        // (A;;GA;;;SY)    — allow generic-all to LocalSystem
        let sddl: Vec<u16> = format!("D:P(A;;GA;;;{sid})(A;;GA;;;SY)")
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let mut psd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        // SAFETY: `sddl` is NUL-terminated; `psd` is a valid out-pointer.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &raw mut psd,
                std::ptr::null_mut(),
            )
        } == 0
        {
            return Err(last_error());
        }
        Ok(Descriptor(psd))
    }

    fn attributes(&self) -> SECURITY_ATTRIBUTES {
        SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: self.0,
            bInheritHandle: 0,
        }
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        // SAFETY: allocated by ConvertStringSecurityDescriptorToSecurityDescriptorW.
        unsafe { LocalFree(self.0.cast::<c_void>()) };
    }
}

/// A named pipe listener.
///
/// Windows named pipes serve one client per instance, so accepting means handing out the
/// waiting instance and immediately creating the next one.
#[derive(Debug)]
pub struct PipeListener {
    name: String,
    next: Option<NamedPipeServer>,
}

impl PipeListener {
    fn create_instance(name: &str, first: bool) -> Result<NamedPipeServer> {
        let descriptor = Descriptor::for_current_user()?;
        let mut attrs = descriptor.attributes();
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(first)
            .reject_remote_clients(true)
            .in_buffer_size(PIPE_BUFFER)
            .out_buffer_size(PIPE_BUFFER);
        // SAFETY: `attrs` points at a live descriptor for the duration of the call, and
        // the pipe copies the security information it needs.
        let server = unsafe {
            options.create_with_security_attributes_raw(name, (&raw mut attrs).cast::<c_void>())
        }?;
        Ok(server)
    }

    /// Accept one connection.
    pub async fn accept(&mut self) -> Result<Connection> {
        let server = match self.next.take() {
            Some(s) => s,
            None => Self::create_instance(&self.name, false)?,
        };
        server.connect().await?;
        self.next = Some(Self::create_instance(&self.name, false)?);
        Ok(Connection::from_io(server))
    }
}

/// Create the pipe and wait for clients.
///
/// `first_pipe_instance` makes this fail if another process already owns the name, which is
/// what stops two servers from serving the same endpoint.
pub fn bind(endpoint: &Endpoint) -> Result<PipeListener> {
    let name = endpoint.pipe_name();
    let first = PipeListener::create_instance(&name, true)?;
    Ok(PipeListener {
        name,
        next: Some(first),
    })
}

/// Connect to `endpoint`, waiting briefly while every instance is busy.
// Only the tests call this until the spawn step re-exports a platform-independent
// `connect`; it is deliberately not re-exported from the crate root yet (see lib.rs).
#[allow(dead_code)]
pub async fn connect(endpoint: &Endpoint) -> Result<Connection> {
    let name = endpoint.pipe_name();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match ClientOptions::new().open(&name) {
            Ok(client) => return Ok(Connection::from_io(client)),
            Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                if std::time::Instant::now() >= deadline {
                    return Err(Error::Timeout("a free pipe instance"));
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            Err(err) => return Err(Error::Io(err)),
        }
    }
}

/// Is a server listening on `endpoint`?
///
/// Unlike the Unix carrier there is no file to go stale: the name exists exactly while a
/// process holds an instance.
// Only the tests call this until the spawn step re-exports a platform-independent
// `probe`; it is deliberately not re-exported from the crate root yet (see lib.rs).
#[allow(dead_code)]
pub async fn probe(endpoint: &Endpoint) -> Result<bool> {
    match ClientOptions::new().open(endpoint.pipe_name()) {
        Ok(_) => Ok(true),
        Err(err) if err.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(Error::Io(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{Message, Notification};

    fn endpoint(name: &str) -> Endpoint {
        Endpoint::in_dir(name, std::env::temp_dir())
    }

    #[test]
    fn the_current_user_sid_looks_like_a_sid() {
        let sid = current_user_sid().unwrap();
        assert!(sid.starts_with("S-1-"), "{sid}");
    }

    #[tokio::test]
    async fn a_client_reaches_the_listener() {
        let ep = endpoint(&format!("ipc-test-{}", std::process::id()));
        let mut listener = bind(&ep).unwrap();

        let server = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            conn.recv().await.unwrap().unwrap()
        });

        let client = connect(&ep).await.unwrap();
        let msg = Message::Notification(Notification {
            method: "hello".into(),
            params: serde_json::Value::Null,
        });
        client.send(msg.clone()).await.unwrap();
        assert_eq!(server.await.unwrap(), msg);
    }

    #[tokio::test]
    async fn probing_an_unused_name_reports_nothing_listening() {
        let ep = endpoint(&format!("ipc-absent-{}", std::process::id()));
        assert!(!probe(&ep).await.unwrap());
    }

    #[tokio::test]
    async fn probing_a_bound_name_reports_a_server() {
        let ep = endpoint(&format!("ipc-present-{}", std::process::id()));
        let _listener = bind(&ep).unwrap();
        assert!(probe(&ep).await.unwrap());
    }
}
