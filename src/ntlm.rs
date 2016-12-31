///! windows authentication for TDS

#[cfg(windows)]
pub mod sso {
    extern crate winapi;
    extern crate secur32;

    use std::slice;
    use std::ptr;
    use std::mem;
    use std::ops::Deref;
    use std::io;

    static NTLM_PROVIDER: &'static [u8] = b"NTLM\0";

    const INIT_REQUESTS: winapi::c_ulong =
        winapi::ISC_REQ_CONFIDENTIALITY |
        winapi::ISC_REQ_REPLAY_DETECT |
        winapi::ISC_REQ_SEQUENCE_DETECT |
        winapi::ISC_REQ_CONNECTION |
        winapi::ISC_REQ_DELEGATE |
        winapi::ISC_REQ_ALLOCATE_MEMORY;

    pub struct NtlmSso {
        ctx: Option<SecurityContext>,
        cred: NtlmCred,
    }


    impl NtlmSso {
        pub fn new() -> Result<(NtlmSso, ContextBuffer), io::Error> {
            unsafe {
                let mut handle = mem::zeroed();
                // accquire the initial token (negotiate for either kerberos or NTLM)
                let ret = secur32::AcquireCredentialsHandleA(ptr::null_mut(),
                                                            NTLM_PROVIDER.as_ptr() as *mut i8,
                                                            winapi::SECPKG_CRED_OUTBOUND,
                                                            ptr::null_mut(),
                                                            ptr::null_mut(),
                                                            None,
                                                            ptr::null_mut(),
                                                            &mut handle,
                                                            ptr::null_mut());
                let cred = match ret {
                    winapi::SEC_E_OK => NtlmCred(handle),
                    err => return Err(io::Error::from_raw_os_error(err as i32)),
                };

                let mut sso = NtlmSso {
                    ctx: None,
                    cred: cred,
                };

                let buffer = try!(sso.next_bytes(None)).unwrap();
                Ok((sso, buffer))
            }
        }

        pub fn next_bytes<'a>(&mut self, in_bytes: Option<&'a [u8]>) -> Result<Option<ContextBuffer>, io::Error> {
            unsafe {
                let mut ctx = None;
                let ctx_ptr = if let Some(ref mut ctx_ptr) = self.ctx {
                    &mut ctx_ptr.0
                } else {
                    ctx = Some(mem::zeroed());
                    ctx.as_mut().unwrap() as *mut _
                };
                let has_in_bytes = in_bytes.is_some();
                let mut inbuf = [secbuf(winapi::SECBUFFER_TOKEN, in_bytes)];
                let mut inbuf_desc = secbuf_desc(&mut inbuf);

                // differentiate between the first and subsequent calls
                let (ctx_ptr_in, inbuf_ptr) = if has_in_bytes {
                    (ctx_ptr, &mut inbuf_desc as *mut _)
                } else {
                    (ptr::null_mut(), ptr::null_mut())
                };

                let mut outbuf = [secbuf(winapi::SECBUFFER_TOKEN, None)];
                let mut outbuf_desc = secbuf_desc(&mut outbuf);
                // create a token message
                let mut attrs = 0u32;
                let ret = secur32::InitializeSecurityContextW(&mut self.cred.0,
                                                    ctx_ptr_in,
                                                    ptr::null_mut(),
                                                    INIT_REQUESTS,
                                                    0,
                                                    winapi::SECURITY_NETWORK_DREP,
                                                    inbuf_ptr,
                                                    0,
                                                    ctx_ptr,
                                                    &mut outbuf_desc,
                                                    &mut attrs,
                                                    ptr::null_mut());
                match ret {
                    winapi::SEC_E_OK | winapi::SEC_I_CONTINUE_NEEDED => {
                        if let Some(new_ctx) = ctx {
                            self.ctx = Some(SecurityContext(new_ctx));
                        }
                        if outbuf[0].cbBuffer > 0 {
                            Ok(Some(ContextBuffer(outbuf[0])))
                        } else {
                            Ok(None)
                        }
                    },
                    err => Err(io::Error::from_raw_os_error(err)),
                }
            }
        }
    }

    // some helper stuff imported frm schannel.rs
    pub struct NtlmCred(winapi::CredHandle);

    impl Drop for NtlmCred {
        fn drop(&mut self) {
            unsafe {
                secur32::FreeCredentialsHandle(&mut self.0);
            }
        }
    }

    pub struct SecurityContext(winapi::CtxtHandle);

    impl Drop for SecurityContext {
        fn drop(&mut self) {
            unsafe {
                secur32::DeleteSecurityContext(&mut self.0);
            }
        }
    }

    pub struct ContextBuffer(pub winapi::SecBuffer);

    impl Drop for ContextBuffer {
        fn drop(&mut self) {
            unsafe {
                secur32::FreeContextBuffer(self.0.pvBuffer);
            }
        }
    }

    impl Deref for ContextBuffer {
        type Target = [u8];

        fn deref(&self) -> &[u8] {
            unsafe { slice::from_raw_parts(self.0.pvBuffer as *const _, self.0.cbBuffer as usize) }
        }
    }


    unsafe fn secbuf(buftype: winapi::c_ulong,
                     bytes: Option<&[u8]>) -> winapi::SecBuffer {
        let (ptr, len) = match bytes {
            Some(bytes) => (bytes.as_ptr(), bytes.len() as winapi::c_ulong),
            None => (ptr::null(), 0),
        };
        winapi::SecBuffer {
            BufferType: buftype,
            cbBuffer: len,
            pvBuffer: ptr as *mut winapi::c_void,
        }
    }

    unsafe fn secbuf_desc(bufs: &mut [winapi::SecBuffer]) -> winapi::SecBufferDesc {
        winapi::SecBufferDesc {
            ulVersion: winapi::SECBUFFER_VERSION,
            cBuffers: bufs.len() as winapi::c_ulong,
            pBuffers: bufs.as_mut_ptr(),
        }
    }
}
