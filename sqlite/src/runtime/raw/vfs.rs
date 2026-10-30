use std::{mem, ptr};

use o3::base::num::bounded::NonZeroU32;

use crate::runtime::{Error, heap};

#[repr(C)]
pub(super) struct Vfs {
    api: libsqlite3_sys::sqlite3_vfs,
    source: *mut libsqlite3_sys::sqlite3_vfs,
    open: unsafe extern "C" fn(
        *mut libsqlite3_sys::sqlite3_vfs,
        libsqlite3_sys::sqlite3_filename,
        *mut libsqlite3_sys::sqlite3_file,
        std::ffi::c_int,
        *mut std::ffi::c_int,
    ) -> std::ffi::c_int,
    offset: usize,
}

#[repr(C)]
struct Mapping {
    api: libsqlite3_sys::sqlite3_io_methods,
    map: unsafe extern "C" fn(
        *mut libsqlite3_sys::sqlite3_file,
        std::ffi::c_int,
        std::ffi::c_int,
        std::ffi::c_int,
        *mut *mut std::ffi::c_void,
    ) -> std::ffi::c_int,
}

static mut VFS: mem::MaybeUninit<Vfs> = mem::MaybeUninit::uninit();

/// Installs the Unix VFS allocation boundary under the configuration permit.
impl Vfs {
    pub(super) unsafe fn install() -> Result<(), Error> {
        let source = unsafe { libsqlite3_sys::sqlite3_vfs_find(ptr::null()) };
        let source = ptr::NonNull::new(source).ok_or(Error::VfsMissing)?;
        let mut api = unsafe { source.as_ptr().read() };
        let native = api.xOpen.ok_or(Error::VfsOpen)?;
        let offset = Mapping::offset(api.szOsFile)?;
        api.szOsFile = (offset + mem::size_of::<Mapping>()) as std::ffi::c_int;
        api.zName = c"cartel".as_ptr();
        api.pNext = ptr::null_mut();
        api.xOpen = Some(open);
        let target = (&raw mut VFS).cast::<Vfs>();
        unsafe {
            target.write(Vfs {
                api,
                source: source.as_ptr(),
                open: native,
                offset,
            })
        };
        let code = unsafe { libsqlite3_sys::sqlite3_vfs_register(target.cast(), 1) };
        if code != libsqlite3_sys::SQLITE_OK {
            return Err(Error::VfsRegistration(crate::sys::Code::new(code)));
        }
        Ok(())
    }
}

impl Mapping {
    fn offset(bytes: std::ffi::c_int) -> Result<usize, Error> {
        use std::ffi::c_int;

        const MAX: u32 = ((c_int::MAX as usize - mem::size_of::<Mapping>())
            / mem::align_of::<Mapping>()
            * mem::align_of::<Mapping>()) as u32;
        let bytes =
            NonZeroU32::<{ mem::size_of::<libsqlite3_sys::sqlite3_file>() as u32 }, MAX>::new(
                bytes as u32,
            )
            .ok_or(Error::VfsFileSize { actual: bytes })?;
        Ok(bytes.into_usize().next_multiple_of(mem::align_of::<Self>()))
    }

    /// # Safety
    /// `file` is the exclusive, live output of the retained VFS's successful `xOpen`, with its valid methods table unchanged.
    /// `offset` locates the aligned `Mapping` tail reserved by `Vfs::install` within that same file allocation.
    unsafe fn install(file: *mut libsqlite3_sys::sqlite3_file, offset: usize) {
        let methods = unsafe { (*file).pMethods };
        if unsafe { (*methods).iVersion } < 2 {
            return;
        }
        let Some(map) = (unsafe { (*methods).xShmMap }) else {
            return;
        };
        let target = unsafe { file.byte_add(offset).cast::<Self>() };
        // SAFETY: the native table is readable, and Vfs::install reserves this aligned
        // Mapping tail within the exclusively owned file. Copy permits overlap;
        // the replacement is completely initialized before publication.
        unsafe {
            ptr::copy(methods, &raw mut (*target).api, 1);
            (&raw mut (*target).api.xShmMap).write(Some(mapping));
            (&raw mut (*target).map).write(map);
            (*file).pMethods = target.cast();
        }
    }
}

unsafe extern "C" fn open(
    vfs: *mut libsqlite3_sys::sqlite3_vfs,
    name: libsqlite3_sys::sqlite3_filename,
    file: *mut libsqlite3_sys::sqlite3_file,
    flags: std::ffi::c_int,
    output: *mut std::ffi::c_int,
) -> std::ffi::c_int {
    let vfs = unsafe { &*vfs.cast::<Vfs>() };
    use heap::raw::Foreign;

    let scope = Foreign::enter();
    let code = unsafe { (vfs.open)(vfs.source, name, file, flags, output) };
    drop(scope);
    if code == libsqlite3_sys::SQLITE_OK {
        unsafe { Mapping::install(file, vfs.offset) };
    }
    code
}

unsafe extern "C" fn mapping(
    file: *mut libsqlite3_sys::sqlite3_file,
    page: std::ffi::c_int,
    bytes: std::ffi::c_int,
    extend: std::ffi::c_int,
    output: *mut *mut std::ffi::c_void,
) -> std::ffi::c_int {
    let mapping = unsafe { &*(*file).pMethods.cast::<Mapping>() };
    use heap::raw::Foreign;

    let scope = Foreign::enter();
    let code = unsafe { (mapping.map)(file, page, bytes, extend, output) };
    drop(scope);
    code
}

const _: () = {
    assert!(mem::offset_of!(Vfs, api) == 0);
    assert!(mem::offset_of!(Mapping, api) == 0);
    assert!(mem::align_of::<Mapping>() <= 8);
    assert!(
        mem::size_of::<Mapping>()
            == mem::size_of::<libsqlite3_sys::sqlite3_io_methods>() + mem::size_of::<usize>()
    );
};
