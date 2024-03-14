use crate::connection::handle::ConnectionHandle;
use crate::connection::LogSettings;
use crate::connection::{ConnectionState, Statements};
use crate::error::Error;
use crate::{SqliteConnectOptions, SqliteError};
use libsqlite3_sys::{
    sqlite3, sqlite3_busy_timeout, sqlite3_db_config, sqlite3_extended_result_codes, sqlite3_free,
    sqlite3_load_extension, sqlite3_open_v2,
};
use rusqlite::{Connection, OpenFlags};
use sqlx_core::common::DebugFn;
use sqlx_core::IndexMap;
use std::borrow::Cow;
use std::ffi::{c_void, CStr, CString};
use std::io;
use std::os::raw::c_int;
use std::path::{Path, PathBuf};
use std::ptr::{addr_of_mut, null, null_mut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

// This was originally `AtomicU64` but that's not supported on MIPS (or PowerPC):
// https://github.com/launchbadge/sqlx/issues/2859
// https://doc.rust-lang.org/stable/std/sync/atomic/index.html#portability
static THREAD_ID: AtomicUsize = AtomicUsize::new(0);

enum SqliteLoadExtensionMode {
    /// Enables only the C-API, leaving the SQL function disabled.
    Enable,
    /// Disables both the C-API and the SQL function.
    DisableAll,
}

impl SqliteLoadExtensionMode {
    fn as_int(self) -> c_int {
        match self {
            SqliteLoadExtensionMode::Enable => 1,
            SqliteLoadExtensionMode::DisableAll => 0,
        }
    }
}

pub struct EstablishParams {
    filename: PathBuf,
    open_flags: OpenFlags,
    busy_timeout: Duration,
    statement_cache_capacity: usize,
    log_settings: LogSettings,
    extensions: IndexMap<PathBuf, Option<String>>,
    modify_fns: Vec<Arc<Mutex<DebugFn<dyn FnMut(&mut rusqlite::Connection) + Send + Sync>>>>,
    pub(crate) thread_name: String,
    pub(crate) command_channel_size: usize,
    #[cfg(feature = "regexp")]
    register_regexp_function: bool,
}

impl EstablishParams {
    pub fn from_options(options: &SqliteConnectOptions) -> Result<Self, Error> {
        let mut filename = options
            .filename
            .to_str()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "filename passed to SQLite must be valid UTF-8",
                )
            })?
            .to_owned();

        // By default, we connect to an in-memory database.
        // [SQLITE_OPEN_NOMUTEX] will instruct [sqlite3_open_v2] to return an error if it
        // cannot satisfy our wish for a thread-safe, lock-free connection object

        let mut flags = if options.serialized {
            OpenFlags::SQLITE_OPEN_FULL_MUTEX
        } else {
            OpenFlags::SQLITE_OPEN_NO_MUTEX
        };

        flags |= if options.read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        } else if options.create_if_missing {
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
        };

        if options.in_memory {
            flags |= OpenFlags::SQLITE_OPEN_MEMORY;
        }

        flags |= if options.shared_cache {
            OpenFlags::SQLITE_OPEN_SHARED_CACHE
        } else {
            OpenFlags::SQLITE_OPEN_PRIVATE_CACHE
        };

        let mut query_params: Vec<String> = vec![];

        if options.immutable {
            query_params.push("immutable=true".into())
        }

        if let Some(vfs) = &options.vfs {
            query_params.push(format!("vfs={vfs}"))
        }

        if !query_params.is_empty() {
            filename = format!(
                "file:{}?{}",
                urlencoding::encode(&filename),
                query_params.join("&")
            );
            flags |= OpenFlags::SQLITE_OPEN_URI;
        }

        // let filename = CString::new(filename).map_err(|_| {
        //     io::Error::new(
        //         io::ErrorKind::InvalidData,
        //         "filename passed to SQLite must not contain nul bytes",
        //     )
        // })?;

        let extensions: IndexMap<_, _> = options
            .extensions
            .iter()
            .map(|(name, entry)| {
                (
                    PathBuf::from(name.to_string()),
                    entry.as_deref().map(|e| e.to_string()),
                )
            })
            .collect();
        // .iter()
        // .map(|(name, entry)| {
        //     let entry = entry
        //         .as_ref()
        //         .map(|e| {
        //             CString::new(e.as_bytes()).map_err(|_| {
        //                 io::Error::new(
        //                     io::ErrorKind::InvalidData,
        //                     "extension entrypoint names passed to SQLite must not contain nul bytes"
        //                 )
        //             })
        //         })
        //         .transpose()?;
        //     Ok((
        //         CString::new(name.as_bytes()).map_err(|_| {
        //             io::Error::new(
        //                 io::ErrorKind::InvalidData,
        //                 "extension names passed to SQLite must not contain nul bytes",
        //             )
        //         })?,
        //         entry,
        //     ))
        // })
        // .collect::<Result<IndexMap<String, Option<String>>, io::Error>>()?;

        let thread_id = THREAD_ID.fetch_add(1, Ordering::AcqRel);

        Ok(Self {
            filename: filename.into(),
            open_flags: flags,
            busy_timeout: options.busy_timeout,
            statement_cache_capacity: options.statement_cache_capacity,
            log_settings: options.log_settings.clone(),
            extensions,
            thread_name: (options.thread_name)(thread_id as u64),
            command_channel_size: options.command_channel_size,
            modify_fns: options.modify_fns.clone(),
            #[cfg(feature = "regexp")]
            register_regexp_function: options.register_regexp_function,
        })
    }

    // // Enable extension loading via the db_config function, as recommended by the docs rather
    // // than the more obvious `sqlite3_enable_load_extension`
    // // https://www.sqlite.org/c3ref/db_config.html
    // // https://www.sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigenableloadextension
    // unsafe fn sqlite3_set_load_extension(
    //     db: *mut sqlite3,
    //     mode: SqliteLoadExtensionMode,
    // ) -> Result<(), Error> {
    //     let status = sqlite3_db_config(
    //         db,
    //         SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
    //         mode.as_int(),
    //         null::<i32>(),
    //     );

    //     if status != SQLITE_OK {
    //         return Err(Error::Database(Box::new(SqliteError::new(db))));
    //     }

    //     Ok(())
    // }

    pub(crate) fn establish(&self) -> Result<ConnectionState, Error> {
        // let mut handle = null_mut();

        // <https://www.sqlite.org/c3ref/open.html>
        // let mut status = unsafe {
        //     sqlite3_open_v2(self.filename.as_ptr(), &mut handle, self.open_flags, null())
        // };
        let mut conn = Connection::open_with_flags(&self.filename, self.open_flags).unwrap();
        for func in &self.modify_fns {
            (func.lock().unwrap())(&mut conn);
        }
        // if handle.is_null() {
        //     // Failed to allocate memory
        //     return Err(Error::Io(io::Error::new(
        //         io::ErrorKind::OutOfMemory,
        //         "SQLite is unable to allocate memory to hold the sqlite3 object",
        //     )));
        // }

        // SAFE: tested for NULL just above
        // This allows any returns below to close this handle with RAII
        let handle = ConnectionHandle::new(conn);

        // if status != SQLITE_OK {
        //     return Err(Error::Database(Box::new(SqliteError::new(handle.as_ptr()))));
        // }

        // Enable extended result codes
        // https://www.sqlite.org/c3ref/extended_result_codes.html
        // unsafe {
        //     // NOTE: ignore the failure here
        //     sqlite3_extended_result_codes(handle.as_ptr(), 1);
        // }

        if !self.extensions.is_empty() {
            // Enable loading extensions
            // unsafe {
            //     Self::sqlite3_set_load_extension(handle.as_ptr(), SqliteLoadExtensionMode::Enable)?;
            // }

            for (path, entry) in self.extensions.iter() {
                unsafe { handle.inner().load_extension(path, entry.as_deref()) };
                // `sqlite3_load_extension` is unusual as it returns its errors via an out-pointer
                // // rather than by calling `sqlite3_errmsg`
                // let mut error = null_mut();
                // status = unsafe {
                //     sqlite3_load_extension(
                //         handle.as_ptr(),
                //         ext.0.as_ptr(),
                //         ext.1.as_ref().map_or(null(), |e| e.as_ptr()),
                //         addr_of_mut!(error),
                //     )
                // };
                unsafe {
                    handle.inner().load_extension_enable();
                }

                // if status != SQLITE_OK {
                //     // SAFETY: We become responsible for any memory allocation at `&error`, so test
                //     // for null and take an RAII version for returns
                //     let err_msg = if !error.is_null() {
                //         unsafe {
                //             let e = CStr::from_ptr(error).into();
                //             sqlite3_free(error as *mut c_void);
                //             e
                //         }
                //     } else {
                //         CString::new("Unknown error when loading extension")
                //             .expect("text should be representable as a CString")
                //     };
                //     return Err(Error::Database(Box::new(SqliteError::extension(
                //         handle.as_ptr(),
                //         &err_msg,
                //     ))));
                // }
            } // Preempt any hypothetical security issues arising from leaving ENABLE_LOAD_EXTENSION
              // on by disabling the flag again once we've loaded all the requested modules.
              // Fail-fast (via `?`) if disabling the extension loader didn't work for some reason,
              // avoids an unexpected state going undetected.
              // unsafe {
              //     Self::sqlite3_set_load_extension(
              //         handle.as_ptr(),
              //         SqliteLoadExtensionMode::DisableAll,
              //     )?;
              // }
            unsafe {
                handle.inner().load_extension_disable();
            }
        }

        #[cfg(feature = "regexp")]
        if self.register_regexp_function {
            // configure a `regexp` function for sqlite, it does not come with one by default
            let status = crate::regexp::register(handle.as_ptr());
            if status != SQLITE_OK {
                return Err(Error::Database(Box::new(SqliteError::new(handle.as_ptr()))));
            }
        }

        // Configure a busy timeout
        // This causes SQLite to automatically sleep in increasing intervals until the time
        // when there is something locked during [sqlite3_step].
        //
        // We also need to convert the u128 value to i32, checking we're not overflowing.
        let ms = i32::try_from(self.busy_timeout.as_millis())
            .expect("Given busy timeout value is too big.");
        handle
            .inner()
            .busy_timeout(Duration::from_millis(ms as u64));

        // status = unsafe { sqlite3_busy_timeout(handle.as_ptr(), ms) };

        // if status != SQLITE_OK {
        //     return Err(Error::Database(Box::new(SqliteError::new(handle.as_ptr()))));
        // }

        Ok(ConnectionState {
            handle,
            statements: Statements::new(self.statement_cache_capacity),
            transaction_depth: 0,
            log_settings: self.log_settings.clone(),
            progress_handler_callback: None,
        })
    }
}
