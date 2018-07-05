//! CSV Virtual Table
//! Port of [csv](http://www.sqlite.org/cgi/src/finfo?name=ext/misc/csv.c) C extension.
//!
extern crate csv;

use std::os::raw::{c_char, c_int, c_void};
use std::path::Path;
use std::result;
use std::str;

use std::io::prelude::*;
use std::io::{self, SeekFrom};

use error::error_from_sqlite_code;
use ffi;
use types::Null;
use vtab::{
    dequote, escape_double_quote, parse_boolean, Context, IndexInfo, Module, VTab, VTabCursor,
    Values,
};
use {Connection, Error, Result};

/// Register the "csv" module. (with )
/// ```sql
/// CREATE VIRTUAL TABLE vtab USING siquery(
///   table = a serialized and stringifyed version of the table
///   [, schema=SCHEMA] -- Alternative CSV schema. 'CREATE TABLE x(col1 TEXT NOT NULL, col2 INT, ...);'
///   [, header=YES|NO] -- First row of CSV defines the names of columns if "yes". Default "no".
///   [, columns=N] -- Assume the CSV file contains N columns.
///   [, delimiter=C] -- CSV delimiter. Default ','.
///   [, quote=C] -- CSV quote. Default '"'. 0 means no quote.
/// );
/// ```
pub fn load_module(conn: &Connection) -> Result<()> {
    let aux: Option<()> = None;
    conn.create_module("siquery", SIQUERYModule(&SIQUERY_MODULE), aux)
}

init_module!(
    SIQUERY_MODULE,
    SIQUERYModule,
    SIQUERYTab,
    (),
    SIQUERYTabCursor,
    siquery_create,
    siquery_connect,
    siquery_best_index,
    siquery_disconnect,
    siquery_disconnect,
    siquery_open,
    siquery_close,
    siquery_filter,
    siquery_next,
    siquery_eof,
    siquery_column,
    siquery_rowid
);

#[repr(C)]
struct SIQUERYModule(&'static ffi::sqlite3_module);

impl SIQUERYModule {
    fn parameter(c_slice: &[u8]) -> Result<(&str, &str)> {
        let arg = try!(str::from_utf8(c_slice)).trim();
        let mut split = arg.split('=');
        if let Some(key) = split.next() {
            if let Some(value) = split.next() {
                let param = key.trim();
                let value = dequote(value);
                return Ok((param, value));
            }
        }
        Err(Error::ModuleError(format!("illegal argument: '{}'", arg)))
    }

    fn parse_byte(arg: &str) -> Option<u8> {
        if arg.len() == 1 {
            arg.bytes().next()
        } else {
            None
        }
    }
}

impl Module for SIQUERYModule {
    type Aux = ();
    type Table = SIQUERYTab;

    fn as_ptr(&self) -> *const ffi::sqlite3_module {
        self.0
    }

    fn connect(
        _: &mut ffi::sqlite3,
        _aux: Option<&()>,
        args: &[&[u8]],
    ) -> Result<(String, SIQUERYTab)> {
        if args.len() < 4 {
            return Err(Error::ModuleError("no table name specified".to_owned()));
        }

        let mut vtab = SIQUERYTab {
            base: ffi::sqlite3_vtab::default(),
            table: String::new().to_owned(),
            has_headers: false,
            delimiter: b',',
            quote: b'"',
            offset_first_row: csv::Position::new(),
        };
        let mut schema = None;
        let mut n_col = None;

        let args: &[&[u8]]   = &args[3..];
        for c_slice in args {
            let (param, value) = try!(SIQUERYModule::parameter(c_slice));
            match param {
                "table" => {
                    if value.is_empty(){
                        println!("no table entered")
                    }
                    else {
                        vtab.table = value.to_string();
                    }
                }
                "schema" => {
                    schema = Some(value.to_owned());
                }
                "columns" => {
                    if let Ok(n) = value.parse::<u16>() {
                        if n_col.is_some() {
                            return Err(Error::ModuleError(
                                "more than one 'columns' parameter".to_owned(),
                            ));
                        } else if n == 0 {
                            return Err(Error::ModuleError(
                                "must have at least one column".to_owned(),
                            ));
                        }
                        n_col = Some(n);
                    } else {
                        return Err(Error::ModuleError(format!(
                            "unrecognized argument to 'columns': {}",
                            value
                        )));
                    }
                }
                "header" => {
                    if let Some(b) = parse_boolean(value) {
                        vtab.has_headers = b;
                    } else {
                        return Err(Error::ModuleError(format!(
                            "unrecognized argument to 'header': {}",
                            value
                        )));
                    }
                }
                "delimiter" => {
                    if let Some(b) = SIQUERYModule::parse_byte(value) {
                        vtab.delimiter = b;
                    } else {
                        return Err(Error::ModuleError(format!(
                            "unrecognized argument to 'delimiter': {}",
                            value
                        )));
                    }
                }
                "quote" => {
                    if let Some(b) = SIQUERYModule::parse_byte(value) {
                        if b == b'0' {
                            vtab.quote = 0;
                        } else {
                            vtab.quote = b;
                        }
                    } else {
                        return Err(Error::ModuleError(format!(
                            "unrecognized argument to 'quote': {}",
                            value
                        )));
                    }
                }
                _ => {
                    return Err(Error::ModuleError(format!(
                        "unrecognized parameter '{}'",
                        param
                    )));
                }
            }
        }

        if vtab.table.is_empty() {
            return Err(Error::ModuleError("no table name specified".to_owned()));
        }

        let mut cols: Vec<String> = Vec::new();
        if vtab.has_headers || (n_col.is_none() && schema.is_none()) {
            let mut reader = vtab.reader();
            if vtab.has_headers {
                {
                    let mut headers = reader.headers().unwrap();
                    // headers ignored if cols is not empty
                    if n_col.is_none() && schema.is_none() {
                        cols = headers
                            .into_iter()
                            .map(|header| escape_double_quote(&header ).into_owned())
                            .collect();
                    }
                }
                vtab.offset_first_row = reader.position().clone();
            } else {
                let mut record = csv::ByteRecord::new();
                if try!(reader.read_byte_record(&mut record)) {
                    for (i, _) in record.iter().enumerate() {
                        cols.push(format!("c{}", i));
                    }
                }
            }
        } else if let Some(n_col) = n_col {
            for i in 0..n_col {
                cols.push(format!("c{}", i));
            }
        }

        if cols.is_empty() && schema.is_none() {
            return Err(Error::ModuleError("no column specified".to_owned()));
        }

        if schema.is_none() {
            let mut sql = String::from("CREATE TABLE x(");
            for (i, col) in cols.iter().enumerate() {
                sql.push('"');
                sql.push_str(col);
                sql.push_str("\" TEXT");
                if i == cols.len() - 1 {
                    sql.push_str(");");
                } else {
                    sql.push_str(", ");
                }
            }
            schema = Some(sql);
        }

        Ok((schema.unwrap().to_owned(), vtab))
    }

}

/// An instance of the CSV virtual table
#[repr(C)]
struct SIQUERYTab {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab,
    /// Name of the CSV file
    table: String,
    has_headers: bool,
    delimiter: u8,
    quote: u8,
    /// Offset to start of data
    offset_first_row: csv::Position,
}

impl SIQUERYTab {

    fn reader(&self) -> csv::Reader<io::Cursor<Vec<u8>>>{

        let mut s = self.table.as_str();
        let mut tab = String::from(s);
        tab = tab.replace("\\n", "\n");

        csv::ReaderBuilder::new()
            //.terminator(csv::Terminator::Any(b'\n'))
            .has_headers(self.has_headers)
            .delimiter(self.delimiter)
            .quote(self.quote)
            .from_reader(io::Cursor::new(tab.as_str().as_bytes().to_vec()))
    }
}

impl VTab for SIQUERYTab {
    type Cursor = SIQUERYTabCursor;

    // Only a forward full table scan is supported.
    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        info.set_estimated_cost(1_000_000.);
        Ok(())
    }

    fn open(&self) -> Result<SIQUERYTabCursor> {
        Ok(SIQUERYTabCursor::new(self.reader()))
    }
}

/// A cursor for the CSV virtual table
#[repr(C)]
struct SIQUERYTabCursor {
    /// Base class. Must be first
    base: ffi::sqlite3_vtab_cursor,
    /// The CSV reader object
    reader: csv::Reader<io::Cursor<Vec<u8>>>,
    /// Current cursor position used as rowid
    row_number: usize,
    /// Values of the current row
    cols: csv::StringRecord,
    eof: bool,
}

impl SIQUERYTabCursor {
    fn new(reader: csv::Reader<io::Cursor<Vec<u8>>>) -> SIQUERYTabCursor {
        SIQUERYTabCursor {
            base: ffi::sqlite3_vtab_cursor::default(),
            reader,
            row_number: 0,
            cols: csv::StringRecord::new(),
            eof: false,
        }
    }
}

impl VTabCursor for SIQUERYTabCursor {
    type Table = SIQUERYTab;

    fn vtab(&self) -> &SIQUERYTab {
        unsafe { &*(self.base.pVtab as *const SIQUERYTab) }
    }

    // Only a full table scan is supported.  So `filter` simply rewinds to
    // the beginning.
    fn filter(&mut self, _idx_num: c_int, _idx_str: Option<&str>, _args: &Values) -> Result<()> {
        {
            let offset_first_row = self.vtab().offset_first_row.clone();
            try!(self.reader.seek(offset_first_row));

        }
        self.row_number = 0;
        self.next()
    }
    fn next(&mut self) -> Result<()> {
        {
            self.eof = self.reader.is_done();
            if self.eof {
                return Ok(());
            }

            self.eof = !try!(self.reader.read_record(&mut self.cols));
        }

        self.row_number += 1;
        Ok(())
    }
    fn eof(&self) -> bool {
        self.eof
    }
    fn column(&self, ctx: &mut Context, col: c_int) -> Result<()> {
        if col < 0 || col as usize >= self.cols.len() {
            return Err(Error::ModuleError(format!(
                "column index out of bounds: {}",
                col
            )));
        }
        if self.cols.is_empty() {
            return ctx.set_result(&Null);
        }
        // TODO Affinity
        ctx.set_result(&self.cols[col as usize].to_owned())
    }
    fn rowid(&self) -> Result<i64> {
        Ok(self.row_number as i64)
    }
}


impl From<csv::Error> for Error {
    fn from(err: csv::Error) -> Error {
        use std::error::Error as StdError;
        Error::ModuleError(String::from(err.description()))
    }
}


#[cfg(test)]
mod test {

    extern crate csv;

    use vtab::siqtab;
    use {Connection, Result};

    use serde::ser::{Serialize, SerializeStruct, Serializer};
    use serde::de::{Deserialize, Deserializer, Visitor, SeqAccess, MapAccess};

    #[derive(Debug, Serialize, Deserialize)]
    pub struct OsVersion {
        pub name: String,
        pub platform_os: String,
        #[serde(skip_serializing_if="String::is_empty")]
        pub version: String,
        pub major: u32,
        pub minor: u32,
    }

    #[test]
    fn test_siqtab_module() {

        let mut wtr = csv::Writer::from_writer(vec![]);

        wtr.serialize(OsVersion {
            name: "WINDOWS1010".to_string(),
            platform_os: "WINDOWS".to_string(),
            version: "".to_string(),
            major: 0,
            minor: 0,
        });

        let db = Connection::open_in_memory().unwrap();
        siqtab::load_module(&db).unwrap();

        let command =  format!("{}{:?}{}", "CREATE VIRTUAL TABLE siqueryTab USING siquery(table=",String::from_utf8(wtr.into_inner().unwrap()).unwrap(), ", header=yes)");

        db.execute_batch(&command).unwrap();

        {
            let mut s = db.prepare("SELECT * FROM siqueryTab").unwrap();
            {
                let headers = s.column_names();
                assert_eq!(vec!["name", "platform_os", "major", "minor"], headers);
            }
        }
        db.execute_batch("DROP TABLE siqueryTab").unwrap();
    }
}
