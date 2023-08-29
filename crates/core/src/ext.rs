use sqlite_nostd::{Connection, Destructor, ManagedStmt, ResultCode, sqlite3};

pub trait SafeManagedStmt {
    fn exec(&self) -> Result<(), ResultCode>;
}

impl SafeManagedStmt for ManagedStmt {
    fn exec(&self) -> Result<(), ResultCode> {
        loop {
            let rs = self.step()?;
            if rs == ResultCode::ROW {
                continue;
            }

            self.reset()?;
            if rs == ResultCode::DONE {
                break;
            } else {
                return Err(rs);
            }
        }
        Ok(())
    }
}


pub trait ExtendedDatabase {
    fn exec_text(&self, sql: &str, param: &str) -> Result<(), ResultCode>;
}

impl ExtendedDatabase for *mut sqlite3 {
    fn exec_text(&self, sql: &str, param: &str) -> Result<(), ResultCode> {
        let statement = self.prepare_v2(sql)?;
        statement.bind_text(1, param, Destructor::STATIC)?;

        statement.exec()?;
        Ok(())
    }
}
