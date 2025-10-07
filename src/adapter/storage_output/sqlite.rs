use std::error::Error;
use crate::models::output::OutputPort;
use crate::User;

pub struct SqliteAdapter {
    db: Database,
}

impl SqliteAdapter {
    pub fn new(path: &str) -> Result<Self, Box<dyn Error >> {
        let db = Database::new(path);
        db.init()?;
        Ok(SqliteAdapter { db })
    }
}

impl OutputPort<User> for SqliteAdapter {
    fn write(&mut self, data: &[User]) -> Result<(), Box<dyn Error>> {
        self.db.insert_user(data)?;
        Ok(())
    }
}



struct Database {
    conn: rusqlite::Connection,
}

impl Database {
    fn new(path: &str) -> Self {
        Database {
            conn: rusqlite::Connection::open(path).unwrap(),
        }
    }

    fn init(&self) -> Result<(), rusqlite::Error> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS users (
                    username TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    first_name TEXT NOT NULL,
                    last_name TEXT NOT NULL
            )", ()
        )?;

        Ok(())
    }

    fn insert_user(&self, users: &[User]) -> Result<(), rusqlite::Error> {
        let tx = self.conn.unchecked_transaction()?;

        for user in users {
            tx.execute(
                "INSERT INTO users (username, identifier, first_name, last_name)
                 VALUES (?1, ?2, ?3, ?4)",
                (
                    &user.username,
                    &user.identifier,
                    &user.first_name,
                    &user.last_name,
                ),
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    fn get_all_users(&self) -> Result<Vec<User>, rusqlite::Error> {

        let mut stmt = self.conn.prepare("SELECT * FROM users")?;
        let users_iter = stmt.query_map([], |row| {
            Ok( User {
                username: row.get(0)?,
                identifier: row.get(1)?,
                first_name: row.get(2)?,
                last_name: row.get(3)?,
            })
        })?;
        
        users_iter.collect::<Result<Vec<_>, _>>()
    }
}