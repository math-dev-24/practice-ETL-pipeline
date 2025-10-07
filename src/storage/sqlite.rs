use crate::User;

pub struct Database {
    conn: rusqlite::Connection,
}

impl Database {
    pub fn new(path: &str) -> Self {
        Database {
            conn: rusqlite::Connection::open(path).unwrap(),
        }
    }

    pub fn init(&self) -> Result<(), rusqlite::Error> {
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

    pub fn insert_user(&self, users: &[User]) -> Result<(), rusqlite::Error> {
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

    pub fn get_all_users(&self) -> Result<Vec<User>, rusqlite::Error> {

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