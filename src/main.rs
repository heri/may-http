#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::fmt::Write;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use may_minihttp::{BodyWriter, HttpService, HttpServiceFactory, Request, Response};
use may_postgres::{self, Client, RowStream, Statement};
use oorandom::Rand32;
use serde::Serialize;
use smallvec::SmallVec;

mod utils {
    use may_postgres::ToSql;
    use std::cmp;

    pub fn get_query_param(query: &str) -> u16 {
        let q = if let Some(pos) = query.find("?q") {
            query.split_at(pos + 3).1.parse::<u16>().ok().unwrap_or(1)
        } else {
            1
        };
        cmp::min(500, cmp::max(1, q))
    }

    pub fn slice_iter<'a>(
        s: &'a [&'a (dyn ToSql + Sync)],
    ) -> impl ExactSizeIterator<Item = &'a dyn ToSql> + 'a {
        s.iter().map(|s| *s as _)
    }
}

#[derive(Serialize)]
pub struct User {
    id: String,
    firstName: String,
    lastName: String,
}

markup::define! {
    UsersTemplate(users: Vec<User>) {
        {markup::doctype()}
        html {
            head {
                title { "Users" }
            }
            body {
                table {
                    tr { th { "id" } th { "message" } }
                    @for u in {users} {
                        tr {
                            td { {u.id} }
                            td { {markup::raw(v_htmlescape::escape(&u.firstName))} }
                            td { {markup::raw(v_htmlescape::escape(&u.lastName))} }
                        }
                    }
                }
            }
        }
    }
}

struct PgConnectionPool {
    idx: AtomicUsize,
    clients: Vec<Arc<PgConnection>>,
}

impl PgConnectionPool {
    fn new(db_url: &str, size: usize) -> PgConnectionPool {
        let mut clients = Vec::with_capacity(size);
        for _ in 0..size {
            let client = PgConnection::new(db_url);
            clients.push(Arc::new(client));
        }

        PgConnectionPool {
            idx: AtomicUsize::new(0),
            clients,
        }
    }

    fn get_connection(&self) -> (Arc<PgConnection>, usize) {
        let idx = self.idx.fetch_add(1, Ordering::Relaxed);
        let len = self.clients.len();
        (self.clients[idx % len].clone(), idx)
    }
}

struct PgConnection {
    client: Client,
    user: Statement,
}

impl PgConnection {
    fn new(db_url: &str) -> Self {
        let client = may_postgres::connect(db_url).unwrap();
        let user = client
            .prepare("SELECT id, firstName, lastName FROM users WHERE id=$1")
            .unwrap();

        PgConnection {
            client,
            user
        }
    }

    fn get_user(&self, id: String) -> Result<User, may_postgres::Error> {
        let row = self.client.query_one(&self.user, &[&id])?;
        Ok(User {
            id: row.get(0),
            firstName: row.get(1),
            lastName: row.get(2),
        })
    }

    fn get_users(
        &self
    ) -> Result<Vec<User>, may_postgres::Error> {

        let rows = self.client.simple_query("SELECT id, firstName, lastName FROM users LIMIT 10");

        let mut users = Vec::with_capacity(10);
        match rows.next().transpose()? {
            Some(user) => users.push(User {
                id: row.get(0),
                firstName: row.get(1),
                lastName: row.get(2),
            }),
            None => unreachable!(),
        }
        Ok(users)
    }

    fn update(&self, id: String, firstName: String, lastName: String) -> Result<Vec<User>, may_postgres::Error> {

        let mut update = String::with_capacity(120 + 12 * num);
        update.push_str("UPDATE users SET firstName = $1, lastName = $2 FROM (VALUES ");
        update.push_str(" WHERE id = $3");

        self.client.simple_query(&update, id, firstName, lastName)?;
        Ok(users)
    }
}

struct App {
    db: Arc<PgConnection>
}

impl HttpService for App {
    fn call(&mut self, req: Request, rsp: &mut Response) -> io::Result<()> {
        // Bare-bones router
        match req.path() {
            "/users" => {
                let users = self.db.get_users(q).unwrap();
                rsp.header("Content-Type: text/html; charset=utf-8");
                write!(rsp.body_mut(), "{}", UsersTemplate { users }).unwrap();
            }
            p if p.starts_with("/webhook") => {
                let q = utils::get_query_param(p) as usize;
                let user = self.db.update(q, &mut self.rng).unwrap();
                rsp.header("Content-Type: application/json");
                serde_json::to_writer(BodyWriter(rsp.body_mut()), &user)?;
            }
            _ => {
                rsp.status_code("404", "Not Found");
            }
        }

        Ok(())
    }
}

struct HttpServer {
    db_pool: PgConnectionPool,
}

impl HttpServiceFactory for HttpServer {
    type Service = App;

    fn new_service(&self) -> Self::Service {
        let (db, idx) = self.db_pool.get_connection();
        App { db }
    }
}

fn main() {
    may::config().set_pool_capacity(10000);
    let server = HttpServer {
        db_pool: PgConnectionPool::new(
            "postgres://user:pass@users-database/users",
            num_cpus::get(),
        ),
    };
    server.start("0.0.0.0:8080").unwrap().join().unwrap();
}
