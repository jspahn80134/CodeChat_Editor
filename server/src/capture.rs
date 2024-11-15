// src/capture.rs

use log::{error, info};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::{Client, Error as PgError, NoTls};

/*
The `Event` struct represents an event to be stored in the database.

Fields:
    - `user_id`: The ID of the user associated with the event.
    - `event_type`: The type of event (e.g., "keystroke", "file_open").
    - `timestamp`: The timestamp of when the event occurred.
    - `data`: Optional additional data associated with the event.

# Example

let event = Event {
    user_id: "user123".to_string(),
    event_type: "keystroke".to_string(),
    timestamp: "2023-10-01T12:34:56Z".to_string(),
    data: Some("Pressed key A".to_string()),
};

 */
#[derive(Deserialize, Debug)]
pub struct Event {
    pub user_id: String,
    pub event_type: String,
    pub timestamp: String,
    pub data: Option<String>,
}

/* The `Config` struct represents the database connection parameters read from `config.json`.

Fields:
    - `db_host`: The hostname or IP address of the database server.
    - `db_user`: The username for the database connection.
    - `db_password`: The password for the database connection.
    - `db_name`: The name of the database.

let config = Config {
    db_host: "localhost".to_string(),
    db_user: "your_db_user".to_string(),
    db_password: "your_db_password".to_string(),
    db_name: "your_db_name".to_string(),
};

*/
#[derive(Deserialize, Serialize, Debug)]
pub struct Config {
    pub db_host: String,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
}

/*
The `EventCapture` struct provides methods to interact with the database.
It holds a `tokio_postgres::Client` for database operations.

# Usage Example

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
     // Initialize logging using the configuration file
     log4rs::init_file("log4rs.yaml", Default::default())?;

     // Create an instance of EventCapture using the configuration file
     let event_capture = EventCapture::new("config.json").await?;

     // Create an event
     let event = Event {
         user_id: "user123".to_string(),
         event_type: "keystroke".to_string(),
         timestamp: "2023-10-01T12:34:56Z".to_string(),
         data: Some("Pressed key A".to_string()),
     };

     // Insert the event into the database
     event_capture.insert_event(event).await?;

     Ok(())
}
 */
pub struct EventCapture {
    db_client: Arc<Mutex<Client>>,
}

impl EventCapture {
    /*
        Creates a new `EventCapture` instance by reading the database connection parameters
        from the `config.json` file and connecting to the PostgreSQL database.

        # Arguments
        - `config_path`: The file path to the `config.json` file.

        # Returns
            A `Result` containing an `EventCapture` instance or a `Box<dyn std::error::Error>`.

    */
    pub async fn new<P: AsRef<Path>>(config_path: P) -> Result<Self, Box<dyn std::error::Error>> {
        // Read the configuration file
        let config_content = fs::read_to_string(config_path)?;
        let config: Config = serde_json::from_str(&config_content)?;

        // Build the connection string for the PostgreSQL database
        let conn_str = format!(
            "host={} user={} password={} dbname={}",
            config.db_host, config.db_user, config.db_password, config.db_name
        );

        println!("Attempting Capture Database COnnection...");

        // Connect to the database asynchronously
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;

        // Spawn a task to manage the database connection in the background
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("Database connection error: [{}]", e);
            }
        });

        info!(
            "Connected to database [{}] as user [{}]",
            config.db_name, config.db_user
        );

        Ok(EventCapture {
            db_client: Arc::new(Mutex::new(client)),
        })
    }

    /*
    Inserts an event into the database.

    # Arguments
    - `event`: An `Event` instance containing the event data to insert.

    # Returns
    A `Result` indicating success or containing a `tokio_postgres::Error`.

    # Example
    #[tokio::main]
    async fn main() -> Result<(), Box<dyn std::error::Error>> {
        let event_capture = EventCapture::new("config.json").await?;

        let event = Event {
            user_id: "user123".to_string(),
            event_type: "keystroke".to_string(),
            timestamp: "2023-10-01T12:34:56Z".to_string(),
            data: Some("Pressed key A".to_string()),
        };

        event_capture.insert_event(event).await?;
        Ok(())
    }
    */
    pub async fn insert_event(&self, event: Event) -> Result<(), PgError> {
        // SQL statement to insert the event into the 'events' table
        let stmt = "
            INSERT INTO events (user_id, event_type, timestamp, data)
            VALUES ($1, $2, $3, $4)
        ";

        // Acquire a lock on the database client for thread-safe access
        let client = self.db_client.lock().await;

        // Execute the SQL statement with the event data
        client
            .execute(
                stmt,
                &[
                    &event.user_id,
                    &event.event_type,
                    &event.timestamp,
                    &event.data,
                ],
            )
            .await?;

        info!("Event inserted into database: {:?}", event);

        Ok(())
    }
}

/*
Database Schema (SQL DDL)

The following SQL statement creates the `events` table used by this library:

CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    data TEXT
);

- **`id SERIAL PRIMARY KEY`**: Auto-incrementing primary key.
- **`user_id TEXT NOT NULL`**: The ID of the user associated with the event.
- **`event_type TEXT NOT NULL`**: The type of event.
- **`timestamp TEXT NOT NULL`**: The timestamp of the event.
- **`data TEXT`**: Optional additional data associated with the event.
**Note:** Ensure this table exists in your PostgreSQL database before using the library.
*/
