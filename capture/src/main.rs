use warp::Filter;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use simplelog::*;
use std::fs::File;
use std::io::Write;

#[derive(Deserialize, Serialize, Debug, Clone)]
struct Event {
    event_type: String,
    timestamp: u64,
    content: Option<String>,
}

#[derive(Clone)]
struct EventStore {
    events: Arc<Mutex<HashMap<String, Vec<Event>>>>,
}

impl EventStore {
    fn new() -> Self {
        EventStore {
            events: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn add_event(&self, session_id: &str, event: Event) {
        let mut events = self.events.lock().unwrap();
        events.entry(session_id.to_string()).or_default().push(event.clone());

        // Log the event to a file
        let log_entry = format!("{:?}\n", event);
        let mut file = File::options().append(true).open("events.log").unwrap();
        file.write_all(log_entry.as_bytes()).unwrap();
    }

    fn get_events(&self, session_id: &str) -> Option<Vec<Event>> {
        let events = self.events.lock().unwrap();
        events.get(session_id).cloned()
    }
}

#[tokio::main]
async fn main() {
    // Initialize the logger
    CombinedLogger::init(vec![
        WriteLogger::new(
            LevelFilter::Info,
            Config::default(),
            File::create("events.log").unwrap(),
        ),
    ])
    .unwrap();

    let store = EventStore::new();

    let store_filter = warp::any().map(move || store.clone());

    let log_event = warp::path("log_event")
        .and(warp::post())
        .and(warp::body::json())
        .and(store_filter.clone())
        .and_then(handle_log_event);

    let get_events = warp::path("get_events")
        .and(warp::get())
        .and(warp::query::<HashMap<String, String>>())
        .and(store_filter)
        .and_then(handle_get_events);

    let routes = log_event.or(get_events);

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

async fn handle_log_event(event: Event, store: EventStore) -> Result<impl warp::Reply, warp::Rejection> {
    if let Some(session_id) = event.content.clone() {
        store.add_event(&session_id, event);
        Ok(warp::reply::json(&"Event logged"))
    } else {
        Ok(warp::reply::json(&"Missing session_id"))
    }
}

async fn handle_get_events(params: HashMap<String, String>, store: EventStore) -> Result<impl warp::Reply, warp::Rejection> {
    if let Some(session_id) = params.get("session_id") {
        if let Some(events) = store.get_events(session_id) {
            Ok(warp::reply::json(&events))
        } else {
            Ok(warp::reply::json(&"No events found for session_id"))
        }
    } else {
        Ok(warp::reply::json(&"Missing session_id"))
    }
}
