mod context;
mod logger;
mod module_manager;
mod title;

use crate::Payload;

// Pull in your library module containing CockatielClient and generated protobufs
use lib_cockatiel;

use context::Context;
use logger::Logger;
use module_manager::ModuleManager;
use title::Title;

use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers,
        MouseButton, MouseEvent, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Size},
};
use std::{io, time::Duration};
use tokio::sync::mpsc;

#[derive(Clone, Copy)]
pub enum ActivePane {
    TopLeft,
    TopRight,
    BottomLeft,
    BottomRight,
}

#[derive(Clone, Copy, PartialEq)]
pub enum DragTarget {
    None,
    HorizontalSplit,
    TopVerticalSplit,
    BottomVerticalSplit,
}

pub struct App {
    pub title: Title,
    pub module_manager: ModuleManager,
    pub logger: Logger,
    pub quadrant_context: Context,

    pub split_y: u16,
    pub top_split_x: u16,
    pub bottom_split_x: u16,

    pub active_pane: ActivePane,
    pub drag_target: DragTarget,
    pub should_quit: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            title: Title::new(),
            module_manager: ModuleManager::new(),
            logger: Logger::new(),
            quadrant_context: Context::new(),
            split_y: 50,
            top_split_x: 60,
            bottom_split_x: 30,
            active_pane: ActivePane::TopLeft,
            drag_target: DragTarget::None,
            should_quit: false,
        }
    }

    pub fn resize_active(&mut self, direction: ResizeDirection) {
        const STEP: u16 = 2;
        match (&self.active_pane, direction) {
            (ActivePane::TopLeft, ResizeDirection::Right) => {
                self.top_split_x = (self.top_split_x + STEP).min(90)
            }
            (ActivePane::TopLeft, ResizeDirection::Left) => {
                self.top_split_x = self.top_split_x.saturating_sub(STEP).max(10)
            }
            (ActivePane::TopLeft, ResizeDirection::Down) => {
                self.split_y = (self.split_y + STEP).min(90)
            }
            (ActivePane::TopLeft, ResizeDirection::Up) => {
                self.split_y = self.split_y.saturating_sub(STEP).max(10)
            }
            _ => {
                // Handle rest of pane resizes as needed
            }
        }
    }

    pub fn handle_mouse(&mut self, mouse: MouseEvent, screen_size: Size) {
        let x_pct = ((mouse.column as f32 / screen_size.width as f32) * 100.0) as u16;
        let y_pct = ((mouse.row as f32 / screen_size.height as f32) * 100.0) as u16;

        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                let dy = (y_pct as i16 - self.split_y as i16).abs();
                if dy <= 3 {
                    self.drag_target = DragTarget::HorizontalSplit;
                } else {
                    self.drag_target = DragTarget::None;
                    if y_pct < self.split_y {
                        self.active_pane = if x_pct < self.top_split_x {
                            ActivePane::TopLeft
                        } else {
                            ActivePane::TopRight
                        };
                    } else {
                        self.active_pane = if x_pct < self.bottom_split_x {
                            ActivePane::BottomLeft
                        } else {
                            ActivePane::BottomRight
                        };
                    }
                }
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                if let DragTarget::HorizontalSplit = self.drag_target {
                    self.split_y = y_pct.clamp(10, 90);
                }
            }
            MouseEventKind::Up(MouseButton::Left) => self.drag_target = DragTarget::None,
            _ => {}
        }
    }

    pub fn next_pane(&mut self) {
        self.active_pane = match self.active_pane {
            ActivePane::TopLeft => ActivePane::TopRight,
            ActivePane::TopRight => ActivePane::BottomRight,
            ActivePane::BottomRight => ActivePane::BottomLeft,
            ActivePane::BottomLeft => ActivePane::TopLeft,
        };
    }

    pub fn route_key(&mut self, key: KeyEvent) {
        let is_shift = key.modifiers.contains(KeyModifiers::SHIFT);
        if is_shift || matches!(key.code, KeyCode::Tab | KeyCode::Esc) {
            match (key.code, is_shift) {
                (KeyCode::Char('q'), false) | (KeyCode::Esc, _) => self.should_quit = true,
                (KeyCode::Tab, _) => self.next_pane(),
                _ => {}
            }
        }
    }
}

pub enum ResizeDirection {
    Up,
    Down,
    Left,
    Right,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();

    // Use CockatielClient from your library file to establish connection
    let client = CockatielClient::connect("tui-manager")
        .priority(1)
        .position("input")
        .connect()
        .await?;

    let client_arc = std::sync::Arc::new(client);
    let (tx, mut rx) = mpsc::channel::<Container>(100);

    // Spawn background receiver task utilizing the library's `receive` method
    let c_clone = client_arc.clone();
    tokio::spawn(async move {
        let _ = c_clone
            .receive(move |container| {
                let _ = tx.blocking_send(container);
            })
            .await;
    });

    while !app.should_quit {
        terminal.draw(|f| render_app(f, &app))?;

        tokio::select! {
            Some(container) = rx.recv() => {
                let formatted_msg = format_container(&container);
                app.logger.display(formatted_msg);
            }
            result = tokio::task::spawn_blocking(move || {
                if event::poll(Duration::from_millis(16)).unwrap_or(false) {
                    Some(event::read())
                } else {
                    None
                }
            }) => {
                if let Ok(Some(Ok(evt))) = result {
                    match evt {
                        Event::Key(key) => app.route_key(key),
                        Event::Mouse(mouse) => {
                            let size = terminal.size()?;
                            app.handle_mouse(mouse, size);
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    Ok(())
}

fn format_container(container: &Container) -> String {
    let module = &container.module_name;
    let msg_type = &container.r#type;

    let details = match &container.payload {
        Some(Payload::ConnectionRequest(req)) => {
            format!("Pin: {}, Pos: {}", req.pin, req.process_position)
        }
        Some(Payload::Log(l)) => l.log.clone(),
        Some(Payload::Err(e)) => format!("Error: {} | Trace: {}", e.log, e.trace),
        Some(Payload::TimelineEvent(te)) => format!("Timeline ID: {} | Cmd: {}", te.i, te.c),
        Some(Payload::MessagePreProcess(m)) => {
            format!("Platform: {} | User: {}", m.platform, m.user_uuid7)
        }
        Some(Payload::MessageInProcess(m)) => format!(
            "Platform: {} | Processed: {}",
            m.platform, m.processed_message
        ),
        Some(Payload::MessagePostProcess(m)) => {
            format!("Platform: {} | Final: {}", m.platform, m.processed_message)
        }
        Some(Payload::CommandPayload(cmd)) => format!("Command: {}", cmd.command_name),
        Some(Payload::Shutdown(s)) => format!("Reason: {}", s.reason),
        _ => format!("Type: {}", msg_type),
    };

    format!("[{}] [{}] {}", module, msg_type, details)
}

fn render_app(frame: &mut Frame, app: &App) {
    let size = frame.area();
    let vertical_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(app.split_y),
            Constraint::Percentage(100 - app.split_y),
        ])
        .split(size);

    let top_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(app.top_split_x),
            Constraint::Percentage(100 - app.top_split_x),
        ])
        .split(vertical_chunks[0]);

    let bottom_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(app.bottom_split_x),
            Constraint::Percentage(100 - app.bottom_split_x),
        ])
        .split(vertical_chunks[1]);

    app.title.render(
        frame,
        top_chunks[0],
        matches!(app.active_pane, ActivePane::TopLeft),
    );
    app.module_manager.render(
        frame,
        top_chunks[1],
        matches!(app.active_pane, ActivePane::TopRight),
    );
    app.logger.render(
        frame,
        bottom_chunks[0],
        matches!(app.active_pane, ActivePane::BottomLeft),
    );
    app.quadrant_context.render(
        frame,
        bottom_chunks[1],
        matches!(app.active_pane, ActivePane::BottomRight),
    );
}
