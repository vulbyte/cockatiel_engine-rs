mod logger;
mod module_manager;
mod quadrant_context;
mod title;

use logger::LoggerQuadrant;
use module_manager::ModuleManagerQuadrant;
use quadrant_context::QuadrantContextQuadrant;
use title::TitleQuadrant;

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
    layout::{Constraint, Direction, Layout, Rect},
};
use std::{io, time::Duration};

pub enum ActivePane {
    TopLeft,
    TopRight,
    BottomLeft,
    BottomRight,
}

pub struct App {
    pub title: TitleQuadrant,
    pub module_manager: ModuleManagerQuadrant,
    pub logger: LoggerQuadrant,
    pub quadrant_context: QuadrantContextQuadrant,

    pub split_x: u16,
    pub split_y: u16,
    pub active_pane: ActivePane,
    pub is_dragging: bool,
    pub should_quit: bool,
}

impl App {
    pub fn new() -> Self {
        Self {
            title: TitleQuadrant::new(),
            module_manager: ModuleManagerQuadrant::new(),
            logger: LoggerQuadrant::new(),
            quadrant_context: QuadrantContextQuadrant::new(),
            split_x: 60,
            split_y: 50,
            active_pane: ActivePane::TopLeft,
            is_dragging: false,
            should_quit: false,
        }
    }

    pub fn resize_active(&mut self, direction: ResizeDirection) {
        const STEP: u16 = 2;

        match (&self.active_pane, direction) {
            (ActivePane::TopLeft, ResizeDirection::Right) => {
                self.split_x = (self.split_x + STEP).min(90)
            }
            (ActivePane::TopLeft, ResizeDirection::Left) => {
                self.split_x = self.split_x.saturating_sub(STEP).max(10)
            }
            (ActivePane::TopLeft, ResizeDirection::Down) => {
                self.split_y = (self.split_y + STEP).min(90)
            }
            (ActivePane::TopLeft, ResizeDirection::Up) => {
                self.split_y = self.split_y.saturating_sub(STEP).max(10)
            }

            (ActivePane::TopRight, ResizeDirection::Left) => {
                self.split_x = self.split_x.saturating_sub(STEP).max(10)
            }
            (ActivePane::TopRight, ResizeDirection::Right) => {
                self.split_x = (self.split_x + STEP).min(90)
            }
            (ActivePane::TopRight, ResizeDirection::Down) => {
                self.split_y = (self.split_y + STEP).min(90)
            }
            (ActivePane::TopRight, ResizeDirection::Up) => {
                self.split_y = self.split_y.saturating_sub(STEP).max(10)
            }

            (ActivePane::BottomLeft, ResizeDirection::Right) => {
                self.split_x = (self.split_x + STEP).min(90)
            }
            (ActivePane::BottomLeft, ResizeDirection::Left) => {
                self.split_x = self.split_x.saturating_sub(STEP).max(10)
            }
            (ActivePane::BottomLeft, ResizeDirection::Up) => {
                self.split_y = self.split_y.saturating_sub(STEP).max(10)
            }
            (ActivePane::BottomLeft, ResizeDirection::Down) => {
                self.split_y = (self.split_y + STEP).min(90)
            }

            (ActivePane::BottomRight, ResizeDirection::Left) => {
                self.split_x = self.split_x.saturating_sub(STEP).max(10)
            }
            (ActivePane::BottomRight, ResizeDirection::Right) => {
                self.split_x = (self.split_x + STEP).min(90)
            }
            (ActivePane::BottomRight, ResizeDirection::Up) => {
                self.split_y = self.split_y.saturating_sub(STEP).max(10)
            }
            (ActivePane::BottomRight, ResizeDirection::Down) => {
                self.split_y = (self.split_y + STEP).min(90)
            }
        }
    }

    pub fn handle_mouse(&mut self, mouse: MouseEvent, screen_size: Rect) {
        let x_pct = ((mouse.column as f32 / screen_size.width as f32) * 100.0) as u16;
        let y_pct = ((mouse.row as f32 / screen_size.height as f32) * 100.0) as u16;

        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                self.is_dragging = true;
                if x_pct < self.split_x && y_pct < self.split_y {
                    self.active_pane = ActivePane::TopLeft;
                } else if x_pct >= self.split_x && y_pct < self.split_y {
                    self.active_pane = ActivePane::TopRight;
                } else if x_pct < self.split_x && y_pct >= self.split_y {
                    self.active_pane = ActivePane::BottomLeft;
                } else {
                    self.active_pane = ActivePane::BottomRight;
                }
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                if self.is_dragging {
                    self.split_x = x_pct.clamp(10, 90);
                    self.split_y = y_pct.clamp(10, 90);
                }
            }
            MouseEventKind::Up(MouseButton::Left) => {
                self.is_dragging = false;
            }
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
}

pub enum ResizeDirection {
    Up,
    Down,
    Left,
    Right,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new();

    while !app.should_quit {
        terminal.draw(|f| render_app(f, &app))?;

        if event::poll(Duration::from_millis(16))? {
            match event::read()? {
                Event::Key(key) => handle_key_events(key, &mut app),
                Event::Mouse(mouse) => {
                    let size = terminal.size()?;
                    app.handle_mouse(mouse, size);
                }
                _ => {}
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

fn handle_key_events(key: KeyEvent, app: &mut App) {
    let is_shift = key.modifiers.contains(KeyModifiers::SHIFT);

    match (key.code, is_shift) {
        (KeyCode::Char('q'), false) | (KeyCode::Esc, _) => app.should_quit = true,
        (KeyCode::Char('c'), true) => app.should_quit = true,
        (KeyCode::Tab, _) => app.next_pane(),

        (KeyCode::Left, true) => app.resize_active(ResizeDirection::Left),
        (KeyCode::Right, true) => app.resize_active(ResizeDirection::Right),
        (KeyCode::Up, true) => app.resize_active(ResizeDirection::Up),
        (KeyCode::Down, true) => app.resize_active(ResizeDirection::Down),

        (KeyCode::Char('H'), _) | (KeyCode::Char('h'), true) => {
            app.resize_active(ResizeDirection::Left)
        }
        (KeyCode::Char('L'), _) | (KeyCode::Char('l'), true) => {
            app.resize_active(ResizeDirection::Right)
        }
        (KeyCode::Char('K'), _) | (KeyCode::Char('k'), true) => {
            app.resize_active(ResizeDirection::Up)
        }
        (KeyCode::Char('J'), _) | (KeyCode::Char('j'), true) => {
            app.resize_active(ResizeDirection::Down)
        }

        _ => {}
    }
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
            Constraint::Percentage(app.split_x),
            Constraint::Percentage(100 - app.split_x),
        ])
        .split(vertical_chunks[0]);

    let bottom_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(app.split_x),
            Constraint::Percentage(100 - app.split_x),
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
