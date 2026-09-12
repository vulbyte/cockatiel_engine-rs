use crossterm::event::KeyEvent;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};

pub struct Logger {
    logs: Vec<String>,
}

impl Logger {
    pub fn new() -> Self {
        Self {
            logs: vec![
                "[INFO] System booting up...".to_string(),
                "[READY] Connected to Cockatiel Client lib stream...".to_string(),
            ],
        }
    }

    /// Displays and buffers an incoming log string
    pub fn display(&mut self, message: String) {
        self.logs.push(message);
        if self.logs.len() > 100 {
            self.logs.remove(0);
        }
    }

    pub fn handle_input(&mut self, _key: KeyEvent) {}

    pub fn render(&self, frame: &mut Frame, area: Rect, is_focused: bool) {
        let border_style = if is_focused {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let block = Block::default()
            .title(format!(" Logger Window [{}] ", self.logs.len()))
            .borders(Borders::ALL)
            .border_style(border_style);

        let content = Paragraph::new(self.logs.join("\n")).block(block);
        frame.render_widget(content, area);
    }
}
