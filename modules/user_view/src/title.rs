use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};

pub struct TitleQuadrant;

impl TitleQuadrant {
    pub fn new() -> Self {
        Self
    }

    pub fn render(&self, frame: &mut Frame, area: Rect, is_focused: bool) {
        let border_style = if is_focused {
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };

        let block = Block::default()
            .title(" Title ")
            .borders(Borders::ALL)
            .border_style(border_style);

        let content = Paragraph::new(
            "     XXXX    cockatiel\n\
             \t XXXX   XXX by vulbyte\n\
             XXXXXXXXXXXXX\n\
             version 0.1.0",
        )
        .block(block);

        frame.render_widget(content, area);
    }
}
