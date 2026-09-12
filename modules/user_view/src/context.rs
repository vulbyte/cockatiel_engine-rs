use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};

pub struct QuadrantContextQuadrant;

impl QuadrantContextQuadrant {
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
            .title(" Quadrant Context ")
            .borders(Borders::ALL)
            .border_style(border_style);

        let content = Paragraph::new(
            "Controls & Navigation:\n\
             - Tab: Change Focus\n\
             - Shift + HJKL: Resize Pane\n\
             - Click & Drag: Resize Split\n\
             - q / Esc: Quit",
        )
        .block(block);

        frame.render_widget(content, area);
    }
}
