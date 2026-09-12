use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};

pub struct ModuleManagerQuadrant;

impl ModuleManagerQuadrant {
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
            .title(" Module Manager [3/12] ")
            .borders(Borders::ALL)
            .border_style(border_style);

        let content = Paragraph::new(
            "mod_4 - RUNNING\n\
             mod_5 - OFFLINE\n\
             mod_6 - CRASHED\n\
             mod_7 - PAUSED\n\
             mod_8 - ...\n\
             ...          9/12",
        )
        .block(block);

        frame.render_widget(content, area);
    }
}
