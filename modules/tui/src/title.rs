use crossterm::event::KeyEvent;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Paragraph},
};

pub struct Title;

impl Title {
    pub fn new() -> Self {
        Self
    }

    pub fn handle_input(&mut self, _key: KeyEvent) {
        // Handle internal input for Title window when captured
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
            "
        XXXXXXXXX      XX   | Cockatiel
      XXXXXXXXXXXXXXXXXXX   | by: @vulbyte
     XX    XXXXXXXXXXXXX    | 
  XXXX      XXXXXXXXXXXXXX  | Version:
 XXXXXX    XXXXXXXXX XX     | 0.0.1
   XXXXXXXXXXXXXXXXX        |
     XXXXXXXXXXXXXXX        |
     XXX XXXXXXX XXX        |
     XX   XXXX    XX        |
        ",
        )
        .block(block);

        frame.render_widget(content, area);
    }
}
