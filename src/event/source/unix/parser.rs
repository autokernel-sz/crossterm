use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::event::{source::GraphicsProbe, sys::unix::parse::parse_event, InternalEvent};

const VERSION_PREFIX: &[u8] = b"\x1bP>|";
const GRAPHICS_PREFIX: &[u8] = b"\x1b_Gi=";
const REPLY_END: &[u8] = b"\x1b\\";
const AMBIGUOUS_PREFIX_GAP: Duration = Duration::from_millis(35);
const REPLY_BODY_GAP: Duration = Duration::from_millis(200);
const PROBE_DRAIN_TIMEOUT: Duration = Duration::from_millis(300);
const MAX_REPLY_LEN: usize = 512;

enum GraphicsReplies {
    Disabled,
    Probing(Instant),
    Draining(Instant),
    Enabled,
}

pub(crate) struct Parser {
    buffer: Vec<u8>,
    events: VecDeque<InternalEvent>,
    reply_deadline: Option<Instant>,
    escape_ended_read: bool,
    graphics_replies: GraphicsReplies,
}

impl Default for Parser {
    fn default() -> Self {
        Self {
            buffer: Vec::with_capacity(256),
            events: VecDeque::with_capacity(128),
            reply_deadline: None,
            escape_ended_read: false,
            graphics_replies: GraphicsReplies::Disabled,
        }
    }
}

impl Parser {
    pub(crate) fn set_graphics_probe(&mut self, probe: GraphicsProbe) {
        self.graphics_replies = match probe {
            GraphicsProbe::Started(deadline) => GraphicsReplies::Probing(deadline),
            // Either reply may still be on its way after the caller draws the fallback.
            GraphicsProbe::Finished {
                expect_late_replies: true,
            } => GraphicsReplies::Draining(Instant::now() + PROBE_DRAIN_TIMEOUT),
            GraphicsProbe::Finished {
                expect_late_replies: false,
            } => GraphicsReplies::Enabled,
        };
        if self.reply_deadline.is_some() {
            let gap = if reply_prefix(&self.buffer).is_some() {
                REPLY_BODY_GAP
            } else {
                AMBIGUOUS_PREFIX_GAP
            };
            self.wait_for_reply(gap);
        }
    }

    pub(crate) fn poll_timeout(&self, timeout: Option<Duration>) -> Option<Duration> {
        match self.reply_deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                Some(timeout.map_or(remaining, |timeout| timeout.min(remaining)))
            }
            None => timeout,
        }
    }

    pub(crate) fn advance(&mut self, bytes: &[u8], more: bool) {
        // A read can wake after the drain deadline even with no partial reply.
        // Leave an existing prefix intact until the caller checks queued bytes.
        if self.reply_deadline.is_none()
            && matches!(self.graphics_replies, GraphicsReplies::Draining(_))
        {
            self.expire_probe_drain(Instant::now());
        }
        for (index, &byte) in bytes.iter().enumerate() {
            let more = index + 1 < bytes.len() || more;
            if matches!(self.graphics_replies, GraphicsReplies::Disabled) {
                self.advance_input(byte, more);
            } else {
                self.advance_with_reply(byte, more);
            }
        }
    }

    fn advance_input(&mut self, byte: u8, more: bool) {
        self.buffer.push(byte);
        self.parse_input(more);
    }

    fn parse_input(&mut self, more: bool) {
        match parse_event(&self.buffer, more) {
            Ok(Some(event)) => {
                self.events.push_back(event);
                self.buffer.clear();
            }
            Ok(None) => {}
            Err(_) => self.buffer.clear(),
        }
    }

    fn advance_with_reply(&mut self, byte: u8, more: bool) {
        if byte == b'\x1b' && self.reply_deadline.is_some() && reply_prefix(&self.buffer).is_none()
        {
            // A user Escape immediately before a reply belongs to the user.
            self.reply_deadline = None;
            self.replay_input(false);
        }
        // An Escape inside a reply is valid only as part of its terminator.
        // On interruption, parse the new sequence starting at that Escape.
        if reply_prefix(&self.buffer).is_some() && self.buffer.ends_with(b"\x1b") && byte != b'\\' {
            self.buffer.truncate(1);
        }
        if byte == b'\x1b' && (self.buffer.is_empty() || reply_prefix(&self.buffer).is_some()) {
            self.escape_ended_read = !more;
        }
        self.buffer.push(byte);
        if let Some(prefix) = reply_prefix(&self.buffer) {
            if let Some(body) = self.buffer.strip_suffix(REPLY_END) {
                if let Some(event) = parse_reply(prefix, &body[prefix.len()..]) {
                    self.events.push_back(event);
                }
                self.clear_reply();
            } else if self.buffer.len() >= MAX_REPLY_LEN {
                self.clear_reply();
            } else {
                self.wait_for_reply(REPLY_BODY_GAP);
            }
        } else if [VERSION_PREFIX, GRAPHICS_PREFIX]
            .iter()
            .any(|prefix| prefix.starts_with(&self.buffer))
        {
            self.wait_for_reply(AMBIGUOUS_PREFIX_GAP);
        } else if self.reply_deadline.is_some() {
            self.reply_deadline = None;
            // CSI, SS3 and OSC input can span reads.
            if self.buffer.len() == 2 && !self.escape_ended_read
                || matches!(self.buffer.as_slice(), [b'\x1b', b'[' | b'O' | b']'])
            {
                self.escape_ended_read = false;
                self.parse_input(more);
            } else {
                self.replay_input(more);
            }
        } else {
            self.parse_input(more);
        }
    }

    fn wait_for_reply(&mut self, gap: Duration) {
        let deadline = match self.graphics_replies {
            GraphicsReplies::Probing(deadline) | GraphicsReplies::Draining(deadline) => deadline,
            _ => Instant::now() + gap,
        };
        self.reply_deadline = Some(deadline);
    }

    fn replay_input(&mut self, more: bool) {
        // Keep the original read boundary between a standalone Escape and
        // subsequent keys. A same-read Alt chord remains a single key.
        let escape_ended_read = std::mem::take(&mut self.escape_ended_read);
        let bytes = std::mem::take(&mut self.buffer);
        let input = if escape_ended_read {
            self.advance_input(bytes[0], false);
            &bytes[1..]
        } else {
            &bytes[..]
        };
        for (index, byte) in input.iter().copied().enumerate() {
            self.advance_input(byte, index + 1 < input.len() || more);
        }
    }

    fn clear_reply(&mut self) {
        self.reply_deadline = None;
        self.escape_ended_read = false;
        self.buffer.clear();
    }

    pub(crate) fn probe_drain_expired(&self, now: Instant) -> bool {
        self.reply_deadline.is_some()
            && matches!(self.graphics_replies, GraphicsReplies::Draining(deadline) if now >= deadline)
    }

    fn expire_probe_drain(&mut self, now: Instant) {
        if matches!(self.graphics_replies, GraphicsReplies::Draining(deadline) if now >= deadline) {
            self.graphics_replies = GraphicsReplies::Enabled;
        }
    }

    pub(crate) fn next_at(&mut self, now: Instant) -> Option<InternalEvent> {
        self.expire_probe_drain(now);
        if !matches!(self.graphics_replies, GraphicsReplies::Probing(_))
            && matches!(self.reply_deadline, Some(deadline) if now >= deadline)
        {
            if reply_prefix(&self.buffer).is_some() {
                self.clear_reply();
            } else {
                self.reply_deadline = None;
                self.replay_input(false);
            }
        }
        self.events.pop_front()
    }

    #[cfg(test)]
    fn next(&mut self) -> Option<InternalEvent> {
        self.next_at(Instant::now())
    }
}

fn reply_prefix(bytes: &[u8]) -> Option<&'static [u8]> {
    [VERSION_PREFIX, GRAPHICS_PREFIX]
        .into_iter()
        .find(|prefix| bytes.starts_with(prefix))
}

fn parse_reply(prefix: &[u8], body: &[u8]) -> Option<InternalEvent> {
    if !body.iter().all(|byte| (b' '..=b'~').contains(byte)) {
        return None;
    }
    let body = std::str::from_utf8(body).ok()?;
    if prefix == VERSION_PREFIX {
        return Some(InternalEvent::TerminalVersion(body.to_owned()));
    }

    let (parameters, status) = body.split_once(';')?;
    let (id, _) = parameters.split_once(',').unwrap_or((parameters, ""));
    Some(InternalEvent::KittyGraphicsReply {
        image_id: id.parse().ok()?,
        ok: status == "OK",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{Event, KeyCode, KeyEvent, KeyModifiers};

    #[test]
    fn fragmented_replies_and_typing_are_distinct() {
        let mut parser = Parser::default();
        parser.set_graphics_probe(GraphicsProbe::Started(
            Instant::now() + Duration::from_millis(500),
        ));
        for bytes in [
            b"a\x1b".as_slice(),
            b"P",
            b">",
            b"|ghostty 1.3.1\x1b",
            b"\\b\x1b_",
            b"Gi",
            b"=31;OK\x1b",
            b"\\c",
        ] {
            parser.advance(bytes, false);
        }
        assert_eq!(parser.next(), Some(key('a', KeyModifiers::NONE)));
        assert_eq!(
            parser.next(),
            Some(InternalEvent::TerminalVersion("ghostty 1.3.1".into()))
        );
        assert_eq!(parser.next(), Some(key('b', KeyModifiers::NONE)));
        assert_eq!(
            parser.next(),
            Some(InternalEvent::KittyGraphicsReply {
                image_id: 31,
                ok: true,
            })
        );
        assert_eq!(parser.next(), Some(key('c', KeyModifiers::NONE)));
        assert_eq!(parser.next(), None);
    }

    #[test]
    fn ambiguous_alt_input_is_preserved() {
        let mut parser = Parser::default();
        parser.graphics_replies = GraphicsReplies::Enabled;
        parser.advance(b"\x1bP>x\x1b_", false);
        assert_eq!(
            parser.next(),
            Some(key('P', KeyModifiers::ALT | KeyModifiers::SHIFT))
        );
        assert_eq!(parser.next(), Some(key('>', KeyModifiers::NONE)));
        assert_eq!(parser.next(), Some(key('x', KeyModifiers::NONE)));
        assert_eq!(parser.next(), None);
        parser.reply_deadline = Some(Instant::now());
        assert_eq!(parser.next(), Some(key('_', KeyModifiers::ALT)));
    }

    #[test]
    fn an_unfinished_prefix_is_released_once_when_draining_expires() {
        let mut parser = Parser::default();
        parser.set_graphics_probe(GraphicsProbe::Started(Instant::now()));
        parser.advance(b"\x1b", false);
        assert_eq!(parser.next(), None);
        parser.set_graphics_probe(GraphicsProbe::Finished {
            expect_late_replies: true,
        });
        assert_eq!(parser.next(), None);
        let deadline = parser.reply_deadline.unwrap();
        assert_eq!(
            parser.next_at(deadline),
            Some(InternalEvent::Event(Event::Key(KeyCode::Esc.into())))
        );
        assert_eq!(parser.next_at(deadline), None);
    }

    #[test]
    fn an_escape_read_after_an_idle_drain_has_the_normal_prefix_gap() {
        let mut parser = Parser::default();
        let deadline = Instant::now();
        parser.graphics_replies = GraphicsReplies::Draining(deadline);
        parser.advance(b"\x1b", false);
        assert_eq!(parser.next_at(deadline), None);
        assert!(matches!(parser.graphics_replies, GraphicsReplies::Enabled));
        assert_eq!(
            parser.next_at(parser.reply_deadline.unwrap()),
            Some(InternalEvent::Event(Event::Key(KeyCode::Esc.into())))
        );
    }

    #[test]
    fn a_reply_that_completes_after_the_probe_keeps_its_framing() {
        let mut parser = Parser::default();
        parser.set_graphics_probe(GraphicsProbe::Started(Instant::now()));
        parser.advance(b"\x1b", false);
        parser.set_graphics_probe(GraphicsProbe::Finished {
            expect_late_replies: true,
        });
        parser.advance(b"P>|ghostty 1.4.0\x1b\\a", false);
        assert_eq!(
            parser.next(),
            Some(InternalEvent::TerminalVersion("ghostty 1.4.0".into()))
        );
        assert_eq!(parser.next(), Some(key('a', KeyModifiers::NONE)));
        assert_eq!(parser.next(), None);
    }

    #[test]
    fn ordinary_input_does_not_combine_with_escape_from_a_previous_read() {
        let mut parser = Parser::default();
        parser.set_graphics_probe(GraphicsProbe::Started(Instant::now()));
        parser.advance(b"\x1b", false);
        parser.set_graphics_probe(GraphicsProbe::Finished {
            expect_late_replies: true,
        });
        parser.advance(b"a", false);
        assert_eq!(
            parser.next(),
            Some(InternalEvent::Event(Event::Key(KeyCode::Esc.into())))
        );
        assert_eq!(parser.next(), Some(key('a', KeyModifiers::NONE)));
        assert_eq!(parser.next(), None);
    }

    #[test]
    fn a_split_arrow_key_is_preserved_while_draining() {
        let mut parser = Parser::default();
        parser.set_graphics_probe(GraphicsProbe::Started(Instant::now()));
        parser.advance(b"\x1b", false);
        parser.set_graphics_probe(GraphicsProbe::Finished {
            expect_late_replies: true,
        });
        parser.advance(b"[A", false);
        assert_eq!(
            parser.next(),
            Some(InternalEvent::Event(Event::Key(KeyCode::Up.into())))
        );
        assert_eq!(parser.next(), None);
    }

    #[test]
    fn a_user_escape_before_a_reply_preserves_both_events() {
        let mut parser = Parser::default();
        parser.set_graphics_probe(GraphicsProbe::Started(
            Instant::now() + Duration::from_millis(500),
        ));
        parser.advance(b"\x1b", false);
        parser.advance(b"\x1bP>|ghostty 1.4.0\x1b\\", false);
        assert_eq!(
            parser.next(),
            Some(InternalEvent::Event(Event::Key(KeyCode::Esc.into())))
        );
        assert_eq!(
            parser.next(),
            Some(InternalEvent::TerminalVersion("ghostty 1.4.0".into()))
        );
    }

    fn key(character: char, modifiers: KeyModifiers) -> InternalEvent {
        InternalEvent::Event(Event::Key(KeyEvent::new(
            KeyCode::Char(character),
            modifiers,
        )))
    }
}
