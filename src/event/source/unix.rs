use std::io;
use std::time::Instant;

use self::parser::Parser;
use crate::event::InternalEvent;
use crate::terminal::sys::file_descriptor::FileDesc;

#[cfg(feature = "use-dev-tty")]
pub(crate) mod tty;

#[cfg(not(feature = "use-dev-tty"))]
pub(crate) mod mio;
pub(crate) mod parser;

#[cfg(feature = "use-dev-tty")]
pub(crate) use self::tty::UnixInternalEventSource;

#[cfg(not(feature = "use-dev-tty"))]
pub(crate) use self::mio::UnixInternalEventSource;

fn next_input_event(
    parser: &mut Parser,
    tty: &FileDesc<'_>,
    buffer: &mut [u8],
) -> io::Result<(Option<InternalEvent>, bool)> {
    let now = Instant::now();
    let mut read_input = false;
    if parser.probe_drain_expired(now) {
        // Startup may delay polling even though the reply already reached the
        // tty. Check queued bytes once before releasing the ambiguous prefix.
        if let Some(count) = tty.read_if_ready(buffer)? {
            read_input = count > 0;
            parser.advance(&buffer[..count], count == buffer.len());
        }
    }
    Ok((parser.next_at(now), read_input))
}

#[cfg(test)]
mod tests {
    use super::UnixInternalEventSource;
    use crate::event::{
        source::{EventSource, GraphicsProbe},
        Event, InternalEvent, KeyCode,
    };
    use crate::terminal::sys::file_descriptor::FileDesc;
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    #[test]
    fn lone_escape_on_blocking_input_does_not_stall_the_probe() {
        let (mut source, mut writer) = input_pair();
        writer.write_all(b"\x1b").unwrap();

        let (result_tx, result_rx) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let wait = Duration::from_millis(50);
            source.set_graphics_probe(GraphicsProbe::Started(Instant::now() + wait));
            let probe = source.try_read(Some(wait));
            source.set_graphics_probe(GraphicsProbe::Finished {
                expect_late_replies: true,
            });
            let key = source.try_read(Some(Duration::from_secs(1)));
            result_tx.send((probe, key)).unwrap();
        });
        // Keep the writer open and silent while the probe reaches its deadline.
        let result = result_rx.recv_timeout(Duration::from_secs(2));
        drop(writer);
        reader.join().unwrap();
        let (probe, key) = result.expect("probe blocked while waiting for another byte");
        assert_eq!(probe.unwrap(), None);
        assert_eq!(
            key.unwrap(),
            Some(InternalEvent::Event(Event::Key(KeyCode::Esc.into())))
        );
    }

    #[test]
    fn a_reply_already_queued_at_expiry_is_parsed_before_releasing_escape() {
        let (mut source, mut writer) = input_pair();
        let wait = Duration::from_millis(25);
        source.set_graphics_probe(GraphicsProbe::Started(Instant::now() + wait));
        writer.write_all(b"\x1b").unwrap();
        assert_eq!(source.try_read(Some(wait)).unwrap(), None);
        source.set_graphics_probe(GraphicsProbe::Finished {
            expect_late_replies: true,
        });
        writer.write_all(b"P>|ghostty 1.4.0\x1b\\a").unwrap();
        std::thread::sleep(Duration::from_millis(450));

        assert_eq!(
            source.try_read(Some(Duration::ZERO)).unwrap(),
            Some(InternalEvent::TerminalVersion("ghostty 1.4.0".into()))
        );
        assert_eq!(
            source.try_read(Some(Duration::ZERO)).unwrap(),
            Some(InternalEvent::Event(Event::Key(KeyCode::Char('a').into())))
        );
        assert_eq!(source.try_read(Some(Duration::ZERO)).unwrap(), None);
    }

    fn input_pair() -> (UnixInternalEventSource, UnixStream) {
        let (input, writer) = UnixStream::pair().unwrap();
        #[cfg(feature = "libc")]
        let input = {
            use std::os::fd::IntoRawFd;
            FileDesc::new(input.into_raw_fd(), true)
        };
        #[cfg(not(feature = "libc"))]
        let input = FileDesc::Owned(input.into());
        (
            UnixInternalEventSource::from_file_descriptor(input).unwrap(),
            writer,
        )
    }
}
