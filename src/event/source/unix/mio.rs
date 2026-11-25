use super::next_input_event;
use super::parser::Parser;
use std::{io, time::Duration};

use mio::{unix::SourceFd, Events, Interest, Poll, Token};
use signal_hook_mio::v1_0::Signals;

#[cfg(feature = "event-stream")]
use crate::event::sys::Waker;
use crate::event::{
    source::{EventSource, GraphicsProbe},
    timeout::PollTimeout,
    Event, InternalEvent,
};
use crate::terminal::sys::file_descriptor::{tty_fd, FileDesc};

// Tokens to identify file descriptor
const TTY_TOKEN: Token = Token(0);
const SIGNAL_TOKEN: Token = Token(1);
#[cfg(feature = "event-stream")]
const WAKE_TOKEN: Token = Token(2);

// I (@zrzka) wasn't able to read more than 1_022 bytes when testing
// reading on macOS/Linux -> we don't need bigger buffer and 1k of bytes
// is enough.
const TTY_BUFFER_SIZE: usize = 1_024;

pub(crate) struct UnixInternalEventSource {
    poll: Poll,
    events: Events,
    parser: Parser,
    tty_buffer: [u8; TTY_BUFFER_SIZE],
    tty_fd: FileDesc<'static>,
    input_ready: bool,
    signals: Signals,
    #[cfg(feature = "event-stream")]
    waker: Waker,
}

impl UnixInternalEventSource {
    pub fn new() -> io::Result<Self> {
        UnixInternalEventSource::from_file_descriptor(tty_fd()?)
    }

    pub(crate) fn from_file_descriptor(input_fd: FileDesc<'static>) -> io::Result<Self> {
        let poll = Poll::new()?;
        let registry = poll.registry();

        let tty_raw_fd = input_fd.raw_fd();
        let mut tty_ev = SourceFd(&tty_raw_fd);
        registry.register(&mut tty_ev, TTY_TOKEN, Interest::READABLE)?;

        let mut signals = Signals::new([signal_hook::consts::SIGWINCH])?;
        registry.register(&mut signals, SIGNAL_TOKEN, Interest::READABLE)?;

        #[cfg(feature = "event-stream")]
        let waker = Waker::new(registry, WAKE_TOKEN)?;

        Ok(UnixInternalEventSource {
            poll,
            events: Events::with_capacity(3),
            parser: Parser::default(),
            tty_buffer: [0u8; TTY_BUFFER_SIZE],
            tty_fd: input_fd,
            input_ready: false,
            signals,
            #[cfg(feature = "event-stream")]
            waker,
        })
    }

    fn next_event(&mut self) -> io::Result<Option<InternalEvent>> {
        let (event, read_input) =
            next_input_event(&mut self.parser, &self.tty_fd, &mut self.tty_buffer)?;
        self.input_ready |= read_input;
        Ok(event)
    }
}

impl EventSource for UnixInternalEventSource {
    fn try_read(&mut self, timeout: Option<Duration>) -> io::Result<Option<InternalEvent>> {
        let timeout = PollTimeout::new(timeout);

        loop {
            if let Some(event) = self.next_event()? {
                return Ok(Some(event));
            }
            if self.input_ready {
                match self.tty_fd.read_if_ready(&mut self.tty_buffer) {
                    Ok(Some(0)) => return Err(io::ErrorKind::UnexpectedEof.into()),
                    Ok(Some(count)) => {
                        self.parser
                            .advance(&self.tty_buffer[..count], count == TTY_BUFFER_SIZE);
                        if let Some(event) = self.next_event()? {
                            return Ok(Some(event));
                        }
                        if timeout.elapsed() {
                            return Ok(None);
                        }
                        continue;
                    }
                    Ok(None) => self.input_ready = false,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                        if timeout.elapsed() {
                            return Ok(None);
                        }
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            if let Err(e) = self.poll.poll(
                &mut self.events,
                self.parser.poll_timeout(timeout.leftover()),
            ) {
                // Mio will throw an interrupted error in case of cursor position retrieval. We need to retry until it succeeds.
                // Previous versions of Mio (< 0.7) would automatically retry the poll call if it was interrupted (if EINTR was returned).
                // https://docs.rs/mio/0.7.0/mio/struct.Poll.html#notes
                if e.kind() == io::ErrorKind::Interrupted && !timeout.elapsed() {
                    continue;
                } else if e.kind() == io::ErrorKind::Interrupted {
                    return Ok(None);
                } else {
                    return Err(e);
                }
            };

            // mio reports edges. Carry unread readiness over if an event is
            // returned before the descriptor has been drained.
            self.input_ready = self.events.iter().any(|event| event.token() == TTY_TOKEN);
            for token in self.events.iter().map(|x| x.token()) {
                match token {
                    TTY_TOKEN => {}
                    SIGNAL_TOKEN => {
                        if self.signals.pending().next() == Some(signal_hook::consts::SIGWINCH) {
                            // TODO Should we remove tput?
                            //
                            // This can take a really long time, because terminal::size can
                            // launch new process (tput) and then it parses its output. It's
                            // not a really long time from the absolute time point of view, but
                            // it's a really long time from the mio, async-std/tokio executor, ...
                            // point of view.
                            let new_size = crate::terminal::size()?;
                            return Ok(Some(InternalEvent::Event(Event::Resize(
                                new_size.0, new_size.1,
                            ))));
                        }
                    }
                    #[cfg(feature = "event-stream")]
                    WAKE_TOKEN => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "Poll operation was woken up by `Waker::wake`",
                        ));
                    }
                    _ => unreachable!("Synchronize Evented handle registration & token handling"),
                }
            }

            // Processing above can take some time, check if timeout expired
            if timeout.elapsed() && !self.input_ready {
                return self.next_event();
            }
        }
    }

    fn set_graphics_probe(&mut self, probe: GraphicsProbe) {
        self.parser.set_graphics_probe(probe);
    }

    #[cfg(feature = "event-stream")]
    fn waker(&self) -> Waker {
        self.waker.clone()
    }
}
