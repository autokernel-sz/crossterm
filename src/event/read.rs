use std::{collections::vec_deque::VecDeque, io, time::Duration};

#[cfg(unix)]
use std::time::Instant;

#[cfg(unix)]
use crate::event::filter::GraphicsProbeFilter;
#[cfg(windows)]
use crate::event::source::windows::WindowsEventSource;
#[cfg(unix)]
use crate::event::source::{unix::UnixInternalEventSource, GraphicsProbe, GRAPHICS_DRAIN_TIMEOUT};
#[cfg(feature = "event-stream")]
use crate::event::sys::Waker;
use crate::event::{filter::Filter, source::EventSource, timeout::PollTimeout, InternalEvent};

/// Can be used to read `InternalEvent`s.
pub(crate) struct InternalEventReader {
    events: VecDeque<InternalEvent>,
    source: Option<Box<dyn EventSource>>,
    skipped_events: Vec<InternalEvent>,
    #[cfg(unix)]
    graphics_query: Option<GraphicsQuery>,
}

#[cfg(unix)]
enum GraphicsQuery {
    Pending(PendingGraphicsQuery),
    Draining {
        deadline: Instant,
        on_input_ready: Box<dyn FnOnce() + Send>,
    },
}

#[cfg(unix)]
impl GraphicsQuery {
    fn deadline(&self) -> Instant {
        match self {
            Self::Pending(query) => query.deadline,
            Self::Draining { deadline, .. } => *deadline,
        }
    }
}

#[cfg(unix)]
struct PendingGraphicsQuery {
    deadline: Instant,
    filter: GraphicsProbeFilter,
    version: Option<String>,
    acknowledged: bool,
    on_complete: Box<dyn FnOnce(Option<String>) + Send>,
    on_input_ready: Box<dyn FnOnce() + Send>,
}

impl Default for InternalEventReader {
    fn default() -> Self {
        #[cfg(windows)]
        let source = WindowsEventSource::new();
        #[cfg(unix)]
        let source = UnixInternalEventSource::new();

        let source = source.ok().map(|x| Box::new(x) as Box<dyn EventSource>);

        InternalEventReader {
            source,
            events: VecDeque::with_capacity(32),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        }
    }
}

impl InternalEventReader {
    #[cfg(unix)]
    pub(crate) fn set_graphics_probe(&mut self, probe: GraphicsProbe) {
        if let Some(source) = &mut self.source {
            source.set_graphics_probe(probe);
        }
    }

    #[cfg(unix)]
    pub(crate) fn has_pending_graphics_query(&self) -> bool {
        self.graphics_query.is_some()
    }

    #[cfg(unix)]
    pub(crate) fn begin_graphics_query(
        &mut self,
        deadline: Instant,
        image_id: u32,
        on_complete: impl FnOnce(Option<String>) + Send + 'static,
        on_input_ready: impl FnOnce() + Send + 'static,
    ) -> io::Result<()> {
        if self.has_pending_graphics_query() {
            on_input_ready();
            return Err(io::ErrorKind::WouldBlock.into());
        }
        self.set_graphics_probe(GraphicsProbe::Started(deadline));
        self.graphics_query = Some(GraphicsQuery::Pending(PendingGraphicsQuery {
            deadline,
            filter: GraphicsProbeFilter { image_id },
            version: None,
            acknowledged: false,
            on_complete: Box::new(on_complete),
            on_input_ready: Box::new(on_input_ready),
        }));
        Ok(())
    }

    #[cfg(unix)]
    pub(crate) fn finish_graphics_query(&mut self, version: Option<String>) {
        if !matches!(&self.graphics_query, Some(GraphicsQuery::Pending(_))) {
            return;
        }
        let Some(GraphicsQuery::Pending(query)) = self.graphics_query.take() else {
            unreachable!();
        };
        if version.is_none() {
            let deadline = Instant::now() + GRAPHICS_DRAIN_TIMEOUT;
            self.set_graphics_probe(GraphicsProbe::DrainingUntil(deadline));
            self.graphics_query = Some(GraphicsQuery::Draining {
                deadline,
                on_input_ready: query.on_input_ready,
            });
            (query.on_complete)(None);
        } else {
            self.set_graphics_probe(GraphicsProbe::Finished {
                expect_late_replies: false,
            });
            (query.on_input_ready)();
            (query.on_complete)(version);
        }
    }

    #[cfg(unix)]
    fn finish_graphics_drain(&mut self) {
        if let Some(GraphicsQuery::Draining { on_input_ready, .. }) = self.graphics_query.take() {
            on_input_ready();
        }
    }

    #[cfg(unix)]
    fn complete_expired_graphics_query(&mut self) {
        if matches!(&self.graphics_query, Some(GraphicsQuery::Pending(query)) if Instant::now() >= query.deadline)
        {
            self.finish_graphics_query(None);
        }
    }

    #[cfg(unix)]
    fn handle_graphics_reply(&mut self, event: &InternalEvent) -> bool {
        let Some(GraphicsQuery::Pending(query)) = self.graphics_query.as_mut() else {
            return false;
        };
        if !query.filter.eval(event) {
            return false;
        }
        match event {
            InternalEvent::TerminalVersion(version) => query.version = Some(version.clone()),
            InternalEvent::KittyGraphicsReply { ok: true, .. } => query.acknowledged = true,
            InternalEvent::KittyGraphicsReply { ok: false, .. } => {
                self.finish_graphics_query(None);
                return true;
            }
            _ => unreachable!(),
        }
        if query.acknowledged && query.version.is_some() {
            let version = query.version.take();
            self.finish_graphics_query(version);
        }
        true
    }

    /// Returns a `Waker` allowing to wake/force the `poll` method to return `Ok(false)`.
    #[cfg(feature = "event-stream")]
    pub(crate) fn waker(&self) -> Waker {
        self.source.as_ref().expect("reader source not set").waker()
    }

    pub(crate) fn poll<F>(&mut self, timeout: Option<Duration>, filter: &F) -> io::Result<bool>
    where
        F: Filter,
    {
        #[cfg(unix)]
        self.complete_expired_graphics_query();
        for event in &self.events {
            if filter.eval(event) {
                return Ok(true);
            }
        }

        if self.source.is_none() {
            #[cfg(unix)]
            {
                self.finish_graphics_query(None);
                self.finish_graphics_drain();
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to initialize input reader",
            ));
        }

        let poll_timeout = PollTimeout::new(timeout);

        loop {
            let remaining = poll_timeout.leftover();
            #[cfg(unix)]
            let remaining = match &self.graphics_query {
                Some(query) => {
                    let query_time = query.deadline().saturating_duration_since(Instant::now());
                    Some(remaining.map_or(query_time, |remaining| remaining.min(query_time)))
                }
                None => remaining,
            };
            let result = self.source.as_mut().unwrap().try_read(remaining);
            #[cfg(unix)]
            self.complete_expired_graphics_query();
            // The source has polled through the drain deadline and processed
            // readable bytes before allowing another process to read stdin.
            #[cfg(unix)]
            if matches!(&result, Ok(None))
                && matches!(&self.graphics_query, Some(GraphicsQuery::Draining { deadline, .. }) if Instant::now() >= *deadline)
            {
                self.finish_graphics_drain();
            }
            let maybe_event = match result {
                Ok(None) => None,
                Ok(Some(event)) => {
                    #[cfg(unix)]
                    let handled = self.handle_graphics_reply(&event);
                    #[cfg(not(unix))]
                    let handled = false;
                    if !handled && filter.eval(&event) {
                        Some(event)
                    } else {
                        if !event.is_graphics_probe_reply() {
                            self.skipped_events.push(event);
                        }
                        None
                    }
                }
                Err(e) => {
                    self.events.extend(self.skipped_events.drain(..));
                    if e.kind() == io::ErrorKind::Interrupted {
                        return Ok(false);
                    }

                    #[cfg(unix)]
                    {
                        self.finish_graphics_query(None);
                        self.finish_graphics_drain();
                    }
                    return Err(e);
                }
            };

            if poll_timeout.elapsed() || maybe_event.is_some() {
                self.events.extend(self.skipped_events.drain(..));

                if let Some(event) = maybe_event {
                    self.events.push_front(event);
                    return Ok(true);
                }

                return Ok(false);
            }
        }
    }

    pub(crate) fn read<F>(&mut self, filter: &F) -> io::Result<InternalEvent>
    where
        F: Filter,
    {
        let mut skipped_events = VecDeque::new();

        loop {
            while let Some(event) = self.events.pop_front() {
                if filter.eval(&event) {
                    while let Some(event) = skipped_events.pop_front() {
                        self.events.push_back(event);
                    }

                    return Ok(event);
                } else {
                    // We can not directly write events back to `self.events`.
                    // If we did, we would put our self's into an endless loop
                    // that would enqueue -> dequeue -> enqueue etc.
                    // This happens because `poll` in this function will always return true if there are events in it's.
                    // And because we just put the non-fulfilling event there this is going to be the case.
                    // Instead we can store them into the temporary buffer,
                    // and then when the filter is fulfilled write all events back in order.
                    skipped_events.push_back(event);
                }
            }

            let _ = self.poll(None, filter)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::{collections::VecDeque, time::Duration};

    #[cfg(unix)]
    use super::super::filter::CursorPositionFilter;
    use super::{
        super::{filter::InternalEventFilter, Event},
        EventSource, InternalEvent, InternalEventReader,
    };

    #[test]
    #[cfg(unix)]
    fn graphics_handoff_waits_until_the_reader_has_drained_input() {
        use super::super::filter::EventFilter;
        use super::{GraphicsQuery, Instant};
        use std::sync::mpsc;

        let key = InternalEvent::Event(Event::Resize(10, 10));
        let source = FakeSource::with_events(&[
            InternalEvent::TerminalVersion("late terminal reply".into()),
            key.clone(),
        ]);
        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::new(),
            graphics_query: None,
        };
        let (send_ready, ready) = mpsc::channel();
        reader
            .begin_graphics_query(
                Instant::now(),
                1,
                |reply| assert_eq!(reply, None),
                move || send_ready.send(()).unwrap(),
            )
            .unwrap();
        reader.complete_expired_graphics_query();
        let Some(GraphicsQuery::Draining { deadline, .. }) = &mut reader.graphics_query else {
            panic!("a timed-out query should still drain late replies");
        };
        *deadline = Instant::now();

        let poll_now = Some(Duration::from_secs(0));
        assert!(!reader.poll(poll_now, &EventFilter).unwrap());
        assert!(reader.poll(poll_now, &EventFilter).unwrap());
        assert_eq!(reader.read(&EventFilter).unwrap(), key);
        assert_eq!(ready.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert!(!reader.poll(poll_now, &EventFilter).unwrap());
        assert_eq!(ready.try_recv(), Ok(()));
    }

    #[test]
    fn test_poll_fails_without_event_source() {
        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: None,
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert!(reader.poll(None, &InternalEventFilter).is_err());
        assert!(reader
            .poll(Some(Duration::from_secs(0)), &InternalEventFilter)
            .is_err());
        assert!(reader
            .poll(Some(Duration::from_secs(10)), &InternalEventFilter)
            .is_err());
    }

    #[test]
    fn test_poll_returns_true_for_matching_event_in_queue_at_front() {
        let mut reader = InternalEventReader {
            events: vec![InternalEvent::Event(Event::Resize(10, 10))].into(),
            source: None,
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert!(reader.poll(None, &InternalEventFilter).unwrap());
    }

    #[test]
    #[cfg(unix)]
    fn test_poll_returns_true_for_matching_event_in_queue_at_back() {
        let mut reader = InternalEventReader {
            events: vec![
                InternalEvent::Event(Event::Resize(10, 10)),
                InternalEvent::CursorPosition(10, 20),
            ]
            .into(),
            source: None,
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert!(reader.poll(None, &CursorPositionFilter).unwrap());
    }

    #[test]
    fn test_read_returns_matching_event_in_queue_at_front() {
        const EVENT: InternalEvent = InternalEvent::Event(Event::Resize(10, 10));

        let mut reader = InternalEventReader {
            events: vec![EVENT].into(),
            source: None,
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
    }

    #[test]
    #[cfg(unix)]
    fn test_read_returns_matching_event_in_queue_at_back() {
        const CURSOR_EVENT: InternalEvent = InternalEvent::CursorPosition(10, 20);

        let mut reader = InternalEventReader {
            events: vec![InternalEvent::Event(Event::Resize(10, 10)), CURSOR_EVENT].into(),
            source: None,
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(reader.read(&CursorPositionFilter).unwrap(), CURSOR_EVENT);
    }

    #[test]
    #[cfg(unix)]
    fn test_read_does_not_consume_skipped_event() {
        const SKIPPED_EVENT: InternalEvent = InternalEvent::Event(Event::Resize(10, 10));
        const CURSOR_EVENT: InternalEvent = InternalEvent::CursorPosition(10, 20);

        let mut reader = InternalEventReader {
            events: vec![SKIPPED_EVENT, CURSOR_EVENT].into(),
            source: None,
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(reader.read(&CursorPositionFilter).unwrap(), CURSOR_EVENT);
        assert_eq!(reader.read(&InternalEventFilter).unwrap(), SKIPPED_EVENT);
    }

    #[test]
    #[cfg(unix)]
    fn graphics_probe_preserves_input_and_discards_unmatched_replies() {
        use super::super::filter::{EventFilter, GraphicsProbeFilter};

        let key = |ch| InternalEvent::Event(Event::Key(super::super::KeyCode::Char(ch).into()));
        let reply = |image_id| InternalEvent::KittyGraphicsReply { image_id, ok: true };
        let source = FakeSource::with_events(&[key('a'), reply(1), key('b'), reply(2), reply(3)]);
        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };
        let filter = GraphicsProbeFilter { image_id: 2 };
        assert!(reader
            .poll(Some(Duration::from_millis(10)), &filter)
            .unwrap());
        assert_eq!(reader.read(&filter).unwrap(), reply(2));
        assert_eq!(reader.read(&EventFilter).unwrap(), key('a'));
        assert_eq!(reader.read(&EventFilter).unwrap(), key('b'));
        assert!(!reader
            .poll(Some(Duration::from_millis(1)), &EventFilter)
            .unwrap());
        assert!(reader.events.is_empty());
    }

    #[test]
    fn test_poll_timeouts_if_source_has_no_events() {
        let source = FakeSource::default();

        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert!(!reader
            .poll(Some(Duration::from_secs(0)), &InternalEventFilter)
            .unwrap());
    }

    #[test]
    fn test_poll_returns_true_if_source_has_at_least_one_event() {
        let source = FakeSource::with_events(&[InternalEvent::Event(Event::Resize(10, 10))]);

        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert!(reader.poll(None, &InternalEventFilter).unwrap());
        assert!(reader
            .poll(Some(Duration::from_secs(0)), &InternalEventFilter)
            .unwrap());
    }

    #[test]
    fn test_reads_returns_event_if_source_has_at_least_one_event() {
        const EVENT: InternalEvent = InternalEvent::Event(Event::Resize(10, 10));

        let source = FakeSource::with_events(&[EVENT]);

        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
    }

    #[test]
    fn test_read_returns_events_if_source_has_events() {
        const EVENT: InternalEvent = InternalEvent::Event(Event::Resize(10, 10));

        let source = FakeSource::with_events(&[EVENT, EVENT, EVENT]);

        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
    }

    #[test]
    fn test_poll_returns_false_after_all_source_events_are_consumed() {
        const EVENT: InternalEvent = InternalEvent::Event(Event::Resize(10, 10));

        let source = FakeSource::with_events(&[EVENT, EVENT, EVENT]);

        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
        assert!(!reader
            .poll(Some(Duration::from_secs(0)), &InternalEventFilter)
            .unwrap());
    }

    #[test]
    fn test_poll_propagates_error() {
        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(FakeSource::new(&[]))),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(
            reader
                .poll(Some(Duration::from_secs(0)), &InternalEventFilter)
                .err()
                .map(|e| format!("{:?}", &e.kind())),
            Some(format!("{:?}", io::ErrorKind::Other))
        );
    }

    #[test]
    fn test_read_propagates_error() {
        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(FakeSource::new(&[]))),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(
            reader
                .read(&InternalEventFilter)
                .err()
                .map(|e| format!("{:?}", &e.kind())),
            Some(format!("{:?}", io::ErrorKind::Other))
        );
    }

    #[test]
    fn test_poll_continues_after_error() {
        const EVENT: InternalEvent = InternalEvent::Event(Event::Resize(10, 10));

        let source = FakeSource::new(&[EVENT, EVENT]);

        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
        assert!(reader.read(&InternalEventFilter).is_err());
        assert!(reader
            .poll(Some(Duration::from_secs(0)), &InternalEventFilter)
            .unwrap());
    }

    #[test]
    fn test_read_continues_after_error() {
        const EVENT: InternalEvent = InternalEvent::Event(Event::Resize(10, 10));

        let source = FakeSource::new(&[EVENT, EVENT]);

        let mut reader = InternalEventReader {
            events: VecDeque::new(),
            source: Some(Box::new(source)),
            skipped_events: Vec::with_capacity(32),
            #[cfg(unix)]
            graphics_query: None,
        };

        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
        assert!(reader.read(&InternalEventFilter).is_err());
        assert_eq!(reader.read(&InternalEventFilter).unwrap(), EVENT);
    }

    #[derive(Default)]
    struct FakeSource {
        events: VecDeque<InternalEvent>,
        error: Option<io::Error>,
    }

    impl FakeSource {
        fn new(events: &[InternalEvent]) -> FakeSource {
            FakeSource {
                events: events.to_vec().into(),
                error: Some(io::Error::new(io::ErrorKind::Other, "")),
            }
        }

        fn with_events(events: &[InternalEvent]) -> FakeSource {
            FakeSource {
                events: events.to_vec().into(),
                error: None,
            }
        }
    }

    impl EventSource for FakeSource {
        fn try_read(&mut self, _timeout: Option<Duration>) -> io::Result<Option<InternalEvent>> {
            // Return error if set in case there's just one remaining event
            if self.events.len() == 1 {
                if let Some(error) = self.error.take() {
                    return Err(error);
                }
            }

            // Return all events from the queue
            if let Some(event) = self.events.pop_front() {
                return Ok(Some(event));
            }

            // Return error if there're no more events
            if let Some(error) = self.error.take() {
                return Err(error);
            }

            // Timeout
            Ok(None)
        }

        #[cfg(feature = "event-stream")]
        fn waker(&self) -> super::super::sys::Waker {
            unimplemented!();
        }
    }
}
