import batsim_py.dispatcher as dispatcher
import batsim_py.events as events
import batsim_py.jobs as jobs


class TestDispatcher:
    def test_subscribe_valid(self):
        def foo(sender):
            self.__called = True

        self.__called = False
        dispatcher.subscribe(foo, events.JobEvent.COMPLETED)
        dispatcher.dispatch(events.JobEvent.COMPLETED)
        assert self.__called

    def test_subscribe_with_sender(self):
        def foo(sender):
            self.__called1 = True

        def foo2(sender):
            self.__called2 = True

        self.__called1 = self.__called2 = False
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j2 = jobs.Job("2", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        dispatcher.subscribe(foo, events.JobEvent.SUBMITTED, sender=j)
        dispatcher.subscribe(foo2, events.JobEvent.SUBMITTED, sender=j2)
        j._submit(0)
        assert self.__called1 and not self.__called2

    def test_subscribe_with_sender_any(self):
        def foo(sender):
            self.__called1 += 1

        def foo2(sender):
            self.__called2 += 1

        self.__called1 = self.__called2 = 0
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j2 = jobs.Job("2", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        dispatcher.subscribe(foo, events.JobEvent.SUBMITTED, sender=j)
        dispatcher.subscribe(foo2, events.JobEvent.SUBMITTED)
        j._submit(0)
        j2._submit(0)
        assert self.__called1 == 1 and self.__called2 == 2

    def test_dispatch_unique(self):
        def foo(sender):
            self.__called += 1

        self.__called = 0
        dispatcher.subscribe(foo, events.JobEvent.COMPLETED)
        for _ in range(5):
            dispatcher.dispatch(events.JobEvent.COMPLETED, unique=True)
        assert self.__called == 1

    def test_close_connections(self):
        def foo(sender):
            self.__called += 1

        self.__called = 0
        dispatcher.subscribe(foo, events.JobEvent.SUBMITTED)
        dispatcher.close_connections()
        dispatcher.dispatch(events.JobEvent.SUBMITTED)
        assert self.__called == 0

    def test_unsubscribe_listeners_with_sender(self):
        def foo(sender):
            self.__called += 1

        def foo2(sender):
            self.__called2 += 1

        self.__called = self.__called2 = 0
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j2 = jobs.Job("2", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        dispatcher.subscribe(foo, events.JobEvent.SUBMITTED, sender=j)
        dispatcher.subscribe(foo2, events.JobEvent.SUBMITTED)
        dispatcher.unsubscribe_listeners(events.JobEvent.SUBMITTED, j)
        j._submit(0)
        j2._submit(0)

        assert self.__called == 0 and self.__called2 == 2

    def test_unsubscribe_listeners_with_any(self):
        def foo(sender):
            self.__called += 1

        def foo2(sender):
            self.__called2 += 1

        self.__called = self.__called2 = 0
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j2 = jobs.Job("2", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        dispatcher.subscribe(foo, events.JobEvent.SUBMITTED, sender=j)
        dispatcher.subscribe(foo2, events.JobEvent.SUBMITTED)
        dispatcher.unsubscribe_listeners(events.JobEvent.SUBMITTED)
        j._submit(0)
        j2._submit(0)

        assert self.__called == 1 and self.__called2 == 0
