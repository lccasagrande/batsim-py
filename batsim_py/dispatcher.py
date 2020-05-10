from typing import Any as AnyType
from typing import Callable

from pydispatch import dispatcher
from pydispatch import saferef

from .events import Event

Any = dispatcher.Any


def subscribe(listener: Callable[[AnyType], None],
              event: Event,
              sender: AnyType = Any) -> None:
    """ Subscribe to an event 

    Args:
        listener: a callable Python object which is to receive
            messages/signals/events. Must be hashable objects.
        event: the event to which the listener should subscribe. 
        sender: the sender to which the listener should subscribe.
            Defaults to Any. If Any, listener will receive the 
            indicated event from any sender.
    Raises:
        AssertionError: In case of invalid arguments type.
    """
    assert callable(listener)
    assert isinstance(event, Event)

    dispatcher.connect(listener, signal=event, sender=sender, weak=True)


def dispatch(event: Event,
             sender: AnyType = Any,
             unique: bool = False) -> None:
    """ Dispatch an event 

    Args:
        event: the event to dispatch. 
        sender: the sender of the signal. Defaults to Any. If Any, only listeners
            registered for Any will receive the message.
        unique: whether this event can be dispatched again by the same sender.
            Defaults to False. If true, all listeners will be automatically
            disconnected after the event is dispatched.

    Raises:
        AssertionError: In case of invalid arguments type.
    """
    assert isinstance(event, Event)
    dispatcher.send(signal=event, sender=sender)

    if unique:
        unsubscribe_listeners(event, sender)


def unsubscribe(listener: Callable[[AnyType], None],
                event: Event,
                sender: AnyType = Any) -> None:
    """ Unsubscribe to an event 

    Args:
        listener: the listener to remove from the subscription list.
        event: the event to which the listener should unsubscribe. 
        sender: the sender to which the listener should unsubscribe. 
            Defaults to Any.
    Raises:
        AssertionError: In case of invalid arguments type.
    """
    assert callable(listener)
    assert isinstance(event, Event)

    dispatcher.disconnect(listener, signal=event, sender=sender, weak=True)


def close_connections():
    for sender_id, events in list(dispatcher.connections.items()):
        for event, listeners in list(events.items()):
            for listener in list(dispatcher.liveReceivers(listeners)):
                l = saferef.safeRef(listener)
                dispatcher._removeOldBackRefs(sender_id, event, l, listeners)
                dispatcher._cleanupConnections(sender_id, event)


def unsubscribe_listeners(event: Event,
                          sender: AnyType = Any) -> None:
    """ Unsubscribe all listeners from an event 

    Args:
        event: the event to which the listener should unsubscribe. 
        sender: the sender to which the listener should unsubscribe.
            Defaults to Any.
    Raises:
        AssertionError: In case of invalid arguments type.
    """
    assert isinstance(event, Event)

    listeners = dispatcher.getReceivers(sender, event)
    for l in list(dispatcher.liveReceivers(listeners)):
        unsubscribe(l, event, sender)
