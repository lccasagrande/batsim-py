from typing import Optional
from typing import Any
from typing import Callable

from pydispatch import dispatcher
from pydispatch import errors
from pydispatch import saferef

from .events import Event


def subscribe(listener: Callable[[Any], None],
              event: Event,
              sender: Any = dispatcher.Any) -> None:
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
             sender: Any = dispatcher.Any,
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


def unsubscribe(listener: Callable[[Any], None],
                event: Event,
                sender: Any = dispatcher.Any) -> None:
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


def __disconnect_by_id(receiver: Any,
                       signal: Any,
                       sender_id: Any,
                       weak: bool = True) -> None:
    """Disconnect receiver from sender for signal using sender ID.

    receiver -- the registered receiver to disconnect
    signal -- the registered signal to disconnect
    sender_id -- the registered sender id to disconnect
    weak -- the weakref state to disconnect

    disconnect reverses the process of connect,
    the semantics for the individual elements are
    logically equivalent to a tuple of
    (receiver, signal, sender, weak) used as a key
    to be deleted from the internal routing tables.
    (The actual process is slightly more complex
    but the semantics are basically the same).

    Note:
        Using disconnect is not required to cleanup
        routing when an object is deleted, the framework
        will remove routes for deleted objects
        automatically.  It's only necessary to disconnect
        if you want to stop routing to a live object.

    returns None, may raise DispatcherTypeError or
        DispatcherKeyError
    """
    if signal is None:
        raise errors.DispatcherTypeError('Signal cannot be None (receiver={} '
                                         'sender={})'.format(receiver, sender_id))
    if weak:
        receiver = saferef.safeRef(receiver)
    try:
        signals = dispatcher.connections[sender_id]
        receivers = signals[signal]
    except KeyError:
        raise errors.DispatcherKeyError('No receivers found for signal {} from '
                                        'sender {}'.format(signal, sender_id))
    try:
        dispatcher._removeOldBackRefs(sender_id, signal, receiver, receivers)
    except ValueError:
        raise errors.DispatcherKeyError('No connection to receiver {} for '
                                        'signal {} from sender {}'
                                        ''.format(receiver, signal, sender_id))

    dispatcher._cleanupConnections(sender_id, signal)


def close_connections():
    for sender_id, events in list(dispatcher.connections.items()):
        for event, listeners in list(events.items()):
            for l in list(dispatcher.liveReceivers(listeners)):
                __disconnect_by_id(l, event, sender_id)


def unsubscribe_listeners(event: Event,
                          sender: Any = dispatcher.Any) -> None:
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
