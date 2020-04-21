from typing import Optional
from typing import Any
from typing import Callable

from pydispatch import dispatcher

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
        TypeError: In case of invalid arguments type.
    """
    if not callable(listener):
        raise TypeError('Expected `listener` argument to be callable '
                        'got {}.'.format(listener))

    if not isinstance(event, Event):
        raise TypeError('Expected `event` argument to be an instance of '
                        '`Event`, got {}.'.format(event))

    dispatcher.connect(listener, signal=event, sender=sender)


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
        TypeError: In case of invalid arguments type.
    """
    if not isinstance(event, Event):
        raise TypeError('Expected `event` argument to be an instance of '
                        '`Event`, got {}.'.format(event))

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
        TypeError: In case of invalid arguments type.
    """
    if not callable(listener):
        raise TypeError('Expected `listener` argument to be callable '
                        'got {}.'.format(listener))

    if not isinstance(event, Event):
        raise TypeError('Expected `event` argument to be an instance of '
                        '`Event`, got {}.'.format(event))

    dispatcher.disconnect(listener, signal=event, sender=sender)


def unsubscribe_listeners(event: Event,
                          sender: Any = dispatcher.Any) -> None:
    """ Unsubscribe all listeners from an event 

    Args:
        event: the event to which the listener should unsubscribe. 
        sender: the sender to which the listener should unsubscribe.
            Defaults to Any.
    Raises:
        TypeError: In case of invalid arguments type.
    """
    if not isinstance(event, Event):
        raise TypeError('Expected `event` argument to be an instance of '
                        '`Event`, got {}.'.format(event))

    listeners = dispatcher.getReceivers(sender, event)
    for l in list(dispatcher.liveReceivers(listeners)):
        unsubscribe(l, event, sender)
