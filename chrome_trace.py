# Copyright 2016 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""Timeline visualization for TensorFlow using Chrome Trace Format."""
#https://github.com/tensorflow/tensorflow/blob/master/tensorflow/python/client/timeline.py
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import json

class _ChromeTraceFormatter(object):
  """A helper class for generating traces in Chrome Trace Format."""

  def __init__(self, show_memory=False):
    """Constructs a new Chrome Trace formatter."""
    self._show_memory = show_memory
    self._events = []
    self._metadata = []

  def _create_event(self, ph, category, name, pid, tid, timestamp):
    """Creates a new Chrome Trace event.
    For details of the file format, see:
    https://github.com/catapult-project/catapult/blob/master/tracing/README.md
    Args:
      ph:  The type of event - usually a single character.
      category: The event category as a string.
      name:  The event name as a string.
      pid:  Identifier of the process generating this event as an integer.
      tid:  Identifier of the thread generating this event as an integer.
      timestamp:  The timestamp of this event as a long integer.
    Returns:
      A JSON compatible event object.
    """
    event = {}
    event['ph'] = ph
    event['cat'] = category
    event['name'] = name
    event['pid'] = pid
    event['tid'] = tid
    event['ts'] = timestamp
    return event

  def emit_pid(self, name, pid):
    """Adds a process metadata event to the trace.
    Args:
      name:  The process name as a string.
      pid:  Identifier of the process as an integer.
    """
    event = {}
    event['name'] = 'process_name'
    event['ph'] = 'M'
    event['pid'] = pid
    event['args'] = {'name': name}
    self._metadata.append(event)

  def emit_tid(self, name, pid, tid):
    """Adds a thread metadata event to the trace.
    Args:
      name:  The thread name as a string.
      pid:  Identifier of the process as an integer.
      tid:  Identifier of the thread as an integer.
    """
    event = {}
    event['name'] = 'thread_name'
    event['ph'] = 'M'
    event['pid'] = pid
    event['tid'] = tid
    event['args'] = {'name': name}
    self._metadata.append(event)

  def emit_region(self, timestamp, duration, pid, tid, category, name, args):
    """Adds a region event to the trace.
    Args:
      timestamp:  The start timestamp of this region as a long integer.
      duration:  The duration of this region as a long integer.
      pid:  Identifier of the process generating this event as an integer.
      tid:  Identifier of the thread generating this event as an integer.
      category: The event category as a string.
      name:  The event name as a string.
      args:  A JSON-compatible dictionary of event arguments.
    """
    event = self._create_event('X', category, name, pid, tid, timestamp)
    event['dur'] = duration
    event['args'] = args
    self._events.append(event)

  def emit_obj_create(self, category, name, timestamp, pid, tid, object_id):
    """Adds an object creation event to the trace.
    Args:
      category: The event category as a string.
      name:  The event name as a string.
      timestamp:  The timestamp of this event as a long integer.
      pid:  Identifier of the process generating this event as an integer.
      tid:  Identifier of the thread generating this event as an integer.
      object_id: Identifier of the object as an integer.
    """
    event = self._create_event('N', category, name, pid, tid, timestamp)
    event['id'] = object_id
    self._events.append(event)

  def emit_obj_delete(self, category, name, timestamp, pid, tid, object_id):
    """Adds an object deletion event to the trace.
    Args:
      category: The event category as a string.
      name:  The event name as a string.
      timestamp:  The timestamp of this event as a long integer.
      pid:  Identifier of the process generating this event as an integer.
      tid:  Identifier of the thread generating this event as an integer.
      object_id: Identifier of the object as an integer.
    """
    event = self._create_event('D', category, name, pid, tid, timestamp)
    event['id'] = object_id
    self._events.append(event)

  def emit_obj_snapshot(self, category, name, timestamp, pid, tid, object_id,
                        snapshot):
    """Adds an object snapshot event to the trace.
    Args:
      category: The event category as a string.
      name:  The event name as a string.
      timestamp:  The timestamp of this event as a long integer.
      pid:  Identifier of the process generating this event as an integer.
      tid:  Identifier of the thread generating this event as an integer.
      object_id: Identifier of the object as an integer.
      snapshot:  A JSON-compatible representation of the object.
    """
    event = self._create_event('O', category, name, pid, tid, timestamp)
    event['id'] = object_id
    event['args'] = {'snapshot': snapshot}
    self._events.append(event)

  def emit_flow_start(self, name, timestamp, pid, tid, flow_id):
    """Adds a flow start event to the trace.
    When matched with a flow end event (with the same 'flow_id') this will
    cause the trace viewer to draw an arrow between the start and end events.
    Args:
      name:  The event name as a string.
      timestamp:  The timestamp of this event as a long integer.
      pid:  Identifier of the process generating this event as an integer.
      tid:  Identifier of the thread generating this event as an integer.
      flow_id: Identifier of the flow as an integer.
    """
    event = self._create_event('s', 'DataFlow', name, pid, tid, timestamp)
    event['id'] = flow_id
    self._events.append(event)

  def emit_flow_end(self, name, timestamp, pid, tid, flow_id):
    """Adds a flow end event to the trace.
    When matched with a flow start event (with the same 'flow_id') this will
    cause the trace viewer to draw an arrow between the start and end events.
    Args:
      name:  The event name as a string.
      timestamp:  The timestamp of this event as a long integer.
      pid:  Identifier of the process generating this event as an integer.
      tid:  Identifier of the thread generating this event as an integer.
      flow_id: Identifier of the flow as an integer.
    """
    event = self._create_event('t', 'DataFlow', name, pid, tid, timestamp)
    event['id'] = flow_id
    self._events.append(event)

  def emit_counter(self, category, name, pid, timestamp, counter, value):
    """Emits a record for a single counter.
    Args:
      category: The event category as a string.
      name:  The event name as a string.
      pid:  Identifier of the process generating this event as an integer.
      timestamp:  The timestamp of this event as a long integer.
      counter: Name of the counter as a string.
      value:  Value of the counter as an integer.
    """
    event = self._create_event('C', category, name, pid, 0, timestamp)
    event['args'] = {counter: value}
    self._events.append(event)

  def emit_counters(self, category, name, pid, timestamp, counters):
    """Emits a counter record for the dictionary 'counters'.
    Args:
      category: The event category as a string.
      name:  The event name as a string.
      pid:  Identifier of the process generating this event as an integer.
      timestamp:  The timestamp of this event as a long integer.
      counters: Dictionary of counter values.
    """
    event = self._create_event('C', category, name, pid, 0, timestamp)
    event['args'] = counters.copy()
    self._events.append(event)

  def format_to_string(self, pretty=False):
    """Formats the chrome trace to a string.
    Args:
      pretty: (Optional.)  If True, produce human-readable JSON output.
    Returns:
      A JSON-formatted string in Chrome Trace format.
    """
    trace = {}
    trace['traceEvents'] = self._metadata + self._events
    if pretty:
      return json.dumps(trace, indent=4, separators=(',', ': '))
    else:
      return json.dumps(trace, separators=(',', ':'))