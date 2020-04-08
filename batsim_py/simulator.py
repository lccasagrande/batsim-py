import signal
import atexit
import subprocess
import os
import math
import tempfile
from shutil import which
from collections import defaultdict

from procset import ProcSet
from pydispatch import dispatcher

from .events import JobEvent
from .events import HostEvent
from .events import SimulatorEvent
from .utils.commons import get_free_tcp_address
from .protocol import get_platform_from_xml
from .protocol import NetworkHandler
from .protocol import BatsimMessage
from .protocol import BatsimNotify
from .protocol import BatsimNotifyType
from .protocol import BatsimEventType
from .protocol import BatsimRequest
from .protocol import RegisterProfileBatsimRequest
from .protocol import RegisterJobBatsimRequest
from .protocol import CallMeLaterBatsimRequest
from .protocol import RequestedCallBatsimEvent
from .protocol import KillJobBatsimRequest
from .protocol import ExecuteJobBatsimRequest
from .protocol import RejectJobBatsimRequest
from .protocol import SetResourceStateBatsimRequest

if which('batsim') is None:
    raise ImportError(
        "(HINT: you need to install Batsim. Check the setup instructions here: https://batsim.readthedocs.io/en/latest/.)")


class SimulatorHandler:
    OFFSET = 0.99

    def __init__(self, tcp_address=None):
        self.__network = NetworkHandler(tcp_address or get_free_tcp_address())
        self.__current_time = 0.
        self.__simulator = None
        self.__simulation_time = None
        self.__platform = None
        self.__no_more_jobs_to_submit = False
        self.__batsim_requests = []
        self.__jobs = []
        self.__job_profiles = defaultdict(dict)
        self.__callbacks = defaultdict(list)

        # Batsim events handlers
        self.__batsim_event_handlers = {
            BatsimEventType.SIMULATION_ENDS: self.__on_batsim_simulation_ends,
            BatsimEventType.JOB_COMPLETED: self.__on_batsim_job_completed,
            BatsimEventType.JOB_SUBMITTED: self.__on_batsim_job_submitted,
            BatsimEventType.RESOURCE_STATE_CHANGED: self.__on_batsim_host_state_changed,
            BatsimEventType.REQUESTED_CALL: self.__on_batsim_requested_call,
            BatsimNotifyType.NO_MORE_STATIC_JOB_TO_SUBMIT: self.__on_batsim_no_more_jobs_to_submit
        }

        atexit.register(self.__close_simulator)
        signal.signal(signal.SIGTERM, self.__on_sigterm)

    @property
    def jobs(self):
        return list(self.__jobs)

    @property
    def queue(self):
        return [j for j in self.__jobs if j.is_submitted]

    @property
    def agenda(self):
        return [(h, h.jobs) for h in self.__platform.hosts]

    @property
    def platform(self):
        return self.__platform

    @property
    def is_running(self):
        return self.__simulator is not None

    @property
    def current_time(self):
        return math.floor(self.__current_time)

    @property
    def is_submitter_finished(self):
        return self.__no_more_jobs_to_submit

    def start(self, platform, workload, simulation_time=None):
        assert not self.is_running, "Simulation is already running"
        self.__platform = get_platform_from_xml(platform)
        self.__jobs = []
        self.__current_time = 0.
        self.__simulation_time = simulation_time
        self.__no_more_jobs_to_submit = False

        cmd = "batsim -E --forward-profiles-on-submission \
                --disable-schedule-tracing \
                --disable-machine-state-tracing"
        cmd += " -s {} -p {} -w {}".format(
            self.__network.address, platform, workload)

        # There isn't an option to avoid exporting batsim results
        cmd += " -e {}".format(tempfile.gettempdir() + "/batsim")

        self.__simulator = subprocess.Popen(
            cmd.split(), stdout=subprocess.PIPE, shell=False)

        self.__network.bind()
        self.__read_batsim_events()

        if self.__simulation_time:
            self.__set_batsim_call_me_later(self.__simulation_time)

        # We use the offset value to get all events from time 0 to OFFSET.
        self.__set_batsim_call_me_later(self.OFFSET)
        while self.is_running and self.__current_time < self.OFFSET:
            self.__goto_next_batsim_event()

        dispatcher.send(signal=SimulatorEvent.SIMULATION_BEGINS, sender=self)

    def close(self):
        if not self.is_running:
            return
        self.__close_simulator()
        self.__network.close()
        self.__simulation_time = None
        self.__batsim_requests.clear()
        self.__job_profiles.clear()
        self.__callbacks.clear()
        dispatcher.send(signal=SimulatorEvent.SIMULATION_ENDS, sender=self)

    def proceed_time(self, time=0):
        assert time >= 0
        assert self.is_running, "Simulation is not running."

        if time == 0:
            # Go to the next event.
            next_decision_time = self.current_time
        elif not self.__simulation_time and self.is_submitter_finished and not self.__jobs:
            # There are no more actions to do. Go to the next event.
            next_decision_time = self.current_time
        else:
            # Setup a call me later request and force it to be the last event
            next_decision_time = int(time) + self.current_time + self.OFFSET
            self.__set_batsim_call_me_later(next_decision_time)

        self.__goto_next_batsim_event()
        self.__start_runnable_jobs()
        while self.is_running and self.__current_time < next_decision_time:
            self.__goto_next_batsim_event()
            self.__start_runnable_jobs()

    def set_callback(self, at, call):
        assert self.is_running, "Simulation is not running."
        assert at > self.current_time
        assert callable(call), "The call argument must be callable."
        self.__callbacks[at].append(call)
        self.__set_batsim_call_me_later(at)

    def allocate(self, job_id, hosts_id):
        assert self.is_running, "Simulation is not running."

        job = next(j for j in self.__jobs if j.id == job_id)
        hosts = self.__platform.get(hosts_id)

        # Allocate
        for host in hosts:
            host._allocate(job)
        job._allocate(hosts_id)

        # Start
        self.__start_runnable_jobs()

    def kill_job(self, job_id):
        assert self.is_running, "Simulation is not running."

        index = next(i for i, j in enumerate(self.__jobs) if j.id == job_id)
        del self.__jobs[index]

        # Sync Batsim
        request = KillJobBatsimRequest(self.current_time, job_id)
        self.__batsim_requests.append(request)

    def reject_job(self, job_id):
        assert self.is_running, "Simulation is not running."

        index = next(i for i, j in enumerate(self.__jobs) if j.id == job_id)
        del self.__jobs[index]

        # Sync Batsim
        request = RejectJobBatsimRequest(self.current_time, job.id)
        self.__batsim_requests.append(request)

    def switch_on(self, hosts_id):
        assert self.is_running, "Simulation is not running."

        for host in self.__platform.get(hosts_id):
            host._switch_on()
            ending_pstate = host.get_default_pstate()

            # Sync Batsim
            self.__set_batsim_host_pstate(host.id, ending_pstate.id)

    def switch_off(self, hosts_id):
        assert self.is_running, "Simulation is not running."

        for host in self.__platform.get(hosts_id):
            host._switch_off()
            ending_pstate = host.get_sleep_pstate()

            # Sync Batsim
            self.__set_batsim_host_pstate(host.id, ending_pstate.id)

    def switch_power_state(self, host_id, pstate_id):
        assert self.is_running, "Simulation is not running."

        host = self.__platform.get(host_id)
        host._set_computation_pstate(pstate_id)

        # Sync Batsim
        self.__set_batsim_host_pstate(host.id, host.pstate.id)

    def __start_runnable_jobs(self):
        for job in [j for j in self.__jobs if j.is_runnable]:
            is_ready = True

            # Check if all hosts are active and switch on sleeping hosts
            for host in self.__platform.get(job.allocation):
                if host.is_sleeping:
                    self.switch_on(host.id)
                    is_ready = False
                elif host.is_switching_on or host.is_switching_off:
                    is_ready = False

            if is_ready:
                job._start(self.current_time)

                # Sync Batsim
                request = ExecuteJobBatsimRequest(
                    self.current_time, job.id, job.allocation)
                self.__batsim_requests.append(request)

    def __goto_next_batsim_event(self):
        self.__send_and_read_batsim_events()
        if self.__simulation_time and self.current_time >= self.__simulation_time:
            self.close()

    def __close_simulator(self):
        if self.__simulator:
            self.__simulator.terminate()
            outs, errs = self.__simulator.communicate()
            self.__simulator = None

    def __set_batsim_call_me_later(self, at):
        if at == self.current_time:
            return
        request = CallMeLaterBatsimRequest(self.current_time, at)
        if not any(r.type == request.type and r.at == request.at for r in self.__batsim_requests):
            self.__batsim_requests.append(request)

    def __set_batsim_host_pstate(self, host_id, pstate_id):
        request = next((r for r in self.__batsim_requests if r.timestamp == self.current_time and isinstance(
            r, SetResourceStateBatsimRequest) and r.state == pstate_id), None)
        if request:
            request.add_resource(host_id)
        else:
            self.__batsim_requests.append(SetResourceStateBatsimRequest(
                self.current_time, [host_id], pstate_id))

    def __read_batsim_events(self):
        msg = self.__network.recv()
        for event in msg.events:
            self.__current_time = event.timestamp
            handler = self.__batsim_event_handlers.get(event.type, None)
            if handler:
                handler(event)
        self.__current_time = msg.now

    def __send_requests(self):
        msg = BatsimMessage(self.__current_time,
                            sorted(self.__batsim_requests, key=lambda r: r.timestamp))
        self.__batsim_requests.clear()
        self.__network.send(msg)

    def __send_and_read_batsim_events(self):
        self.__send_requests()
        return self.__read_batsim_events()

    def __on_batsim_simulation_ends(self, event):
        if self.__simulator:
            self.__network.send(BatsimMessage(self.current_time, []))  # ack
            self.__simulator.wait(5)
        self.close()

    def __on_batsim_host_state_changed(self, event):
        for host in self.__platform.get(event.resources):
            if host.is_switching_off:
                host._set_off()
            elif host.is_switching_on:
                host._set_on()
            elif (host.is_idle or host.is_computing) and host.pstate.type != event.state.type:
                host._set_computation_pstate(int(event.state))

            assert host.pstate.type == event.state
        self.__start_runnable_jobs()

    def __on_batsim_requested_call(self, event):
        if self.current_time in self.__callbacks:
            for callback in self.__callbacks[self.current_time]:
                callback(self.current_time)
            del self.__callbacks[self.current_time]

    def __on_batsim_job_submitted(self, event):
        self.__jobs.append(event.job)
        event.job._submit(self.current_time)

    def __on_batsim_job_completed(self, event):
        i = next(i for i, j in enumerate(self.__jobs) if j.id == event.job_id)
        job = self.__jobs.pop(i)
        job._terminate(self.current_time, event.job_state)
        self.__start_runnable_jobs()

    def __on_batsim_no_more_jobs_to_submit(self, event):
        self.__no_more_jobs_to_submit = True

    def __on_sigterm(self, signum, frame):
        self.__close_simulator()
        sys.exit(signum)