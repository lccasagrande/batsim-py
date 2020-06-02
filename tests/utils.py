import random
from typing import Optional, Tuple
from typing import Sequence
from typing import Dict

from procset import ProcSet

from batsim_py.jobs import JobState


class BatsimAPI:
    @staticmethod
    def get_message(now: float = 0, events: Sequence[dict] = ()) -> dict:
        d = {
            "now": now,
            "events": list(events)
        }
        return d

    @staticmethod
    def get_config(
            allow_compute_sharing: bool = True,
            allow_storage_sharing: bool = True,
            redis_enabled: bool = False,
            redis_hostname: str = "127.0.0.1",
            redis_port: int = 6379,
            redis_prefix: str = "default",
            profiles_forwarded: bool = True,
            dynamic_jobs_enabled: bool = False,
            dynamic_jobs_ack: bool = False,
            profile_reuse_enabled: bool = False,
            forward_unknown_events: bool = False) -> dict:
        d = {
            "allow_compute_sharing": allow_compute_sharing,
            "allow_storage_sharing": allow_storage_sharing,
            "config": {
                "redis-enabled": redis_enabled,
                "redis-hostname": redis_hostname,
                "redis-port": redis_port,
                "redis-prefix": redis_prefix,
                "profiles-forwarded-on-submission": profiles_forwarded,
                "dynamic-jobs-enabled": dynamic_jobs_enabled,
                "dynamic-jobs-acknowledged": dynamic_jobs_ack,
                "profile-reuse-enabled": profile_reuse_enabled,
                "forward-unknown-events": forward_unknown_events
            }
        }
        return d


class BatsimPlatformAPI:
    @staticmethod
    def get_resource_properties(
            role: str = "COMPUTE",
            watt_off: float = 9.,
            watt_switch_off: float = 120,
            watt_switch_on: float = 100,
            watt_on: Sequence[Tuple[int, int]] = ((90, 180),)) -> dict:

        w_off = "{}:{}".format(watt_off, watt_off)
        w_s_off = "{}:{}".format(watt_switch_off, watt_switch_off)
        w_s_on = "{}:{}".format(watt_switch_on, watt_switch_on)
        w_on = ",".join("{}:{}".format(w[0], w[1]) for w in watt_on)
        watt_per_state = ",".join([w_off, w_s_off, w_s_on, w_on])
        d = {
            "role": role,
            "watt_off": watt_off,
            "watt_per_state": watt_per_state,
            "sleep_pstates": "0:1:2"
        }
        return d

    @staticmethod
    def get_resource(
            id: int = 0,
            name: str = "0",
            properties: dict = None) -> dict:
        props = properties or BatsimPlatformAPI.get_resource_properties()
        d = {
            "id": id,
            "name": name,
            "state": "idle",
            "properties": props
        }

        return d


class BatsimEventAPI:
    @staticmethod
    def get_job_submitted(
            timestamp: float = 0,
            job_id: str = "w!0",
            profile_name: str = "p",
            res: int = 1,
            walltime: Optional[float] = None,
            profile: dict = None,
            **job_kwargs) -> dict:
        d: dict = {
            "timestamp": timestamp,
            "type": "JOB_SUBMITTED",
            "data": {
                "job_id": job_id,
                "job": {
                    "profile": profile_name,
                    "res": res,
                    "id": job_id
                }
            }
        }
        if walltime is not None:
            d["data"]["job"]["walltime"] = walltime

        for k, v in job_kwargs.items():
            d['data']['job'][k] = v

        d["data"]["profile"] = profile or BatsimJobProfileAPI.get_delay()
        return d

    @staticmethod
    def get_job_completted(
            timestamp: float = 0,
            job_id: str = "w!0",
            job_state: JobState = JobState.COMPLETED_SUCCESSFULLY,
            return_code: int = 1,
            alloc: Sequence[int] = (1, 2)) -> dict:
        d: dict = {
            "timestamp": timestamp,
            "type": "JOB_COMPLETED",
            "data": {
                "job_id": job_id,
                "job_state": str(job_state),
                "return_code": return_code,
                "alloc": str(ProcSet(*alloc))
            }
        }
        return d

    @staticmethod
    def get_simulation_begins(
            timestamp: float = 0,
            config: dict = None,
            resources: Sequence[dict] = (),
            storages: Sequence[dict] = (),
            workloads: dict = None,
            profiles: Dict[str, Dict[str, dict]] = None) -> dict:

        if workloads or profiles:
            assert profiles and workloads
            assert all(n in profiles for n in workloads.keys())

        data = config or BatsimAPI.get_config()
        compute_resources = resources or [BatsimPlatformAPI.get_resource()]
        data["nb_resources"] = len(compute_resources) + len(storages)
        data["nb_compute_resources"] = len(compute_resources)
        data["nb_storage_resources"] = len(storages)
        data["compute_resources"] = list(compute_resources)
        data["storage_resources"] = list(storages)
        data["workloads"] = workloads or {}
        data["profiles"] = profiles or {}
        d = {
            "timestamp": timestamp,
            "type": "SIMULATION_BEGINS",
            "data": data
        }
        return d

    @staticmethod
    def get_simulation_ends(timestamp: float = 0) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "SIMULATION_ENDS",
            "data": {}
        }
        return d

    @staticmethod
    def get_resource_state_changed(
            timestamp: float = 0,
            resources: Sequence[int] = (1, 2, 3),
            state: int = 0) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "RESOURCE_STATE_CHANGED",
            "data": {
                "resources": str(ProcSet(*resources)),
                "state": str(state)
            }
        }
        return d

    @staticmethod
    def get_job_killed(timestamp: float = 0,
                       job_ids: Sequence[str] = ("w!0", "w!1")) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "JOB_KILLED",
            "data": {"job_ids": list(job_ids)}
        }
        return d

    @staticmethod
    def get_requested_call(timestamp: float = 0) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "REQUESTED_CALL",
            "data": {}
        }
        return d

    @staticmethod
    def get_notify_machine_unavailable(timestamp: float = 0, resources: Sequence[int] = (1,)) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "NOTIFY",
            "data": {
                "type": "EVENT_MACHINE_UNAVAILABLE",
                "resources": str(ProcSet(*resources)),
                "timestamp": timestamp
            },
        }
        return d

    @staticmethod
    def get_notify_machine_available(timestamp: float = 0, resources: Sequence[int] = (1,)) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "NOTIFY",
            "data": {
                "type": "EVENT_MACHINE_AVAILABLE",
                "resources": str(ProcSet(*resources)),
                "timestamp": timestamp
            },
        }
        return d

    @staticmethod
    def get_notify_no_more_static_job_to_submit(timestamp: float = 0) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "NOTIFY",
            "data": {"type": "no_more_static_job_to_submit"},
        }
        return d

    @staticmethod
    def get_notify_no_more_external_event_to_occur(timestamp: float = 0) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "NOTIFY",
            "data": {"type": "no_more_external_event_to_occur"},
        }
        return d


class BatsimJobProfileAPI:
    @staticmethod
    def get_parallel(nb_res: int = 1) -> dict:
        d = {
            "type": "parallel",
            "cpu": random.sample(range(0, 5000), nb_res),
            "com": random.sample(range(0, 5000), nb_res*nb_res)
        }
        return d

    @staticmethod
    def get_parallel_homogeneous(cpu: int = 1, com: int = 0) -> dict:
        d = {
            "type": "parallel_homogeneous",
            "cpu": cpu,
            "com": com
        }
        return d

    @staticmethod
    def get_parallel_homogeneous_total(cpu: int = 1, com: int = 0) -> dict:
        d = {
            "type": "parallel_homogeneous_total",
            "cpu": cpu,
            "com": com
        }
        return d

    @staticmethod
    def get_parallel_homogeneous_pfs(
            storage: str = "nfs",
            bytes_to_read: int = 1,
            bytes_to_write: int = 0) -> dict:
        d = {
            "type": "parallel_homogeneous_pfs",
            "bytes_to_read": bytes_to_read,
            "bytes_to_write": bytes_to_write,
            "storage": storage
        }
        return d

    @staticmethod
    def get_data_staging(
            src: str = "nfs",
            dest: str = "pfs",
            nb_bytes: int = 1) -> dict:
        d = {
            "type": "data_staging",
            "nb_bytes": nb_bytes,
            "from": src,
            "to": dest
        }
        return d

    @staticmethod
    def get_composed(
            repeat: int = 1,
            seq: Sequence[str] = ("1", "2")) -> dict:
        d = {
            "type": "composed",
            "repeat": repeat,
            "seq": list(seq)
        }
        return d

    @staticmethod
    def get_delay(delay: int = 1) -> dict:
        d = {"type": "delay", "delay": delay}
        return d


class BatsimRequestAPI:
    @staticmethod
    def get_notify_registration_finished(timestamp: float = 0) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "NOTIFY",
            "data": {"type": "registration_finished"}
        }
        return d

    @staticmethod
    def get_notify_continue_registration(timestamp: float = 0) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "NOTIFY",
            "data": {"type": "continue_registration"},
        }
        return d

    @staticmethod
    def get_reject_job(timestamp: float = 0, job_id: str = "w12!45") -> dict:
        d = {
            "timestamp": timestamp,
            "type": "REJECT_JOB",
            "data": {"job_id": job_id}
        }
        return d

    @staticmethod
    def get_execute_job(
            timestamp: float = 0,
            job_id: str = "w1!1",
            alloc: Sequence[int] = (1, 2),
            storage_mapping: Dict[str, int] = None) -> dict:

        d: dict = {
            "timestamp": timestamp,
            "type": "EXECUTE_JOB",
            "data": {
                "job_id": job_id,
                "alloc": str(ProcSet(*alloc)),
            }
        }
        if storage_mapping:
            d["data"]["storage_mapping"] = storage_mapping
        return d

    @staticmethod
    def get_call_me_later(timestamp: float = 0, at: float = 10) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "CALL_ME_LATER",
            "data": {"timestamp": at}
        }

        return d

    @staticmethod
    def get_kill_job(timestamp: float = 0, ids: Sequence = ("w!0", "w!1")) -> dict:
        assert ids
        d = {
            "timestamp": timestamp,
            "type": "KILL_JOB",
            "data": {"job_ids": list(ids)}
        }
        return d

    @staticmethod
    def get_register_job(
            timestamp: float = 0,
            job_name: str = "1",
            job_workload: str = "w",
            profile_name: str = "p",
            res: int = 1,
            walltime: Optional[float] = None,
            **kwards) -> dict:
        job_id = "{}!{}".format(job_workload, job_name)
        d: dict = {
            "timestamp": timestamp,
            "type": "REGISTER_JOB",
            "data": {
                "job_id": job_id,
                "job": {
                    "profile": profile_name,
                    "res": res,
                    "id": job_id
                }
            }
        }
        if walltime is not None:
            d["data"]["job"]["walltime"] = walltime

        for name, value in kwards.items():
            d["data"]["job"][name] = value

        return d

    @staticmethod
    def get_register_profile(
            timestamp: float = 0,
            workload_name: str = "1",
            profile_name: str = "w",
            profile: dict = None) -> dict:
        d = {
            "timestamp": timestamp,
            "type": "REGISTER_PROFILE",
            "data": {
                "workload_name": workload_name,
                "profile_name":  profile_name,
                "profile": profile or BatsimJobProfileAPI.get_delay()
            }
        }
        return d

    @staticmethod
    def get_set_resource_state(
            timestamp: float = 0,
            resources: Sequence[int] = (1, 2, 3),
            state: str = "1") -> dict:

        d = {
            "timestamp": timestamp,
            "type": "SET_RESOURCE_STATE",
            "data": {
                "resources": str(ProcSet(*resources)),
                "state": state
            }
        }
        return d

    @staticmethod
    def get_change_job_state(
            timestamp: float = 0,
            job_id: str = "w!0",
            job_state: JobState = JobState.COMPLETED_KILLED,
            kill_reason: str = "") -> dict:
        d = {
            "timestamp": timestamp,
            "type": "CHANGE_JOB_STATE",
            "data": {
                "job_id": job_id,
                "job_state": job_state.name.upper(),
                "kill_reason": kill_reason
            }
        }
        return d
