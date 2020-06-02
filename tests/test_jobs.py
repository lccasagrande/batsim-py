import random

import pytest

from batsim_py import jobs


class TestJobState:
    def test_str(self):
        for s in jobs.JobState:
            assert str(s) == s.name.upper()


class TestDelayJobProfile:
    def test_valid_instance(self):
        delay, name = 250, "n"
        p = jobs.DelayJobProfile(name, delay)

        assert p.delay == delay
        assert p.name == name

    def test_delay_cannot_be_zero(self):
        with pytest.raises(ValueError):
            jobs.DelayJobProfile("N", 0)

    def test_delay_cannot_be_negative(self):
        with pytest.raises(ValueError):
            jobs.DelayJobProfile("N", -1)


class TestParallelJobProfile:
    def test_valid_instance(self):
        cpu = random.sample(range(0, 5000), 3)
        com = random.sample(range(0, 5000), 3*3)
        name = "n"

        p = jobs.ParallelJobProfile(name, cpu=cpu, com=com)

        assert p.cpu == cpu
        assert p.com == com
        assert p.name == name

    def test_com_size_greater_than_two_times_cpu_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelJobProfile("n", [1., 1.], [1., 1., 1., 1., 1.])

    def test_com_size_less_than_two_times_cpu_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelJobProfile("n", [1., 1.], [1., 1., 1.])

    def test_com_values_less_than_zero_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelJobProfile("n", [1., 1.], [1., 1., 1., -1.])

    def test_cpu_values_less_than_zero_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelJobProfile("n", [1., -1.], [1., 1., 1., 1.])

    def test_empty_cpu_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelJobProfile("n", [], [])


class TestParallelHomogeneousJobProfile:
    def test_valid_instance(self):
        cpu = 5e6
        com = 10e6
        name = "n"

        p = jobs.ParallelHomogeneousJobProfile(name, cpu=cpu, com=com)

        assert p.cpu == cpu
        assert p.com == com
        assert p.name == name

    def test_zero_com_must_be_valid(self):
        p = jobs.ParallelHomogeneousJobProfile("n", cpu=5e6, com=0)
        assert p.com == 0

    def test_zero_cpu_must_be_valid(self):
        p = jobs.ParallelHomogeneousJobProfile("n", cpu=0, com=5e6)
        assert p.cpu == 0

    def test_zero_cpu_and_com_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousJobProfile("n", cpu=0, com=0)

    def test_negative_cpu_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousJobProfile("n", cpu=-1, com=10)

    def test_negative_com_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousJobProfile("n", cpu=1, com=-2)


class TestParallelHomogeneousTotalJobProfile:
    def test_valid_instance(self):
        cpu = 5e6
        com = 10e6
        name = "n"

        p = jobs.ParallelHomogeneousTotalJobProfile(name, cpu=cpu, com=com)

        assert p.cpu == cpu
        assert p.com == com
        assert p.name == name

    def test_zero_com_must_be_valid(self):
        p = jobs.ParallelHomogeneousTotalJobProfile("n", cpu=5e6, com=0)
        assert p.com == 0

    def test_zero_cpu_must_be_valid(self):
        p = jobs.ParallelHomogeneousTotalJobProfile("n", cpu=0, com=5e6)
        assert p.cpu == 0

    def test_zero_cpu_and_com_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousTotalJobProfile("n", cpu=0, com=0)

    def test_negative_cpu_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousTotalJobProfile("n", cpu=-1, com=10)

    def test_negative_com_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousTotalJobProfile("n", cpu=1, com=-2)


class TestComposedJobProfile:
    def test_valid_instance(self):
        name = "n"
        repeat = random.randint(1, 10)
        profiles = [
            jobs.DelayJobProfile("p1", 100),
            jobs.ParallelHomogeneousJobProfile("p2", 1, 2)
        ]

        p = jobs.ComposedJobProfile(name, profiles, repeat)

        assert p.name == name
        assert p.profiles == profiles
        assert p.repeat == repeat

    def test_empty_profiles_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ComposedJobProfile("n", [])

    def test_repeat_less_than_zero_must_raise(self):

        with pytest.raises(ValueError):
            jobs.ComposedJobProfile("n", ["p1"], -1)

        with pytest.raises(ValueError):
            jobs.ComposedJobProfile("n", ["p1"], 0)


class TestParallelHomogeneousPFSJobProfile:
    def test_valid_instance(self):
        r = 5e6
        w = 10e6
        name = "n"
        storage = "s1"

        p = jobs.ParallelHomogeneousPFSJobProfile(name, r, w, storage)

        assert p.bytes_to_read == r
        assert p.bytes_to_write == w
        assert p.storage == storage
        assert p.name == name

    def test_zero_bytes_to_read_must_be_valid(self):
        p = jobs.ParallelHomogeneousPFSJobProfile("n", 0, 1, "pfs")
        assert p.bytes_to_read == 0

    def test_zero_bytes_to_write_must_be_valid(self):
        p = jobs.ParallelHomogeneousPFSJobProfile("n", 1, 0, "pfs")
        assert p.bytes_to_write == 0

    def test_zero_bytes_to_write_and_read_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousPFSJobProfile("n", 0, 0, "pfs")

    def test_negative_bytes_to_write_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousPFSJobProfile("n", 1, -1, "pfs")

    def test_negative_bytes_to_read_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousPFSJobProfile("n", -1, 1, "pfs")

    def test_empty_storage_must_raise(self):
        with pytest.raises(ValueError):
            jobs.ParallelHomogeneousPFSJobProfile("n", 1, 1, "")


class TestDataStagingJobProfile:
    def test_valid_instance(self):
        name, nb_bytes = "n", 15e6
        src, dest = "src", "dest"

        p = jobs.DataStagingJobProfile(name, nb_bytes, src, dest)

        assert p.nb_bytes == nb_bytes
        assert p.src == src
        assert p.dest == dest
        assert p.name == name

    def test_empty_src_must_raise(self):
        with pytest.raises(ValueError):
            jobs.DataStagingJobProfile("n", 1e6, "", "d")

    def test_empty_dest_must_raise(self):
        with pytest.raises(ValueError):
            jobs.DataStagingJobProfile("n", 1e6, "s", "")

    def test_nb_bytes_not_greater_than_zero_must_raise(self):
        with pytest.raises(ValueError):
            jobs.DataStagingJobProfile("n", -1e6, "s", "d")

        with pytest.raises(ValueError):
            jobs.DataStagingJobProfile("n", 0, "s", "d")


class TestJob:
    def test_valid_instance(self):
        name, workload, res, subtime = "1", "w", 2, 1
        profile = jobs.DelayJobProfile("p", 100)

        j = jobs.Job(name, workload, res, profile, subtime, user_id=22)

        assert j.name == name and j.workload == j.workload
        assert j.res == res and j.profile == profile
        assert j.subtime == subtime and j.state == jobs.JobState.UNKNOWN
        assert j.walltime is None and j.user_id == 22 and j.allocation is None
        assert j.start_time is None and j.stop_time is None

    def test_repr(self):
        j = jobs.Job("a", "w", 1, jobs.DelayJobProfile("p", 100), 1)
        assert repr(j) == "Job_{}".format(j.id)

    def test_id_follows_batsim_format(self):
        workload, name = "w", "1"
        j = jobs.Job(name, workload, 1, jobs.DelayJobProfile("p", 100), 1)

        j_id = "{}!{}".format(workload, name)
        assert j.id == j_id

    def test_empty_name_must_raise(self):
        with pytest.raises(ValueError):
            jobs.Job("", "w", 1, jobs.DelayJobProfile("p", 100), 1)

    def test_empty_workload_must_raise(self):
        with pytest.raises(ValueError):
            jobs.Job("1", "", 1, jobs.DelayJobProfile("p", 100), 1)

    def test_subtime_less_than_zero_must_raise(self):
        with pytest.raises(ValueError):
            jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), -1)

    def test_walltime_less_than_one_must_raise(self):
        with pytest.raises(ValueError):
            jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 1, 0)

        with pytest.raises(ValueError):
            jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 1, -1)

    def test_res_less_than_one_must_raise(self):
        with pytest.raises(ValueError):
            jobs.Job("1", "w", 0, jobs.DelayJobProfile("p", 100), 1)

        with pytest.raises(ValueError):
            jobs.Job("1", "w", -1, jobs.DelayJobProfile("p", 100), 1)

    def test_allocate_staging_job_without_mapping_must_raise(self):
        prof = jobs.DataStagingJobProfile("p", 1, "a", "b")
        alloc = [1]
        j = jobs.Job("1", "w", 1, prof, 0)
        j._submit(0)
        with pytest.raises(ValueError) as excinfo:
            j._allocate(alloc)
        assert "mapping" in str(excinfo.value)

    def test_allocate_staging_job_with_missing_dest_mapping_must_raise(self):
        prof = jobs.DataStagingJobProfile("p", 1, "a", "b")
        alloc = [1]
        j = jobs.Job("1", "w", 1, prof, 0)
        j._submit(0)
        with pytest.raises(ValueError) as excinfo:
            j._allocate(alloc, {"a": 1})
        assert "b" in str(excinfo.value)

    def test_allocate_staging_job_with_missing_src_mapping_must_raise(self):
        prof = jobs.DataStagingJobProfile("p", 1, "a", "b")
        alloc = [1]
        j = jobs.Job("1", "w", 1, prof, 0)
        j._submit(0)
        with pytest.raises(ValueError) as excinfo:
            j._allocate(alloc, {"b": 1})
        assert "a" in str(excinfo.value)

    def test_allocate_pfs_job_with_missing_storage_must_raise(self):
        prof = jobs.ParallelHomogeneousPFSJobProfile("p", 10, 20, "a")
        alloc = [1]
        j = jobs.Job("1", "w", 1, prof, 0)
        j._submit(0)
        with pytest.raises(ValueError) as excinfo:
            j._allocate(alloc, {"b": 1})
        assert "a" in str(excinfo.value)

    def test_allocate_pfs_job_without_mapping_must_raise(self):
        prof = jobs.ParallelHomogeneousPFSJobProfile("p", 10, 20, "a")
        alloc = [1]
        j = jobs.Job("1", "w", 1, prof, 0)
        j._submit(0)
        with pytest.raises(ValueError) as excinfo:
            j._allocate(alloc)
        assert "mapping" in str(excinfo.value)

    def test_allocate_not_storage_job_with_mapping_must_raise(self):
        prof = jobs.DelayJobProfile("p", 100)
        alloc = [1]
        j = jobs.Job("1", "w", 1, prof, 0)
        j._submit(0)
        with pytest.raises(ValueError) as excinfo:
            j._allocate(alloc, {"b": 1})
        assert "mapping" in str(excinfo.value)

    def test_allocate_valid(self):
        alloc = [1]
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate(alloc)
        assert j.is_runnable and j.state == jobs.JobState.ALLOCATED
        assert j.allocation == alloc

    def test_allocate_cannot_be_changed(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        with pytest.raises(RuntimeError):
            j._allocate([3])

    def test_allocate_not_submitted_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        with pytest.raises(RuntimeError) as excinfo:
            j._allocate([3])

        assert "queue" in str(excinfo.value)

    def test_allocate_size_bigger_than_requested_must_raise(self):
        j = jobs.Job("1", "w", 3, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        with pytest.raises(ValueError) as excinfo:
            j._allocate([3, 2, 5, 10])

        assert "hosts" in str(excinfo.value)

    def test_allocate_size_less_than_requested_must_raise(self):
        j = jobs.Job("1", "w", 3, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        with pytest.raises(ValueError) as excinfo:
            j._allocate([3, 2])

        assert "hosts" in str(excinfo.value)

    def test_reject_job_not_submitted_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        with pytest.raises(RuntimeError) as excinfo:
            j._reject()
        assert 'queue' in str(excinfo.value)

    def test_reject_job_submitted_valid(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._reject()
        assert j.state == jobs.JobState.REJECTED and j.is_rejected

    def test_reject_allocated_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        with pytest.raises(RuntimeError) as excinfo:
            j._reject()

        assert 'queue' in str(excinfo.value)

    def test_reject_finished_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)
        j._terminate(10, jobs.JobState.COMPLETED_SUCCESSFULLY)
        with pytest.raises(RuntimeError) as excinfo:
            j._reject()
        assert 'queue' in str(excinfo.value)

    def test_submit_valid(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        assert j.subtime == 0
        assert j.state == jobs.JobState.SUBMITTED and j.is_submitted

    def test_submit_submitted_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        with pytest.raises(RuntimeError) as excinfo:
            j._submit(0)
        assert 'submitted' in str(excinfo.value)

    def test_submit_invalid_time_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        with pytest.raises(ValueError) as excinfo:
            j._submit(-1)
        assert 'subtime' in str(excinfo.value)

    def test_start_valid(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(2)
        assert j.is_running
        assert j.state == jobs.JobState.RUNNING
        assert j.start_time == 2

    def test_start_not_allocated_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        with pytest.raises(RuntimeError) as excinfo:
            j._start(0)
        assert 'runnable' in str(excinfo.value)

    def test_start_finished_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(1)
        j._terminate(2, jobs.JobState.COMPLETED_FAILED)
        with pytest.raises(RuntimeError) as excinfo:
            j._start(2)
        assert 'runnable' in str(excinfo.value)

    def test_start_running_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(1)
        with pytest.raises(RuntimeError) as excinfo:
            j._start(2)
        assert 'runnable' in str(excinfo.value)

    def test_start_rejected_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._reject()
        with pytest.raises(RuntimeError) as excinfo:
            j._start(2)
        assert 'runnable' in str(excinfo.value)

    def test_start_invalid_time_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(2)
        j._allocate([1])
        with pytest.raises(ValueError) as excinfo:
            j._start(1)
        assert 'current_time' in str(excinfo.value)

    def test_terminate_with_invalid_final_state_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)

        with pytest.raises(ValueError) as excinfo:
            j._terminate(10, jobs.JobState.ALLOCATED)
        assert 'final_state' in str(excinfo.value)

        with pytest.raises(ValueError) as excinfo:
            j._terminate(10, jobs.JobState.REJECTED)
        assert 'final_state' in str(excinfo.value)

    def test_terminate_successfully_valid(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)
        j._terminate(10, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert j.is_finished
        assert j.state == jobs.JobState.COMPLETED_SUCCESSFULLY
        assert j.stop_time == 10

    def test_terminate_failed_valid(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)
        j._terminate(10, jobs.JobState.COMPLETED_FAILED)
        assert j.is_finished
        assert j.state == jobs.JobState.COMPLETED_FAILED
        assert j.stop_time == 10

    def test_terminate_killed_valid(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)
        j._terminate(10, jobs.JobState.COMPLETED_KILLED)
        assert j.is_finished
        assert j.state == jobs.JobState.COMPLETED_KILLED
        assert j.stop_time == 10

    def test_terminate_walltime_reached_valid(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)
        j._terminate(10, jobs.JobState.COMPLETED_WALLTIME_REACHED)
        assert j.is_finished
        assert j.state == jobs.JobState.COMPLETED_WALLTIME_REACHED
        assert j.stop_time == 10

    def test_terminate_finished_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)
        j._terminate(10, jobs.JobState.COMPLETED_SUCCESSFULLY)
        with pytest.raises(RuntimeError) as excinfo:
            j._terminate(10, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert 'running' in str(excinfo.value)

    def test_terminate_not_running_job_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        with pytest.raises(RuntimeError) as excinfo:
            j._terminate(10, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert 'running' in str(excinfo.value)

    def test_terminate_invalid_time_must_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(2)
        with pytest.raises(ValueError) as excinfo:
            j._terminate(1, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert 'current_time' in str(excinfo.value)

    def test_waiting_time(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(10)
        assert j.waiting_time == 10

    def test_waiting_time_when_not_started_must_not_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        assert j.waiting_time is None

    def test_runtime(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)
        j._terminate(10, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert j.runtime == 10

    def test_runtime_when_not_finished_must_not_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(0)
        assert j.runtime is None

    def test_stretch_with_walltime(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0, 100)
        j._submit(0)
        j._allocate([1])
        j._start(10)
        j._terminate(20, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert j.stretch == j.waiting_time / j.walltime

    def test_stretch_with_runtime(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(10)
        j._terminate(30, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert j.stretch == j.waiting_time / j.runtime

    def test_turnaround_time(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(10)
        j._terminate(30, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert j.turnaround_time == j.waiting_time + j.runtime

    def test_turnaround_time_when_not_finished_must_not_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(10)
        assert j.turnaround_time is None

    def test_per_processor_slowdown(self):
        j = jobs.Job("1", "w", 2, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1, 2])
        j._start(10)
        j._terminate(30, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert j.per_processor_slowdown == max(
            1, j.turnaround_time / (j.res * j.runtime))

    def test_per_processor_slowdown_when_not_finished_must_not_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(10)
        assert j.per_processor_slowdown is None

    def test_slowdown(self):
        j = jobs.Job("1", "w", 2, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1, 2])
        j._start(10)
        j._terminate(30, jobs.JobState.COMPLETED_SUCCESSFULLY)
        assert j.slowdown == max(1, j.turnaround_time / j.runtime)

    def test_slowdown_when_not_finished_must_not_raise(self):
        j = jobs.Job("1", "w", 1, jobs.DelayJobProfile("p", 100), 0)
        j._submit(0)
        j._allocate([1])
        j._start(10)
        assert j.slowdown is None
