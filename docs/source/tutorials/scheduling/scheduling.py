import argparse
import batsim_py


def run(args):
    simulator = batsim_py.SimulatorHandler()

    # instantiate monitors to collect simulation statistics
    cons_energy_mon = batsim_py.monitors.ConsumedEnergyMonitor(simulator)
    jobs_mon = batsim_py.monitors.JobMonitor(simulator)
    sim_mon = batsim_py.monitors.SimulationMonitor(simulator)
    pstates_mon = batsim_py.monitors.HostPowerStateSwitchMonitor(simulator)
    states_mon = batsim_py.monitors.HostStateSwitchMonitor(simulator)

    # start simulation
    simulator.start(args.platform, args.workload,
                    args.verbosity, args.simulation_time)

    # switch off all hosts
    simulator.switch_off([h.id for h in simulator.platform.hosts])

    # proceed 3t
    simulator.proceed_time(1)
    simulator.proceed_time(1)
    simulator.proceed_time(1)

    # switch on all hosts
    simulator.switch_on([h.id for h in simulator.platform.hosts])

    # proceed 1t
    simulator.proceed_time(1)

    # switch off all hosts
    simulator.switch_off([h.id for h in simulator.platform.hosts])

    while simulator.is_running:
        # get not allocated hosts
        avail_hosts = [h.id for h, _ in simulator.agenda if not h.is_allocated]

        # schedule following the First-Fit policy
        for job in simulator.queue:
            if job.res <= len(avail_hosts):
                # the simulator will automatically switch-on allocated resources.
                simulator.allocate(job.id, avail_hosts[:job.res])
                del avail_hosts[:job.res]

        # proceed directly to the next event instead of time
        simulator.proceed_time()

    # close simulator
    simulator.close()

    # dump simulation statistics
    cons_energy_mon.to_csv(args.output + "/consumed_energy.csv")
    jobs_mon.to_csv(args.output + "/jobs.csv")
    pstates_mon.to_csv(args.output + "/pstate_changes.csv")
    states_mon.to_csv(args.output + "/machine_states.csv")
    sim_mon.to_csv(args.output + "/scheduler.csv")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--workload", type=str,
                        default='files/workloads/1.json')
    parser.add_argument("-p", "--platform", type=str,
                        default='files/platform.xml')
    parser.add_argument("-o", "--output", type=str, default='/tmp')
    parser.add_argument("-v", "--verbosity", type=str, default='quiet')
    parser.add_argument("-t", "--simulation_time", type=int, default=None)
    return parser.parse_args()


if __name__ == '__main__':
    run(parse_args())
