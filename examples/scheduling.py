import argparse
import batsim_py


def run(args):
    simulator = batsim_py.SimulatorHandler()

    simulator.start(args.platform, args.workload, args.output_fn)
    while simulator.is_running:
        avail_hosts = [h.id for h, _ in simulator.agenda if not h.is_allocated]
        for job in simulator.queue:
            if job.res <= len(avail_hosts):
                simulator.allocate(job.id, avail_hosts[:job.res])
                del avail_hosts[:job.res]

        simulator.proceed_time()

    simulator.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--workload", type=str,
                        default='examples/files/workloads/1.json')
    parser.add_argument("-p", "--platform", type=str,
                        default='examples/files/platform.xml')
    parser.add_argument("-o", "--output_fn", type=str,
                        default='/tmp/scheduling')
    return parser.parse_args()


if __name__ == '__main__':
    run(parse_args())
