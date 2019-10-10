import argparse

from batsim_py.utils.schedulers import FirstComeFirstServed
from batsim_py.rjms import RJMSHandler


def run(args):
    rjms = RJMSHandler(args.use_batsim)
    scheduler = FirstComeFirstServed()

    rjms.start(args.platform, args.workload, args.output_fn)
    rjms.proceed_time()
    while rjms.is_running:
        reserved_time = rjms.get_reserved_time()
        jobs = scheduler.schedule(rjms.jobs_queue, reserved_time)
        for job_id in jobs:
            rjms.allocate(job_id)
        rjms.start_ready_jobs()
        rjms.proceed_time()

    rjms.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--use_batsim", action="store_true")
    parser.add_argument("-w", "--workload", type=str, default='examples/files/workloads/1.json')
    parser.add_argument("-p", "--platform", type=str, default='examples/files/platform.xml')
    parser.add_argument("-o", "--output_fn", type=str, default='/tmp/batsim_py/scheduling')
    return parser.parse_args()

if __name__ == '__main__':
    run(parse_args())
