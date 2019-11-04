import argparse

from batsim_py.utils.schedulers import EASYBackfilling
from batsim_py.rjms import RJMSHandler


def run(args):
    rjms = RJMSHandler(args.use_batsim)
    scheduler = EASYBackfilling(rjms)

    rjms.start(args.platform, args.workload, args.output_fn)
    rjms.proceed_time()
    while rjms.is_running:
        scheduler.schedule()
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
